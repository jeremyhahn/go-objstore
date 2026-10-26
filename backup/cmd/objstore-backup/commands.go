package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jeremyhahn/go-objstore/backup/pkg/backup"
	"github.com/jeremyhahn/go-objstore/backup/pkg/keyseam"
	"github.com/jeremyhahn/go-objstore/backup/pkg/prune"
	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
	"github.com/jeremyhahn/go-objstore/backup/pkg/restore"
	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

const connHelp = `
The repository is reached through the go-objstore Go client SDK: --address
and --protocol name the server, --repo the prefix the repository lives under.
The passphrase comes from --passphrase-file, a regular file with no group or
other permissions, or from standard input with --passphrase-stdin. It is never
taken from an argument or the environment.
`

const initHelp = `Usage: objstore-backup init [flags]

Create a repository: a new random data key, wrapped under a key derived from
the passphrase, and the repository's config.
` + connHelp

const backupHelp = `Usage: objstore-backup backup [flags] <path>...

Back up each directory, one snapshot per path. Only chunks the repository
does not hold yet are uploaded. The backup identity needs no delete right.
` + snapHelp + connHelp

const snapshotsHelp = `Usage: objstore-backup snapshots [flags]

List the snapshots, oldest first. A snapshot is named by its ID or by any
prefix of it that names one snapshot only.
` + connHelp

const restoreHelp = `Usage: objstore-backup restore [flags] <snapshot> <target>

Restore a snapshot into the directory target, creating it if it is missing.
Every chunk and file digest is checked before a file takes its final name.
Existing files are refused unless --overwrite is given.
` + connHelp

const verifyHelp = `Usage: objstore-backup verify [flags] [<snapshot>...]

Check every index, pack and chunk, and the files of the named snapshots (all
snapshots by default), without writing files. Exits 3 if problems are found.
` + connHelp

const pruneHelp = `Usage: objstore-backup prune [flags]

Forget the snapshots the retention policy does not keep, and remove the data
only they used. The policy applies to each host's snapshots of each source
directory on its own, and prune refuses a run that would forget every
snapshot of one. A pack no kept snapshot uses is first marked, and removed by
a later run once the grace period has passed; the grace period must exceed
the longest backup.

Prune deletes objects, so it needs a delete-capable identity, which the
backup identity must not be. It connects with its own settings, the
--prune-* flags (--prune-address, --prune-token-file, ...), and does not
take the backup identity's. --repo and the passphrase are the repository's.
`

// repoOpts are the flags of a command that runs as the backup identity.
type repoOpts struct {
	conn connFlags
	repo repoFlags
}

func (o *repoOpts) register(fs *flag.FlagSet) {
	o.conn.register(fs, "", "backup")
	o.repo.register(fs)
}

func (o *repoOpts) check() error {
	if err := o.conn.check(); err != nil {
		return err
	}
	return o.repo.check()
}

// session is a passphrase provider and an open store connection.
type session struct {
	provider *keyseam.PassphraseProvider
	closer   func() error
}

func (s *session) close() error {
	return errors.Join(s.closer(), s.provider.Close())
}

// connect reads the passphrase and connects with conn.
func (e *env) connect(ctx context.Context, conn *connFlags, rf *repoFlags, iterations int) (*session, store.PruneStore, error) {
	p, err := rf.provider(e.stdin, iterations)
	if err != nil {
		return nil, nil, err
	}
	cfg, err := conn.config()
	if err != nil {
		return nil, nil, errors.Join(err, p.Close())
	}
	st, closer, err := e.open(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Join(err, p.Close())
	}
	return &session{provider: p, closer: closer}, st, nil
}

// withRepo opens the repository as the backup identity, calls fn and
// closes everything. fn returns the exit code and an error to print.
func (e *env) withRepo(ctx context.Context, name string, o *repoOpts, fn func(r *repo.Repository, w io.Writer) (int, error)) int {
	s, st, err := e.connect(ctx, &o.conn, &o.repo, 0)
	if err != nil {
		return e.fail(name, err)
	}
	r, err := repo.Open(ctx, st, o.repo.repo, s.provider)
	if err != nil {
		return e.fail(name, errors.Join(err, s.close()))
	}
	out := &reportWriter{w: e.stdout}
	code, err := fn(r, out)
	if cerr := errors.Join(r.Close(), s.close()); cerr != nil && err == nil {
		err = cerr
	}
	if out.err != nil && (err == nil || errors.Is(err, out.err)) {
		err = out.failed()
	}
	if err != nil {
		return e.fail(name, err)
	}
	return code
}

// reportWriter writes a command's report and keeps the first error writing
// it, so a report the output refused fails the command.
type reportWriter struct {
	w   io.Writer
	err error
}

func (r *reportWriter) Write(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	n, err := r.w.Write(p)
	r.err = err
	return n, err
}

// failed is the error writing the report, if any.
func (r *reportWriter) failed() error {
	if r.err == nil {
		return nil
	}
	return fmt.Errorf("write report: %w", r.err)
}

func cmdInit(ctx context.Context, e *env, args []string) int {
	const name = "init"
	fs := newFlags(name)
	var o repoOpts
	o.register(fs)
	iterations := fs.Int("iterations", keyseam.MinIterations,
		"PBKDF2 iterations deriving the key that wraps the data key")
	if _, code, ok := e.parse(name, initHelp, fs, args, 0, 0); !ok {
		return code
	}
	if err := o.check(); err != nil {
		return e.usageError(name, err)
	}
	if *iterations < keyseam.MinIterations || *iterations > keyseam.MaxIterations {
		return e.usageError(name, fmt.Errorf("--iterations must be from %d to %d", keyseam.MinIterations, keyseam.MaxIterations))
	}
	s, st, err := e.connect(ctx, &o.conn, &o.repo, *iterations)
	if err != nil {
		return e.fail(name, err)
	}
	r, err := repo.Init(ctx, st, o.repo.repo, s.provider, repo.Options{})
	if err != nil {
		return e.fail(name, errors.Join(err, s.close()))
	}
	id := r.Config().ID
	if err := errors.Join(r.Close(), s.close()); err != nil {
		return e.fail(name, err)
	}
	out := &reportWriter{w: e.stdout}
	fmt.Fprintf(out, "created repository %s at %q\n", id, r.Layout().Prefix())
	if err := out.failed(); err != nil {
		return e.fail(name, err)
	}
	return exitOK
}

func cmdBackup(ctx context.Context, e *env, args []string) int {
	const name = "backup"
	fs := newFlags(name)
	var o repoOpts
	o.register(fs)
	var so snapOpts
	so.register(fs)
	host := fs.String("host", "", "host name recorded in the snapshots (default: this system's)")
	paths, code, ok := e.parse(name, backupHelp, fs, args, 1, -1)
	if !ok {
		return code
	}
	if err := o.check(); err != nil {
		return e.usageError(name, err)
	}
	scfg, err := so.config()
	if err != nil {
		return e.usageError(name, err)
	}
	return e.withRepo(ctx, name, &o, func(r *repo.Repository, w io.Writer) (code int, err error) {
		log := slog.New(slog.NewTextHandler(e.stderr, nil))
		snap, err := e.snapshotter(scfg, log)
		if err != nil {
			return exitFailure, err
		}
		set, err := snapshot(ctx, snap, paths, log)
		if err != nil {
			return exitFailure, err
		}
		defer func() {
			if cerr := set.Close(); cerr != nil {
				code, err = exitFailure, errors.Join(err, cerr)
			}
		}()
		for _, path := range paths {
			res, err := backup.Run(ctx, r, path, backup.Options{Host: *host, ReadFrom: set.Path(path), Skip: set.Skip()})
			if err != nil {
				return exitFailure, err
			}
			st := res.Stats
			fmt.Fprintf(w, "snapshot %s saved: %s\n", res.Snapshot, path)
			fmt.Fprintf(w, "  %d files, %d dirs, %d symlinks, %d bytes; %d skipped\n",
				st.Files, st.Dirs, st.Symlinks, st.Bytes, st.Skipped)
			fmt.Fprintf(w, "  %d new chunks (%d bytes), %d deduplicated; %d packs written\n",
				st.ChunksNew, st.BytesNew, st.ChunksDeduped, st.PacksWritten)
		}
		return exitOK, nil
	})
}

// snapshotInfo is one line of the snapshots listing.
type snapshotInfo struct {
	ID       seal.ID   `json:"id"`
	Time     time.Time `json:"time"`
	Host     string    `json:"host"`
	Root     string    `json:"root"`
	Files    int       `json:"files"`
	Dirs     int       `json:"dirs"`
	Symlinks int       `json:"symlinks"`
	Bytes    int64     `json:"bytes"`
	Skipped  int       `json:"skipped"`
}

func cmdSnapshots(ctx context.Context, e *env, args []string) int {
	const name = "snapshots"
	fs := newFlags(name)
	var o repoOpts
	o.register(fs)
	asJSON := fs.Bool("json", false, "print the list as JSON")
	if _, code, ok := e.parse(name, snapshotsHelp, fs, args, 0, 0); !ok {
		return code
	}
	if err := o.check(); err != nil {
		return e.usageError(name, err)
	}
	return e.withRepo(ctx, name, &o, func(r *repo.Repository, w io.Writer) (int, error) {
		ids, err := r.ListSnapshots(ctx)
		if err != nil {
			return exitFailure, err
		}
		list := make([]snapshotInfo, 0, len(ids))
		for _, id := range ids {
			s, err := r.ReadSnapshot(ctx, id)
			if err != nil {
				return exitFailure, err
			}
			info := snapshotInfo{ID: id, Time: s.Time, Host: s.Host, Root: s.Root, Skipped: s.Skipped}
			for _, n := range s.Nodes {
				switch n.Type {
				case repo.NodeFile:
					info.Files++
					info.Bytes += n.Size
				case repo.NodeDir:
					info.Dirs++
				case repo.NodeSymlink:
					info.Symlinks++
				}
			}
			list = append(list, info)
		}
		slices.SortStableFunc(list, func(a, b snapshotInfo) int { return a.Time.Compare(b.Time) })
		if *asJSON {
			return exitOK, writeJSON(w, list)
		}
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tTIME\tHOST\tROOT\tFILES\tBYTES")
		for _, s := range list {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%d\n",
				s.ID.String()[:12], s.Time.Local().Format(time.DateTime), s.Host, s.Root, s.Files, s.Bytes)
		}
		return exitOK, tw.Flush()
	})
}

var (
	errNoSnapshot = errors.New("no snapshot has that ID")
	errAmbiguous  = errors.New("more than one snapshot has that ID prefix")
	errBadID      = errors.New("a snapshot ID is hexadecimal")
)

// resolve returns the snapshot whose ID is s or begins with s.
func resolve(ctx context.Context, r *repo.Repository, s string) (seal.ID, error) {
	s = strings.ToLower(s)
	if len(s) == 2*seal.IDSize {
		return seal.ParseID(s)
	}
	if s == "" || strings.Trim(s, "0123456789abcdef") != "" {
		return seal.ID{}, fmt.Errorf("snapshot %q: %w", s, errBadID)
	}
	ids, err := r.ListSnapshots(ctx)
	if err != nil {
		return seal.ID{}, err
	}
	var found []seal.ID
	for _, id := range ids {
		if strings.HasPrefix(id.String(), s) {
			found = append(found, id)
		}
	}
	switch len(found) {
	case 0:
		return seal.ID{}, fmt.Errorf("snapshot %q: %w", s, errNoSnapshot)
	case 1:
		return found[0], nil
	default:
		return seal.ID{}, fmt.Errorf("snapshot %q: %w", s, errAmbiguous)
	}
}

func cmdRestore(ctx context.Context, e *env, args []string) int {
	const name = "restore"
	fs := newFlags(name)
	var o repoOpts
	o.register(fs)
	overwrite := fs.Bool("overwrite", false, "replace existing files and symlinks in the target")
	pos, code, ok := e.parse(name, restoreHelp, fs, args, 2, 2)
	if !ok {
		return code
	}
	if err := o.check(); err != nil {
		return e.usageError(name, err)
	}
	return e.withRepo(ctx, name, &o, func(r *repo.Repository, w io.Writer) (int, error) {
		id, err := resolve(ctx, r, pos[0])
		if err != nil {
			return exitFailure, err
		}
		st, err := restore.Restore(ctx, r, id, pos[1], restore.Options{Overwrite: *overwrite})
		if err != nil {
			return exitFailure, err
		}
		fmt.Fprintf(w, "restored snapshot %s to %s: %d files, %d dirs, %d symlinks, %d bytes\n",
			id, pos[1], st.Files, st.Dirs, st.Symlinks, st.Bytes)
		return exitOK, nil
	})
}

// problem is one fault verify found, as JSON.
type problem struct {
	Op     string `json:"op"`
	Object string `json:"object,omitempty"`
	Chunk  string `json:"chunk,omitempty"`
	Kind   string `json:"kind"`
	Error  string `json:"error"`
}

// verifyReport is verify's report, as JSON.
type verifyReport struct {
	Snapshots int       `json:"snapshots"`
	Indexes   int       `json:"indexes"`
	Packs     int       `json:"packs"`
	Chunks    int       `json:"chunks"`
	Files     int       `json:"files"`
	Bytes     int64     `json:"bytes"`
	Problems  []problem `json:"problems"`
}

func cmdVerify(ctx context.Context, e *env, args []string) int {
	const name = "verify"
	fs := newFlags(name)
	var o repoOpts
	o.register(fs)
	asJSON := fs.Bool("json", false, "print the report as JSON")
	pos, code, ok := e.parse(name, verifyHelp, fs, args, 0, -1)
	if !ok {
		return code
	}
	if err := o.check(); err != nil {
		return e.usageError(name, err)
	}
	return e.withRepo(ctx, name, &o, func(r *repo.Repository, w io.Writer) (int, error) {
		var ids []seal.ID
		for _, s := range pos {
			id, err := resolve(ctx, r, s)
			if err != nil {
				return exitFailure, err
			}
			ids = append(ids, id)
		}
		rep, err := restore.Verify(ctx, r, restore.VerifyOptions{Snapshots: ids})
		if err != nil && (rep == nil || len(rep.Problems) == 0 ||
			errors.Is(err, restore.ErrRepository) || errors.Is(err, restore.ErrCanceled)) {
			return exitFailure, err
		}
		out := verifyReport{
			Snapshots: rep.Snapshots, Indexes: rep.Indexes, Packs: rep.Packs,
			Chunks: rep.Chunks, Files: rep.Files, Bytes: rep.Bytes,
			Problems: make([]problem, 0, len(rep.Problems)),
		}
		for _, p := range rep.Problems {
			out.Problems = append(out.Problems, problem{
				Op: p.Op, Object: p.Object, Chunk: p.Chunk, Kind: p.Err.Error(), Error: p.Error(),
			})
		}
		code := exitOK
		if len(out.Problems) > 0 {
			code = exitProblems
		}
		if *asJSON {
			return code, writeJSON(w, out)
		}
		fmt.Fprintf(w, "checked %d snapshots, %d indexes, %d packs, %d chunks, %d files (%d bytes)\n",
			out.Snapshots, out.Indexes, out.Packs, out.Chunks, out.Files, out.Bytes)
		for _, p := range out.Problems {
			fmt.Fprintf(w, "problem: %s\n", p.Error)
		}
		if code == exitOK {
			fmt.Fprintln(w, "no problems found")
		} else {
			fmt.Fprintf(w, "%d problems found\n", len(out.Problems))
		}
		return code, nil
	})
}

// pruneReport is prune's report, as JSON.
type pruneReport struct {
	DryRun       bool         `json:"dry_run"`
	Kept         []seal.ID    `json:"kept"`
	Forgotten    []seal.ID    `json:"forgotten"`
	Groups       []pruneGroup `json:"groups"`
	Index        string       `json:"index,omitempty"`
	Superseded   []seal.ID    `json:"superseded"`
	Marked       int          `json:"marked"`
	Removed      []seal.ID    `json:"removed"`
	RemovedBytes int64        `json:"removed_bytes"`
	Partial      int          `json:"partial"`
	UnusedBytes  int64        `json:"unused_bytes"`
}

// pruneGroup is one host's snapshots of one directory, which the policy
// applies to on its own.
type pruneGroup struct {
	Host      string    `json:"host"`
	Root      string    `json:"root"`
	Kept      []seal.ID `json:"kept"`
	Forgotten []seal.ID `json:"forgotten"`
}

func pruneGroups(gs []prune.Group) []pruneGroup {
	out := make([]pruneGroup, 0, len(gs))
	for _, g := range gs {
		out = append(out, pruneGroup{Host: g.Host, Root: g.Root, Kept: nonNil(g.Kept), Forgotten: nonNil(g.Forgotten)})
	}
	return out
}

func cmdPrune(ctx context.Context, e *env, args []string) int {
	const name = "prune"
	fs := newFlags(name)
	var (
		conn connFlags
		rf   repoFlags
		pol  prune.Policy
		opts = prune.Options{Grace: prune.DefaultGrace}
	)
	conn.register(fs, "prune-", "prune")
	rf.register(fs)
	fs.IntVar(&pol.Last, "keep-last", 0, "keep the newest `n` snapshots")
	fs.Var(days{&pol.Within}, "keep-within", "keep every snapshot taken within this `duration` (such as 72h or 30d)")
	fs.IntVar(&pol.Daily, "keep-daily", 0, "keep the newest snapshot of each of the last `n` days")
	fs.IntVar(&pol.Weekly, "keep-weekly", 0, "keep the newest snapshot of each of the last `n` ISO weeks")
	fs.IntVar(&pol.Monthly, "keep-monthly", 0, "keep the newest snapshot of each of the last `n` months")
	fs.IntVar(&pol.Yearly, "keep-yearly", 0, "keep the newest snapshot of each of the last `n` years")
	fs.Var(days{&opts.Grace}, "grace", "how long a pack stays marked before it is removed (such as 24h or 2d)")
	fs.BoolVar(&opts.DryRun, "dry-run", false, "report what would be removed; write and remove nothing")
	asJSON := fs.Bool("json", false, "print the report as JSON")
	if _, code, ok := e.parse(name, pruneHelp, fs, args, 0, 0); !ok {
		return code
	}
	if err := errors.Join(conn.check(), rf.check(), checkPolicy(pol, opts.Grace)); err != nil {
		return e.usageError(name, err)
	}
	s, st, err := e.connect(ctx, &conn, &rf, 0)
	if err != nil {
		return e.fail(name, err)
	}
	pr, err := prune.Open(ctx, st, rf.repo, s.provider)
	if err != nil {
		return e.fail(name, errors.Join(err, s.close()))
	}
	rep, err := pr.Run(ctx, pol, opts)
	if cerr := errors.Join(pr.Close(), s.close()); err == nil {
		err = cerr
	}
	if err != nil {
		return e.fail(name, err)
	}
	out := pruneReport{
		DryRun: rep.DryRun, Kept: nonNil(rep.Kept), Forgotten: nonNil(rep.Forgotten), Groups: pruneGroups(rep.Groups),
		Superseded: nonNil(rep.Superseded), Marked: rep.Marked, Removed: nonNil(rep.Removed),
		RemovedBytes: rep.RemovedBytes, Partial: rep.Partial, UnusedBytes: rep.UnusedBytes,
	}
	if rep.Index != (seal.ID{}) {
		out.Index = rep.Index.String()
	}
	w := &reportWriter{w: e.stdout}
	if *asJSON {
		err = writeJSON(w, out)
	} else {
		printPrune(w, out)
	}
	if w.err != nil {
		err = w.failed()
	}
	if err != nil {
		return e.fail(name, err)
	}
	return exitOK
}

func checkPolicy(pol prune.Policy, grace time.Duration) error {
	switch {
	case pol.Last < 0 || pol.Within < 0 || pol.Daily < 0 || pol.Weekly < 0 || pol.Monthly < 0 || pol.Yearly < 0:
		return errors.New("a --keep flag is negative")
	case pol == prune.Policy{}:
		return errors.New("the policy keeps nothing: give at least one --keep flag")
	case grace <= 0:
		return errors.New("--grace must be positive")
	}
	return nil
}

func printPrune(w io.Writer, r pruneReport) {
	verb := "removed"
	if r.DryRun {
		verb = "would remove"
		fmt.Fprintln(w, "dry run: nothing was written or removed")
	}
	fmt.Fprintf(w, "kept %d snapshots, forgot %d\n", len(r.Kept), len(r.Forgotten))
	for _, g := range r.Groups {
		fmt.Fprintf(w, "host %s, %s: kept %d, forgot %d\n", g.Host, g.Root, len(g.Kept), len(g.Forgotten))
		for _, id := range g.Kept {
			fmt.Fprintf(w, "  kept %s\n", id)
		}
		for _, id := range g.Forgotten {
			fmt.Fprintf(w, "  forgot %s\n", id)
		}
	}
	if r.Index != "" {
		fmt.Fprintf(w, "wrote index %s, superseding %d\n", r.Index, len(r.Superseded))
	}
	fmt.Fprintf(w, "%s %d packs (%d bytes); %d unused packs marked, kept for the grace period\n",
		verb, len(r.Removed), r.RemovedBytes, r.Marked)
	if r.Partial > 0 {
		fmt.Fprintf(w, "%d partly used packs hold %d unused bytes\n", r.Partial, r.UnusedBytes)
	}
}

func nonNil(ids []seal.ID) []seal.ID {
	if ids == nil {
		return []seal.ID{}
	}
	return ids
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
