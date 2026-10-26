package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/jeremyhahn/go-objstore/backup/internal/schedule"
	"github.com/jeremyhahn/go-objstore/backup/pkg/backup"
	"github.com/jeremyhahn/go-objstore/backup/pkg/prune"
	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
	"github.com/jeremyhahn/go-objstore/backup/pkg/srcsnap"
)

const serviceHelp = `Usage: objstore-backup service --config <file> [--check]

Run as a long-lived service that backs the configured sources up on a
schedule and, if the configuration has a prune section, prunes after each
backup as the separate prune identity. The configuration is a JSON file that
group and others cannot write; docs/backup-agent/README.md describes it.

Each run reads the passphrase from its file, opens the repository, backs up,
prunes, and closes the repository, which destroys the data key until the next
run. SIGINT, SIGTERM or a Windows service stop request ends the service: the
current run is abandoned, which leaves the repository consistent.

With source_snapshot set, each run reads the sources from snapshots of their
volumes and removes them after; leftovers of an interrupted run are removed
at start and before each run.

--check loads and checks the configuration file and exits.
`

func cmdService(ctx context.Context, e *env, args []string) int {
	const name = "service"
	fs := newFlags(name)
	path := fs.String("config", "", "configuration file (required)")
	check := fs.Bool("check", false, "check the configuration file and exit")
	if _, code, ok := e.parse(name, serviceHelp, fs, args, 0, 0); !ok {
		return code
	}
	if *path == "" {
		return e.usageError(name, errors.New("--config is required"))
	}
	cfg, err := loadConfig(*path)
	if err != nil {
		return e.fail(name, err)
	}
	if *check {
		fmt.Fprintf(e.stdout, "configuration %s is valid: %d sources, %s\n", *path, len(cfg.sources), cfg.schedule)
		return exitOK
	}
	logOut, closeLog, err := cfg.logWriter(e.stderr)
	if err != nil {
		return e.fail(name, err)
	}
	defer closeLog()
	log := cfg.logger(logOut)
	if err := serve(ctx, func(ctx context.Context) error { return e.service(ctx, cfg, log) }); err != nil {
		log.Error("service failed", "error", err.Error())
		return e.fail(name, err)
	}
	return exitOK
}

// logWriter is where the service logs: the log file if one is set, which
// is created with no group or other permissions, or else w.
func (c *serviceConfig) logWriter(w io.Writer) (io.Writer, func(), error) {
	if c.logFile == "" {
		return w, func() {}, nil
	}
	f, err := os.OpenFile(c.logFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("log file %s: %w", c.logFile, pathless(err))
	}
	return f, func() { _ = f.Close() }, nil
}

func (c *serviceConfig) logger(w io.Writer) *slog.Logger {
	if c.logFormat == "json" {
		return slog.New(slog.NewJSONHandler(w, nil))
	}
	return slog.New(slog.NewTextHandler(w, nil))
}

// service runs the schedule until ctx is done.
func (e *env) service(ctx context.Context, cfg *serviceConfig, log *slog.Logger) error {
	log.Info("service started", "schedule", cfg.schedule.String(), "jitter", cfg.schedule.Jitter().String(),
		"sources", len(cfg.sources), "prune", cfg.prune != nil, "source_snapshot", string(cfg.snap.Method))
	snap, err := e.snapshotter(cfg.snap, log)
	if err != nil {
		return err
	}
	if cfg.snap.Method != srcsnap.None {
		if err := snap.Sweep(ctx, cfg.sources); err != nil {
			log.Warn("source snapshot sweep failed", "method", string(cfg.snap.Method), "error", err.Error())
		}
	}
	n := 0
	loop := &schedule.Loop{
		Schedule: cfg.schedule,
		Clock:    e.clock,
		Due:      func(at time.Time) { log.Info("next run", "at", at) },
	}
	err = loop.Run(ctx, func(ctx context.Context) {
		n++
		e.runOnce(ctx, cfg, snap, log.With("run", n))
		if e.ran != nil {
			e.ran()
		}
	})
	if errors.Is(err, context.Canceled) {
		log.Info("service stopped", "runs", n)
		return nil
	}
	return err
}

// runOnce backs every source up and then prunes. It logs every outcome and
// returns nothing: a failed run leaves the service waiting for the next.
func (e *env) runOnce(ctx context.Context, cfg *serviceConfig, snap *srcsnap.Snapshotter, log *slog.Logger) {
	began := time.Now()
	log.Info("run started")
	saved, err := e.backupAll(ctx, cfg, snap, log)
	if err == nil && cfg.prune != nil {
		err = e.pruneOnce(ctx, cfg, log)
	}
	if err != nil {
		log.Error("run failed", "snapshots", saved, "duration", time.Since(began), "error", err.Error())
		return
	}
	log.Info("run finished", "snapshots", saved, "duration", time.Since(began))
}

// backupAll opens the repository as the backup identity, backs each source
// up, and closes the repository, so the data key is destroyed. It goes on
// past a failed source and returns the snapshots saved and every error.
func (e *env) backupAll(ctx context.Context, cfg *serviceConfig, snap *srcsnap.Snapshotter, log *slog.Logger) (saved int, err error) {
	s, st, err := e.connect(ctx, &cfg.conn, &cfg.repo, 0)
	if err != nil {
		return 0, err
	}
	r, err := repo.Open(ctx, st, cfg.repo.repo, s.provider)
	if err != nil {
		return 0, errors.Join(err, s.close())
	}
	set, err := snapshot(ctx, snap, cfg.sources, log)
	if err != nil {
		return 0, errors.Join(err, r.Close(), s.close())
	}
	// The snapshots go before the repository closes, and on every path out,
	// a panic included.
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, set.Close())
		}
	}()
	var errs []error
	for _, src := range cfg.sources {
		if ctx.Err() != nil {
			errs = append(errs, ctx.Err())
			break
		}
		began := time.Now()
		res, err := backup.Run(ctx, r, src, backup.Options{Host: cfg.host, ReadFrom: set.Path(src), Skip: set.Skip()})
		if err != nil {
			log.Error("backup failed", "source", src, "duration", time.Since(began), "error", err.Error())
			errs = append(errs, err)
			continue
		}
		saved++
		st := res.Stats
		log.Info("snapshot saved", "source", src, "snapshot", res.Snapshot.String(),
			"files", st.Files, "dirs", st.Dirs, "symlinks", st.Symlinks, "skipped", st.Skipped,
			"bytes", st.Bytes, "new_chunks", st.ChunksNew, "new_bytes", st.BytesNew,
			"deduplicated_chunks", st.ChunksDeduped, "packs_written", st.PacksWritten,
			"duration", time.Since(began))
	}
	closed = true
	errs = append(errs, set.Close(), r.Close(), s.close())
	return saved, errors.Join(errs...)
}

// pruneOnce applies the retention policy as the prune identity, with its
// own connection, and closes the repository.
func (e *env) pruneOnce(ctx context.Context, cfg *serviceConfig, log *slog.Logger) error {
	began := time.Now()
	s, st, err := e.connect(ctx, &cfg.prune.conn, &cfg.repo, 0)
	if err != nil {
		return err
	}
	pr, err := prune.Open(ctx, st, cfg.repo.repo, s.provider)
	if err != nil {
		return errors.Join(err, s.close())
	}
	rep, err := pr.Run(ctx, cfg.prune.policy, cfg.prune.opts)
	if err = errors.Join(err, pr.Close(), s.close()); err != nil {
		return err
	}
	for _, g := range rep.Groups {
		log.Info("prune group", "host", g.Host, "root", g.Root, "kept", len(g.Kept), "forgotten", len(g.Forgotten))
	}
	log.Info("prune finished", "kept", len(rep.Kept), "forgotten", len(rep.Forgotten),
		"marked_packs", rep.Marked, "removed_packs", len(rep.Removed), "removed_bytes", rep.RemovedBytes,
		"partial_packs", rep.Partial, "unused_bytes", rep.UnusedBytes, "duration", time.Since(began))
	return nil
}
