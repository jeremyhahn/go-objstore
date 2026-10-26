// Package prune removes the snapshots a retention policy forgets, and the
// index entries and packs only they used, from a snapshot repository.
//
// Prune is the only code that can remove objects: it runs with a
// store.PruneStore, a privileged capability the backup and restore paths
// never hold. Removal is a mark and sweep that needs no lock. Each run records
// the packs no kept snapshot uses in a sealed mark; a backup reads every mark
// before it loads the index and neither reuses a marked pack nor writes one
// under a marked name. A later run removes a pack only when it is still
// unreferenced and the newest earlier mark has named it for a full grace
// period, longer than any backup runs.
package prune

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jeremyhahn/go-objstore/backup/pkg/keyseam"
	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

// DefaultGrace is the grace period when Options.Grace is zero.
const DefaultGrace = 24 * time.Hour

// Options tune a run.
type Options struct {
	// Grace is how long a pack must stay marked before it is removed. It must
	// exceed the longest backup. Zero means DefaultGrace.
	Grace time.Duration
	// DryRun reports what the run would remove and writes and removes
	// nothing.
	DryRun bool
	// Now returns the run's time. Nil means time.Now.
	Now func() time.Time
}

// Report says what a run removed, or with DryRun what it would remove.
type Report struct {
	// DryRun is set when nothing was written or removed.
	DryRun bool
	// Kept and Forgotten are the snapshots the policy kept and forgot.
	Kept, Forgotten []seal.ID
	// Index is the index object that replaces the Superseded ones. It is the
	// zero ID when the run wrote none, and always with DryRun, when
	// Superseded lists every index read.
	Index      seal.ID
	Superseded []seal.ID
	// Marked counts the unreferenced packs the run marked and keeps for now.
	Marked int
	// Removed lists the packs removed, and RemovedBytes the bytes of the
	// chunks they held.
	Removed      []seal.ID
	RemovedBytes int64
	// Partial counts the kept packs that hold chunks no kept snapshot uses,
	// and UnusedBytes those chunks' bytes. Prune does not repack them.
	Partial     int
	UnusedBytes int64
}

// Pruner prunes one repository.
type Pruner struct {
	st store.PruneStore
	r  *repo.Repository
}

// Open opens the repository under prefix on st for pruning. Close the Pruner
// when done.
func Open(ctx context.Context, st store.PruneStore, prefix string, p keyseam.Provider) (*Pruner, error) {
	if st == nil {
		return nil, newError("open", "", ErrInvalidOptions, nil)
	}
	r, err := repo.Open(ctx, st, prefix, p)
	if err != nil {
		return nil, fromRepo("open", err)
	}
	return &Pruner{st: st, r: r}, nil
}

// Repository returns the repository the Pruner prunes. It cannot remove
// objects.
func (p *Pruner) Repository() *repo.Repository {
	return p.r
}

// Close closes the repository and destroys its keys.
func (p *Pruner) Close() error {
	return p.r.Close()
}

// run is the state of one prune run.
type run struct {
	p    *Pruner
	ctx  context.Context
	opts Options
	now  func() time.Time
	rep  Report
	// marks are the marks read, prev the newest of them.
	marks []seal.ID
	prev  *repo.Mark
	// indexes are the index objects read and their entries.
	indexes []seal.ID
	entries []repo.Entry
}

// Run forgets the snapshots pol does not keep and removes what only they
// used. Objects are removed in an order that leaves every kept snapshot
// restorable if the run stops part way: the new index is written first, then
// forgotten snapshots, superseded indexes, unreferenced packs and old marks
// are removed. Running again completes an interrupted run.
func (p *Pruner) Run(ctx context.Context, pol Policy, opts Options) (*Report, error) {
	if err := ctx.Err(); err != nil {
		return nil, newError("start", "", ErrCanceled, err)
	}
	if err := pol.validate(); err != nil {
		return nil, err
	}
	if opts.Grace < 0 {
		return nil, newError("check options", "", ErrInvalidOptions, nil)
	}
	if opts.Grace == 0 {
		opts.Grace = DefaultGrace
	}
	ru := &run{p: p, ctx: ctx, opts: opts, now: opts.Now}
	if ru.now == nil {
		ru.now = time.Now
	}
	ru.rep.DryRun = opts.DryRun
	if err := ru.run(pol); err != nil {
		return nil, err
	}
	return &ru.rep, nil
}

func (ru *run) run(pol Policy) error {
	r := ru.p.r
	start := ru.now().UTC()
	if err := ru.readMarks(); err != nil {
		return err
	}
	// Snapshots are listed before indexes and indexes before packs, so every
	// snapshot seen has its index listed and every index its packs.
	snapIDs, err := r.ListSnapshots(ru.ctx)
	if err != nil {
		return fromRepo("list snapshots", err)
	}
	if ru.indexes, err = r.ListIndexes(ru.ctx); err != nil {
		return fromRepo("list indexes", err)
	}
	packIDs, err := r.ListPacks(ru.ctx)
	if err != nil {
		return fromRepo("list packs", err)
	}

	snaps := make([]dated, 0, len(snapIDs))
	manifests := make(map[seal.ID]*repo.Snapshot, len(snapIDs))
	for _, id := range snapIDs {
		s, err := r.ReadSnapshot(ru.ctx, id)
		if err != nil {
			return fromRepo("read snapshot", err)
		}
		manifests[id] = s
		snaps = append(snaps, dated{id: id, time: s.Time})
	}
	kept := pol.keep(snaps, start)
	if len(kept) == 0 {
		return newError("apply policy", "", ErrKeepsNothing, nil)
	}
	live := make(map[seal.ID]bool)
	for _, id := range snapIDs {
		if !kept[id] {
			ru.rep.Forgotten = append(ru.rep.Forgotten, id)
			continue
		}
		ru.rep.Kept = append(ru.rep.Kept, id)
		nodes := manifests[id].Nodes
		for i := range nodes {
			for _, c := range nodes[i].Chunks {
				live[c] = true
			}
		}
	}

	for _, id := range ru.indexes {
		entries, err := r.ReadIndex(ru.ctx, id)
		if err != nil {
			return fromRepo("read index", err)
		}
		ru.entries = append(ru.entries, entries...)
	}
	// Every pack that holds a copy of a live chunk is live.
	livePacks := make(map[seal.ID]bool)
	indexed := make(map[seal.ID]bool)
	for _, e := range ru.entries {
		if live[e.Chunk] {
			livePacks[e.Pack] = true
			indexed[e.Chunk] = true
		}
	}
	for c := range live {
		if !indexed[c] {
			return newError("check index", "", ErrUnindexed, nil)
		}
	}

	since := make(map[seal.ID]time.Time)
	if ru.prev != nil {
		for _, m := range ru.prev.Packs {
			since[m.Pack] = m.Since
		}
	}
	marked := make(map[seal.ID]bool)
	removed := make(map[seal.ID]bool)
	for _, id := range packIDs {
		if livePacks[id] {
			continue
		}
		marked[id] = true
		if t, ok := since[id]; ok && !t.After(start.Add(-ru.opts.Grace)) {
			removed[id] = true
			ru.rep.Removed = append(ru.rep.Removed, id)
		} else {
			ru.rep.Marked++
		}
	}
	ru.account(live, livePacks, removed)

	if err := ru.reindex(marked, removed, since); err != nil {
		return err
	}
	if ru.opts.DryRun {
		return nil
	}
	markID, err := ru.writeMark(packIDs, marked, since)
	if err != nil {
		return err
	}
	if err := ru.remove(repo.KindSnapshot, ru.rep.Forgotten); err != nil {
		return err
	}
	if err := ru.remove(repo.KindIndex, ru.rep.Superseded); err != nil {
		return err
	}
	if err := ru.remove(repo.KindPack, ru.rep.Removed); err != nil {
		return err
	}
	old := slices.DeleteFunc(slices.Clone(ru.marks), func(id seal.ID) bool { return id == markID })
	return ru.remove(repo.KindMark, old)
}

// readMarks reads every mark and keeps the newest. A mark that does not
// open stops the run: without it a pack could be removed too early.
func (ru *run) readMarks() error {
	r := ru.p.r
	ids, err := r.ListMarks(ru.ctx)
	if err != nil {
		return fromRepo("list marks", err)
	}
	ru.marks = ids
	for _, id := range ids {
		m, err := r.ReadMark(ru.ctx, id)
		if err != nil {
			return fromRepo("read mark", err)
		}
		if ru.prev == nil || m.Time.After(ru.prev.Time) {
			ru.prev = m
		}
	}
	return nil
}

// account adds the bytes the run removes and the bytes kept packs hold for
// no kept snapshot.
func (ru *run) account(live, livePacks, removed map[seal.ID]bool) {
	type key struct{ pack, chunk seal.ID }
	seen := make(map[key]bool)
	partial := make(map[seal.ID]bool)
	named := make(map[seal.ID]bool)
	for _, e := range ru.entries {
		k := key{e.Pack, e.Chunk}
		if seen[k] {
			continue
		}
		seen[k] = true
		named[e.Pack] = true
		switch {
		case removed[e.Pack]:
			ru.rep.RemovedBytes += int64(e.Length)
		case livePacks[e.Pack] && !live[e.Chunk]:
			partial[e.Pack] = true
			ru.rep.UnusedBytes += int64(e.Length)
		}
	}
	ru.rep.Partial = len(partial)
	// A removed pack no index names is sized from its own trailer, when it
	// opens.
	for _, id := range ru.rep.Removed {
		if named[id] {
			continue
		}
		if pk, err := ru.p.r.LoadPack(ru.ctx, id); err == nil {
			for _, e := range pk.Entries {
				ru.rep.RemovedBytes += int64(e.Length)
			}
		}
	}
}

// reindex writes one index of the entries in packs that stay, when packs go
// or more than one index was read, and names the indexes it supersedes.
func (ru *run) reindex(marked, removed map[seal.ID]bool, since map[seal.ID]time.Time) error {
	if len(removed) == 0 && len(ru.indexes) < 2 {
		return nil
	}
	// One entry per chunk, preferring a pack no mark names, so a copy a
	// backup stored again after a mark wins and the marked one can go.
	rank := func(pack seal.ID) int {
		switch {
		case marked[pack]:
			return 2
		case !since[pack].IsZero():
			return 1
		default:
			return 0
		}
	}
	best := make(map[seal.ID]repo.Entry)
	for _, e := range ru.entries {
		if removed[e.Pack] {
			continue
		}
		if cur, ok := best[e.Chunk]; !ok || rank(e.Pack) < rank(cur.Pack) {
			best[e.Chunk] = e
		}
	}
	entries := make([]repo.Entry, 0, len(best))
	for _, e := range best {
		entries = append(entries, e)
	}
	slices.SortFunc(entries, func(a, b repo.Entry) int { return bytes.Compare(a.Chunk[:], b.Chunk[:]) })
	if len(entries) > 0 && !ru.opts.DryRun {
		id, err := ru.p.r.WriteIndex(ru.ctx, entries)
		if err != nil {
			return fromRepo("write index", err)
		}
		ru.rep.Index = id
	}
	for _, id := range ru.indexes {
		if id != ru.rep.Index || len(entries) == 0 {
			ru.rep.Superseded = append(ru.rep.Superseded, id)
		}
	}
	return nil
}

// writeMark records the packs marked, each with the time a run first marked
// it.
func (ru *run) writeMark(packIDs []seal.ID, marked map[seal.ID]bool, since map[seal.ID]time.Time) (seal.ID, error) {
	// The time is taken now, not at the start, so no backup that has not
	// read this mark starts later than the time recorded.
	now := ru.now().UTC()
	m := &repo.Mark{Time: now, Packs: []repo.Marked{}}
	for _, id := range packIDs {
		if !marked[id] {
			continue
		}
		t, ok := since[id]
		if !ok {
			t = now
		}
		m.Packs = append(m.Packs, repo.Marked{Pack: id, Since: t})
	}
	id, err := ru.p.r.WriteMark(ru.ctx, m)
	if err != nil {
		return seal.ID{}, fromRepo("write mark", err)
	}
	return id, nil
}

// remove deletes each object of kind k. An object already gone counts as
// removed, so a rerun completes an interrupted run.
func (ru *run) remove(k repo.Kind, ids []seal.ID) error {
	op := "remove " + kindName(k)
	for _, id := range ids {
		name := ru.p.r.Layout().Name(k, id)
		err := ru.p.st.Remove(ru.ctx, name)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			e := fromRepo(op, err)
			e.Object = name
			return e
		}
	}
	return nil
}

func kindName(k repo.Kind) string {
	switch k {
	case repo.KindSnapshot:
		return "snapshot"
	case repo.KindIndex:
		return "index"
	case repo.KindPack:
		return "pack"
	default:
		return "mark"
	}
}
