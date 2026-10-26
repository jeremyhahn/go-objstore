package prune

import (
	"bytes"
	"cmp"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
)

// Policy says which snapshots to keep. It applies to each group of snapshots
// with the same host and root on its own: within a group, a snapshot is kept
// when any rule keeps it, and every other snapshot is forgotten. Times are the
// ones sealed inside the manifests, in UTC.
type Policy struct {
	// Last keeps the newest Last snapshots.
	Last int
	// Within keeps every snapshot taken within Within before the run.
	Within time.Duration
	// Daily, Weekly, Monthly and Yearly keep the newest snapshot of each of
	// that many most recent days, ISO weeks, months and years that hold a
	// snapshot.
	Daily, Weekly, Monthly, Yearly int
}

func (p Policy) validate() error {
	if p.Last < 0 || p.Within < 0 || p.Daily < 0 || p.Weekly < 0 || p.Monthly < 0 || p.Yearly < 0 {
		return newError("check policy", "", ErrInvalidOptions, nil)
	}
	return nil
}

// dated is a snapshot and the time its manifest records.
type dated struct {
	id    seal.ID
	time  time.Time
	group source
}

// source is what snapshots are grouped by: the host and the directory backed
// up. The policy applies to each group on its own.
type source struct {
	host, root string
}

// Group is one host's snapshots of one directory and what the policy did
// with them.
type Group struct {
	// Host and Root are the host and the directory the snapshots hold.
	Host, Root string
	// Kept and Forgotten are the group's snapshots the policy kept and
	// forgot.
	Kept, Forgotten []seal.ID
}

// String names the group as host:root.
func (g Group) String() string {
	return g.Host + ":" + g.Root
}

// keepGroups applies the policy to each group of snapshots with the same
// host and root independently, so one source's snapshots never take the
// place of another's. It returns the groups ordered by host and root, each
// listing its snapshots in the order given.
func (p Policy) keepGroups(snaps []dated, now time.Time) []Group {
	bySource := make(map[source][]dated)
	for _, s := range snaps {
		bySource[s.group] = append(bySource[s.group], s)
	}
	keys := slices.SortedFunc(maps.Keys(bySource), func(a, b source) int {
		return cmp.Or(strings.Compare(a.host, b.host), strings.Compare(a.root, b.root))
	})
	groups := make([]Group, 0, len(keys))
	for _, k := range keys {
		in := bySource[k]
		kept := p.keep(in, now)
		g := Group{Host: k.host, Root: k.root}
		for _, s := range in {
			if kept[s.id] {
				g.Kept = append(g.Kept, s.id)
			} else {
				g.Forgotten = append(g.Forgotten, s.id)
			}
		}
		groups = append(groups, g)
	}
	return groups
}

// keep returns the snapshots of one group p keeps at now.
func (p Policy) keep(snaps []dated, now time.Time) map[seal.ID]bool {
	snaps = slices.Clone(snaps)
	slices.SortFunc(snaps, func(a, b dated) int {
		if c := b.time.Compare(a.time); c != 0 {
			return c
		}
		return bytes.Compare(a.id[:], b.id[:])
	})
	kept := make(map[seal.ID]bool)
	for i, s := range snaps {
		if i < p.Last || (p.Within > 0 && !s.time.Before(now.Add(-p.Within))) {
			kept[s.id] = true
		}
	}
	periods := []struct {
		n   int
		key func(time.Time) int
	}{
		{p.Daily, func(t time.Time) int { return t.Year()*1000 + t.YearDay() }},
		{p.Weekly, func(t time.Time) int { y, w := t.ISOWeek(); return y*100 + w }},
		{p.Monthly, func(t time.Time) int { return t.Year()*100 + int(t.Month()) }},
		{p.Yearly, func(t time.Time) int { return t.Year() }},
	}
	for _, per := range periods {
		seen := 0
		last := 0
		for _, s := range snaps {
			if seen == per.n {
				break
			}
			if k := per.key(s.time.UTC()); seen == 0 || k != last {
				kept[s.id] = true
				seen++
				last = k
			}
		}
	}
	return kept
}
