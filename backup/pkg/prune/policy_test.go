package prune

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
)

func TestPolicy(t *testing.T) {
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	// Snapshots, newest first: two today, one yesterday, one last week, one
	// last month, one last year.
	times := []time.Time{
		now.Add(-1 * time.Hour),      // 0: Mon 26 Oct, ISO week 44
		now.Add(-2 * time.Hour),      // 1: Mon 26 Oct
		now.Add(-26 * time.Hour),     // 2: Sun 25 Oct, ISO week 43
		now.Add(-8 * 24 * time.Hour), // 3: Sun 18 Oct, ISO week 42
		now.AddDate(0, -1, 0),        // 4: 26 Sep
		now.AddDate(-1, 0, 0),        // 5: 26 Oct 2025
		now.AddDate(-1, 0, 0).Add(-2 * time.Hour),
	}
	snaps := make([]dated, len(times))
	for i, at := range times {
		snaps[i] = dated{id: seal.ID{byte(i + 1)}, time: at}
	}
	// Shuffle so the order kept does not depend on the order listed.
	slices.Reverse(snaps)
	for name, tc := range map[string]struct {
		pol  Policy
		want []int
	}{
		"none":    {Policy{}, nil},
		"last":    {Policy{Last: 3}, []int{0, 1, 2}},
		"all":     {Policy{Last: 100}, []int{0, 1, 2, 3, 4, 5, 6}},
		"within":  {Policy{Within: 26 * time.Hour}, []int{0, 1, 2}},
		"daily":   {Policy{Daily: 3}, []int{0, 2, 3}},
		"weekly":  {Policy{Weekly: 2}, []int{0, 2}},
		"weekly+": {Policy{Weekly: 4}, []int{0, 2, 3, 4}},
		"monthly": {Policy{Monthly: 2}, []int{0, 4}},
		"yearly":  {Policy{Yearly: 5}, []int{0, 5}},
		"union":   {Policy{Last: 1, Within: 90 * time.Minute, Weekly: 2, Yearly: 2}, []int{0, 2, 5}},
		"union2":  {Policy{Last: 2, Daily: 2, Monthly: 3}, []int{0, 1, 2, 4, 5}},
	} {
		t.Run(name, func(t *testing.T) {
			kept := tc.pol.keep(snaps, now)
			var got []int
			for i := range times {
				if kept[seal.ID{byte(i + 1)}] {
					got = append(got, i)
				}
			}
			if !slices.Equal(got, tc.want) || len(kept) != len(tc.want) {
				t.Fatalf("kept %v, want %v", got, tc.want)
			}
		})
	}
	if kept := (Policy{Last: 1}).keep(nil, now); len(kept) != 0 {
		t.Fatalf("kept %v of no snapshots", kept)
	}
	// Snapshots taken at the same time are ordered by ID.
	same := []dated{{id: seal.ID{2}, time: now}, {id: seal.ID{1}, time: now}}
	if kept := (Policy{Last: 1}).keep(same, now); !kept[seal.ID{1}] || len(kept) != 1 {
		t.Fatalf("kept %v of two at one time", kept)
	}
}

func TestPolicyValidate(t *testing.T) {
	for _, pol := range []Policy{{Last: -1}, {Within: -1}, {Daily: -1}, {Weekly: -1}, {Monthly: -1}, {Yearly: -1}} {
		if err := pol.validate(); !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("%+v: validate = %v", pol, err)
		}
	}
	if err := (Policy{Last: 1}).validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}
