// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed:
//
// 1. GNU Affero General Public License v3.0 (AGPL-3.0)
//    See LICENSE file or visit https://www.gnu.org/licenses/agpl-3.0.html
//
// 2. Commercial License
//    Contact licensing@automatethethings.com for commercial licensing options.

package local

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// pageThrough lists every object with the given page size and returns the
// keys in order.
func pageThrough(t *testing.T, l *Local, pageSize int, onPage func(*common.ListResult)) []string {
	t.Helper()
	var keys []string
	token := ""
	for pages := 0; ; pages++ {
		if pages > 10000 {
			t.Fatal("listing never ended")
		}
		res, err := l.ListWithOptions(context.Background(), &common.ListOptions{MaxResults: pageSize, ContinueFrom: token})
		if err != nil {
			t.Fatal(err)
		}
		for _, obj := range res.Objects {
			keys = append(keys, obj.Key)
		}
		if onPage != nil {
			onPage(res)
		}
		if !res.Truncated {
			return keys
		}
		token = res.NextToken
	}
}

// TestLocal_ListPagingReadsEachObjectOnce: paging through a local listing
// reads each object's metadata once and visits the tree about once overall,
// not the whole tree on every page.
func TestLocal_ListPagingReadsEachObjectOnce(t *testing.T) {
	l := new(Local)
	if err := l.Configure(map[string]string{"path": t.TempDir()}, noCreds); err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// "d00-x" sorts before "d00/..." as a string but after it in the walk.
	const dirs, perDir = 20, 5
	var want int
	for d := range dirs {
		for f := range perDir {
			if err := l.Put(fmt.Sprintf("d%02d/f%d", d, f), bytes.NewBufferString("x")); err != nil {
				t.Fatal(err)
			}
			want++
		}
		if err := l.Put(fmt.Sprintf("d%02d-x", d), bytes.NewBufferString("x")); err != nil {
			t.Fatal(err)
		}
		want++
	}

	l.walkVisits.Store(0)
	all := pageThrough(t, l, 1000, nil)
	if len(all) != want {
		t.Fatalf("single page listed %d objects, want %d", len(all), want)
	}
	fullWalk := l.walkVisits.Load()

	l.walkVisits.Store(0)
	l.metadataReads.Store(0)
	pages := 0
	paged := pageThrough(t, l, 10, func(*common.ListResult) { pages++ })
	if fmt.Sprint(paged) != fmt.Sprint(all) {
		t.Fatalf("paged listing differs from the single page:\n got %v\nwant %v", paged, all)
	}
	if got := l.metadataReads.Load(); got != int64(want) {
		t.Errorf("paging read metadata %d times, want %d (once per object)", got, want)
	}
	// Rescanning the tree per page would visit fullWalk entries on every
	// page. Resuming skips the directories before the token, so a page
	// visits the top-level entries, one directory and the page itself.
	if got, rescan := l.walkVisits.Load(), fullWalk*int64(pages); got*3 > rescan {
		t.Errorf("paging %d pages visited %d entries, want under a third of a rescan per page (%d)", pages, got, rescan)
	}
}

// TestLocal_ListPagingResumesAfterDeletedToken: deleting the object the
// continuation token names, as ApplyPolicies may between pages, neither
// restarts the listing nor skips the objects after it.
func TestLocal_ListPagingResumesAfterDeletedToken(t *testing.T) {
	l := new(Local)
	if err := l.Configure(map[string]string{"path": t.TempDir()}, noCreds); err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for i := range 9 {
		if err := l.Put(fmt.Sprintf("k/%d", i), bytes.NewBufferString("x")); err != nil {
			t.Fatal(err)
		}
	}
	var seen []string
	seenSet := map[string]bool{}
	pageThrough(t, l, 3, func(res *common.ListResult) {
		for _, obj := range res.Objects {
			if seenSet[obj.Key] {
				t.Fatalf("%s listed twice", obj.Key)
			}
			seenSet[obj.Key] = true
			seen = append(seen, obj.Key)
		}
		if res.Truncated {
			if err := l.Delete(res.NextToken); err != nil {
				t.Fatal(err)
			}
		}
	})
	if len(seen) != 9 {
		t.Fatalf("listed %v, want all 9 keys once", seen)
	}
}
