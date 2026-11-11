package local

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestList_SkipsReservedAndPolicyFiles checks that List and ListWithOptions
// never return a name Get, Put and Delete refuse: the reserved policy files
// and the store's custom lifecycle policy file.
func TestList_SkipsReservedAndPolicyFiles(t *testing.T) {
	dir := t.TempDir()
	const customPolicy = "custom-lifecycle.json"
	s := New()
	if err := s.Configure(map[string]string{
		"path":                 dir,
		"lifecycleManagerType": "persistent",
		"lifecyclePolicyFile":  customPolicy,
	}, noCreds); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	l := s.(*Local)

	objects := []string{"a.txt", "dir/b.txt"}
	for _, key := range objects {
		if err := l.Put(key, strings.NewReader("data")); err != nil {
			t.Fatalf("Put(%q): %v", key, err)
		}
	}
	for _, name := range []string{common.ReplicationPolicyFile, common.LifecyclePolicyFile, customPolicy} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := l.Get(name); err == nil {
			t.Errorf("Get(%q) succeeded on a policy file", name)
		}
	}

	keys, err := l.List("")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	slices.Sort(keys)
	if !slices.Equal(keys, objects) {
		t.Errorf("List = %v, want %v", keys, objects)
	}

	res, err := l.ListWithOptions(context.Background(), &common.ListOptions{})
	if err != nil {
		t.Fatalf("ListWithOptions: %v", err)
	}
	var got []string
	for _, o := range res.Objects {
		got = append(got, o.Key)
	}
	slices.Sort(got)
	if !slices.Equal(got, objects) {
		t.Errorf("ListWithOptions = %v, want %v", got, objects)
	}
}
