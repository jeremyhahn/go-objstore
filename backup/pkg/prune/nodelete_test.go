package prune

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBackupCannotRemove checks the sources of the backup and restore paths,
// and of the packages they build on, never name the capability to remove
// objects: no reference to store.PruneStore, no interface with a Remove
// method to assert a store to, and no import of prune.
func TestBackupCannotRemove(t *testing.T) {
	fset := token.NewFileSet()
	for _, dir := range []string{"../backup", "../restore", "../repo", "../chunker", "../keyseam", "../seal"} {
		names, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil || len(names) == 0 {
			t.Fatalf("no sources in %s: %v", dir, err)
		}
		for _, name := range names {
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			src, err := os.ReadFile(name)
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			f, err := parser.ParseFile(fset, name, src, 0)
			if err != nil {
				t.Fatalf("ParseFile: %v", err)
			}
			for _, imp := range f.Imports {
				if strings.HasSuffix(strings.Trim(imp.Path.Value, `"`), "/pkg/prune") {
					t.Errorf("%s imports prune", name)
				}
			}
			ast.Inspect(f, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.Ident:
					if n.Name == "PruneStore" {
						t.Errorf("%s: names PruneStore", fset.Position(n.Pos()))
					}
				case *ast.InterfaceType:
					for _, m := range n.Methods.List {
						for _, id := range m.Names {
							if id.Name == "Remove" {
								t.Errorf("%s: declares an interface with Remove", fset.Position(id.Pos()))
							}
						}
					}
				}
				return true
			})
		}
	}
}
