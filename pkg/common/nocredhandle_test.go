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

package common

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// TestBackendsHoldNoCredentialHandle proves that no backend retains a
// credential handle: a Configure call borrows the handle, builds the SDK
// credential, and lets go. No struct field in a backend package may have the
// type enclave.ByteAccess or common.Credentials (directly, as a pointer,
// slice, array or map element, or embedded). Every file is parsed regardless
// of build tags.
func TestBackendsHoldNoCredentialHandle(t *testing.T) {
	backends := []string{"s3", "azure", "azurearchive", "gcs", "glacier", "local", "memory"}
	fset := token.NewFileSet()
	for _, b := range backends {
		dir := filepath.Join("..", b)
		pkgs, err := parser.ParseDir(fset, dir, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", dir, err)
		}
		for _, pkg := range pkgs {
			for path, file := range pkg.Files {
				if strings.HasSuffix(path, "_test.go") {
					continue
				}
				ast.Inspect(file, func(n ast.Node) bool {
					st, ok := n.(*ast.StructType)
					if !ok {
						return true
					}
					for _, f := range st.Fields.List {
						if name := credentialType(f.Type); name != "" {
							t.Errorf("%s: struct field of type %s retains a credential handle",
								fset.Position(f.Pos()), name)
						}
					}
					return true
				})
			}
		}
	}
}

// credentialType returns the offending type name, or "" when typ holds none.
func credentialType(typ ast.Expr) string {
	switch e := typ.(type) {
	case *ast.SelectorExpr:
		if x, ok := e.X.(*ast.Ident); ok {
			switch x.Name + "." + e.Sel.Name {
			case "enclave.ByteAccess", "common.Credentials":
				return x.Name + "." + e.Sel.Name
			}
		}
	case *ast.StarExpr:
		return credentialType(e.X)
	case *ast.ArrayType:
		return credentialType(e.Elt)
	case *ast.MapType:
		if n := credentialType(e.Key); n != "" {
			return n
		}
		return credentialType(e.Value)
	case *ast.Ident:
		if e.Name == "Credentials" || e.Name == "ByteAccess" {
			return e.Name
		}
	}
	return ""
}
