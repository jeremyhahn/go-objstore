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

package depguard

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"
)

const modulePath = "github.com/jeremyhahn/go-objstore"

// backendTags selects every backend implementation, so each backend package is
// resolved with its cloud SDK in the graph, as the unit-test target builds it.
const backendTags = "local,awss3,gcpstorage,azureblob,glacier,azurearchive"

// backends are the storage backend packages. Importing any one of them must
// not link the server-side stacks.
var backends = []string{
	"local",
	"memory",
	"s3",
	"gcs",
	"azure",
	"glacier",
	"azurearchive",
}

// forbidden are import paths (and everything beneath them) a backend must
// never link: the gin HTTP framework, the mongo bson codec and the ugorji
// codec, and the audit middleware and servers that bring them in. Backends
// take the audit contract from pkg/auditlog, which is not listed here.
var forbidden = []string{
	"github.com/gin-gonic/gin",
	"go.mongodb.org/mongo-driver",
	"github.com/ugorji/go",
	modulePath + "/pkg/audit",
	modulePath + "/pkg/server",
}

// isForbidden reports whether the import path is, or lies beneath, one of the
// forbidden import paths.
func isForbidden(path string) bool {
	for _, prefix := range forbidden {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

// listDeps returns the import path of every package the given package links.
func listDeps(t *testing.T, pkg string) []string {
	t.Helper()

	cmd := exec.Command("go", "list", "-deps", "-tags", backendTags, "-f", "{{.ImportPath}}", pkg)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go list -deps %s: %v\n%s", pkg, err, stderr.String())
	}
	return strings.Fields(stdout.String())
}

// TestBackendsDoNotLinkServerStacks fails if any backend package's dependency
// graph includes gin, the mongo driver, ugorji, or the audit/server packages
// that bring them in.
func TestBackendsDoNotLinkServerStacks(t *testing.T) {
	for _, backend := range backends {
		t.Run(backend, func(t *testing.T) {
			for _, dep := range listDeps(t, modulePath+"/pkg/"+backend) {
				if isForbidden(dep) {
					t.Errorf("pkg/%s links %s; a backend must depend only on pkg/auditlog, not pkg/audit or the server stacks", backend, dep)
				}
			}
		})
	}
}

func TestIsForbidden(t *testing.T) {
	cases := map[string]bool{
		"github.com/gin-gonic/gin":            true,
		"github.com/gin-gonic/gin/binding":    true,
		"go.mongodb.org/mongo-driver/bson":    true,
		"go.mongodb.org/mongo-driver/v2/bson": true,
		"github.com/ugorji/go/codec":          true,
		modulePath + "/pkg/audit":             true,
		modulePath + "/pkg/server/middleware": true,
		modulePath + "/pkg/auditlog":          false,
		modulePath + "/pkg/common":            false,
		"github.com/aws/aws-sdk-go-v2":        false,
		"google.golang.org/grpc/metadata":     false,
	}
	for path, want := range cases {
		if got := isForbidden(path); got != want {
			t.Errorf("isForbidden(%q) = %v, want %v", path, got, want)
		}
	}
}
