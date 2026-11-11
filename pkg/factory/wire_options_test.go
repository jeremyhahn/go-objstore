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

package factory_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-objstore/internal/wireprobe"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

func TestErrClientLocalArchiverIsInvalidArgument(t *testing.T) {
	if !errors.Is(factory.ErrClientLocalArchiver, common.ErrInvalidArgument) {
		t.Fatal("ErrClientLocalArchiver must wrap common.ErrInvalidArgument")
	}
	if common.Classify(factory.ErrClientLocalArchiver) != common.CodeInvalidArgument {
		t.Fatal("ErrClientLocalArchiver must classify as invalid argument")
	}
}

func TestNewArchiverFromWireRefusesLocalByDefault(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "outside")
	wire := map[string]string{"path": dir}
	a, err := factory.NewArchiverFromWire("local", wire)
	if !errors.Is(err, factory.ErrClientLocalArchiver) {
		t.Fatalf("err = %v, want ErrClientLocalArchiver", err)
	}
	if a != nil {
		t.Fatal("a refused call must return no archiver")
	}
	if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
		t.Fatalf("a refused call must not create the destination directory (stat: %v)", statErr)
	}

	_, err = factory.NewArchiverFromWireOpts("local", map[string]string{"path": dir}, factory.WireOptions{})
	if !errors.Is(err, factory.ErrClientLocalArchiver) {
		t.Fatalf("zero WireOptions: err = %v, want ErrClientLocalArchiver", err)
	}
}

func TestNewArchiverFromWireOptsAllowsLocalWhenSet(t *testing.T) {
	dir := t.TempDir()
	a, err := factory.NewArchiverFromWireOpts("local", map[string]string{"path": dir},
		factory.WireOptions{AllowLocalDestinations: true})
	if err != nil {
		t.Fatalf("NewArchiverFromWireOpts: %v", err)
	}
	if a == nil {
		t.Fatal("want an archiver")
	}
}

func TestNewArchiverFromWireNonPathTypeUnaffected(t *testing.T) {
	wireprobe.Register(t)
	if _, err := factory.NewArchiverFromWire(wireprobe.Backend, map[string]string{"region": "eu-1"}); err != nil {
		t.Fatalf("NewArchiverFromWire: %v", err)
	}
	if factory.IsPathArchiverType(wireprobe.Backend) {
		t.Fatal("the probe backend takes no path")
	}
	if !factory.IsPathArchiverType("local") {
		t.Fatal("local takes a path")
	}
	if err := factory.CheckClientArchiverType("glacier", factory.WireOptions{}); err != nil {
		t.Fatalf("glacier: %v", err)
	}
}
