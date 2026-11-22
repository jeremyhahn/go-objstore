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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

// ExampleNewStorage builds the in-memory backend from empty settings,
// stores a value and reads it back.
func ExampleNewStorage() {
	storage, err := factory.NewStorage("memory", map[string]string{}, common.Credentials{})
	if err != nil {
		fmt.Println("new storage:", err)
		return
	}

	if err := storage.Put("greeting.txt", strings.NewReader("hello")); err != nil {
		fmt.Println("put:", err)
		return
	}

	rc, err := storage.Get("greeting.txt")
	if err != nil {
		fmt.Println("get:", err)
		return
	}
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	if err != nil {
		fmt.Println("read:", err)
		return
	}
	fmt.Println(string(data))
	// Output: hello
}

// ExampleNewStorage_local builds the local filesystem backend rooted at a
// directory, stores an object with a content type and reads its metadata.
func ExampleNewStorage_local() {
	dir, err := os.MkdirTemp("", "objstore-example-")
	if err != nil {
		fmt.Println("mkdir:", err)
		return
	}
	defer func() { _ = os.RemoveAll(dir) }()

	storage, err := factory.NewStorage("local", map[string]string{"path": dir}, common.Credentials{})
	if err != nil {
		fmt.Println("new storage:", err)
		return
	}

	ctx := context.Background()
	meta := &common.Metadata{ContentType: "text/plain"}
	if err := storage.PutWithMetadata(ctx, "notes/readme.txt", strings.NewReader("hello, local"), meta); err != nil {
		fmt.Println("put:", err)
		return
	}

	got, err := storage.GetMetadata(ctx, "notes/readme.txt")
	if err != nil {
		fmt.Println("metadata:", err)
		return
	}
	fmt.Println(got.ContentType)
	fmt.Println(got.Size)
	// Output:
	// text/plain
	// 12
}

// ExampleNewStorage_archiveOnly shows that an archive-only backend cannot be
// built as primary storage, whichever backends the binary was built with.
func ExampleNewStorage_archiveOnly() {
	_, err := factory.NewStorage("glacier", map[string]string{}, common.Credentials{})
	fmt.Println(errors.Is(err, factory.ErrArchiveOnlyBackend))
	// Output: true
}

// ExampleNewStorage_missingKey shows that reading a key that does not exist
// returns an error wrapping common.ErrKeyNotFound.
func ExampleNewStorage_missingKey() {
	storage, err := factory.NewStorage("memory", map[string]string{}, common.Credentials{})
	if err != nil {
		fmt.Println("new storage:", err)
		return
	}

	_, err = storage.Get("does-not-exist")
	fmt.Println(errors.Is(err, common.ErrKeyNotFound))
	// Output: true
}

// ExampleListStorageBackends checks the registered primary storage backends.
// The full list depends on build tags, so only the always-present backends
// are checked.
func ExampleListStorageBackends() {
	backends := factory.ListStorageBackends()
	fmt.Println("memory:", slices.Contains(backends, "memory"))
	fmt.Println("local:", slices.Contains(backends, "local"))
	fmt.Println("glacier:", slices.Contains(backends, "glacier"))
	// Output:
	// memory: true
	// local: true
	// glacier: false
}
