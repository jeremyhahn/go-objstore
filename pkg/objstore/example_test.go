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

package objstore_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
)

// ExampleInitialize configures the facade with a single in-memory backend
// and stores and reads back an object through it.
func ExampleInitialize() {
	objstore.Reset()
	defer objstore.Reset()

	err := objstore.Initialize(&objstore.FacadeConfig{
		BackendConfigs: map[string]objstore.BackendConfig{
			"default": {Type: "memory"},
		},
		DefaultBackend: "default",
	})
	if err != nil {
		fmt.Println("initialize:", err)
		return
	}

	if err := objstore.Put("greeting.txt", strings.NewReader("hello, objstore")); err != nil {
		fmt.Println("put:", err)
		return
	}

	rc, err := objstore.Get("greeting.txt")
	if err != nil {
		fmt.Println("get:", err)
		return
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		fmt.Println("read:", err)
		return
	}
	fmt.Println(string(data))
	// Output: hello, objstore
}

// ExampleInitialize_routing configures two named backends and routes
// "backend:key" references to them; a plain key goes to the default backend.
func ExampleInitialize_routing() {
	objstore.Reset()
	defer objstore.Reset()

	dir, err := os.MkdirTemp("", "objstore-example-")
	if err != nil {
		fmt.Println("mkdirtemp:", err)
		return
	}
	defer os.RemoveAll(dir)

	err = objstore.Initialize(&objstore.FacadeConfig{
		BackendConfigs: map[string]objstore.BackendConfig{
			"hot":  {Type: "memory"},
			"cold": {Type: "local", Settings: map[string]string{"path": dir}},
		},
		DefaultBackend: "hot",
	})
	if err != nil {
		fmt.Println("initialize:", err)
		return
	}

	ctx := context.Background()
	if err := objstore.PutWithContext(ctx, "cold:report.txt", strings.NewReader("quarterly")); err != nil {
		fmt.Println("put cold:", err)
		return
	}
	if err := objstore.PutWithContext(ctx, "note.txt", strings.NewReader("remember")); err != nil {
		fmt.Println("put default:", err)
		return
	}

	for _, ref := range []string{"cold:report.txt", "hot:report.txt", "hot:note.txt"} {
		ok, err := objstore.Exists(ctx, ref)
		if err != nil {
			fmt.Println("exists:", err)
			return
		}
		fmt.Println(ref, ok)
	}
	// Output:
	// cold:report.txt true
	// hot:report.txt false
	// hot:note.txt true
}

// ExampleBackends lists the configured backend names. The facade keeps its
// backends in a map, so the names are sorted for a stable order.
func ExampleBackends() {
	objstore.Reset()
	defer objstore.Reset()

	err := objstore.Initialize(&objstore.FacadeConfig{
		BackendConfigs: map[string]objstore.BackendConfig{
			"primary":   {Type: "memory"},
			"secondary": {Type: "memory"},
		},
		DefaultBackend: "primary",
	})
	if err != nil {
		fmt.Println("initialize:", err)
		return
	}

	names := objstore.Backends()
	sort.Strings(names)
	fmt.Println(names)
	// Output: [primary secondary]
}

// ExampleGet_notFound shows that a missing key is reported with
// common.ErrKeyNotFound.
func ExampleGet_notFound() {
	objstore.Reset()
	defer objstore.Reset()

	err := objstore.Initialize(&objstore.FacadeConfig{
		BackendConfigs: map[string]objstore.BackendConfig{
			"default": {Type: "memory"},
		},
		DefaultBackend: "default",
	})
	if err != nil {
		fmt.Println("initialize:", err)
		return
	}

	rc, err := objstore.Get("missing.txt")
	if rc != nil {
		_ = rc.Close()
	}
	fmt.Println(errors.Is(err, common.ErrKeyNotFound))
	// Output: true
}

// ExampleBackend_unknown shows the errors for an unknown backend name and
// for a facade that has not been initialized.
func ExampleBackend_unknown() {
	objstore.Reset()
	defer objstore.Reset()

	err := objstore.Initialize(&objstore.FacadeConfig{
		BackendConfigs: map[string]objstore.BackendConfig{
			"default": {Type: "memory"},
		},
		DefaultBackend: "default",
	})
	if err != nil {
		fmt.Println("initialize:", err)
		return
	}

	_, err = objstore.Backend("missing")
	fmt.Println(errors.Is(err, objstore.ErrBackendNotFound))

	objstore.Reset()
	rc, err := objstore.Get("anything.txt")
	if rc != nil {
		_ = rc.Close()
	}
	fmt.Println(errors.Is(err, objstore.ErrNotInitialized))
	// Output:
	// true
	// true
}
