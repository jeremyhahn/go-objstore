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

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("usage: mergecov <profile1> [<profile2> ...]")
	}
	lines := make(map[string]struct{})
	mode := ""
	for _, p := range os.Args[1:] {
		f, err := os.Open(p) // #nosec G304 -- CLI tool intentionally opens user-specified coverage files
		if err != nil {
			log.Fatalf("open %s: %v", p, err)
		}
		s := bufio.NewScanner(f)
		first := true
		for s.Scan() {
			t := s.Text()
			if first {
				// header like: mode: set
				if mode == "" {
					mode = t
				}
				first = false
				continue
			}
			lines[t] = struct{}{}
		}
		_ = f.Close() // #nosec G104 -- Errors in deferred cleanup are non-critical for this CLI tool
		if err := s.Err(); err != nil {
			log.Fatalf("scan %s: %v", p, err)
		}
	}
	if mode == "" {
		mode = "mode: set"
	}
	fmt.Println(mode)
	for l := range lines {
		if l == "" {
			continue
		}
		fmt.Println(l)
	}
}
