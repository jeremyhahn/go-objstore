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
	"fmt"
	"path/filepath"
	"strings"
)

// Names a backend keeps for itself next to the objects it stores. A key that
// would land on one of them is refused by ValidateKey, so a client can never
// read, overwrite or delete the store's own state through the object API.
const (
	// MetadataSuffix ends the name of the sidecar file that holds an
	// object's metadata (the local backend keeps it as <key>.metadata.json).
	MetadataSuffix = ".metadata.json"

	// TempFilePrefix starts the name of a file or directory that is being
	// written and has not been published yet.
	TempFilePrefix = ".tmp-"

	// LifecyclePolicyFile is the default name of the file that persists the
	// lifecycle policies.
	LifecyclePolicyFile = ".lifecycle-policies.json"

	// ReplicationPolicyFile is the default name of the file that persists
	// the replication policies.
	ReplicationPolicyFile = ".replication-policies.json"
)

// PolicyStagingName returns the name a policy file is written under before it
// is renamed over policyFile. The staging file sits beside the policy file
// with TempFilePrefix in front of its base name, so it falls in the reserved
// temp namespace for any policy file name: no object key can name it, and a
// client with object write access cannot plant content to be renamed over the
// policy file.
func PolicyStagingName(policyFile string) string {
	return filepath.Join(filepath.Dir(policyFile), TempFilePrefix+filepath.Base(policyFile))
}

// keySegments splits key into its path segments the way a filesystem would
// resolve them: both separators count, and empty and "." segments vanish.
func keySegments(key string) []string {
	parts := strings.FieldsFunc(key, func(r rune) bool { return r == '/' || r == '\\' })
	out := parts[:0]
	for _, p := range parts {
		if p != "." {
			out = append(out, p)
		}
	}
	return out
}

// SameKey reports whether a and b name the same file below a root once
// "./", backslashes and letter case are set aside.
func SameKey(a, b string) bool {
	sa, sb := keySegments(a), keySegments(b)
	if len(sa) != len(sb) || len(sa) == 0 {
		return false
	}
	for i := range sa {
		if !strings.EqualFold(sa[i], sb[i]) {
			return false
		}
	}
	return true
}

// reservedName returns a message when key names a file the backend keeps for
// itself: a metadata sidecar or an in-flight temp file at any depth, or one
// of the policy files at the top level.
func reservedName(key string) string {
	if SameKey(key, LifecyclePolicyFile) || SameKey(key, ReplicationPolicyFile) {
		return fmt.Sprintf("key %q is a reserved policy file name", key)
	}
	for _, seg := range keySegments(key) {
		lower := strings.ToLower(seg)
		if strings.HasSuffix(lower, MetadataSuffix) {
			return fmt.Sprintf("key cannot end a path segment in the reserved suffix %q", MetadataSuffix)
		}
		if strings.HasPrefix(lower, TempFilePrefix) {
			return fmt.Sprintf("key cannot start a path segment with the reserved prefix %q", TempFilePrefix)
		}
	}
	return ""
}
