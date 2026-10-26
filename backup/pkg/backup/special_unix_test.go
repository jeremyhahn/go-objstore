//go:build unix

package backup

import (
	"syscall"
	"testing"
)

// mkSpecial makes a named pipe at path, an entry the backup skips.
func mkSpecial(t *testing.T, path string) bool {
	t.Helper()
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatalf("Mkfifo: %v", err)
	}
	return true
}
