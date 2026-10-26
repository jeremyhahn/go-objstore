//go:build !unix

package backup

import "testing"

// mkSpecial makes no special file where named pipes are not available.
func mkSpecial(*testing.T, string) bool {
	return false
}
