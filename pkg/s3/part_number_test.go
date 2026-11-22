//go:build awss3

package s3

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestPartNumber32(t *testing.T) {
	for _, n := range []int{1, 2, common.MaxParts} {
		got, err := partNumber32(n)
		if err != nil {
			t.Fatalf("partNumber32(%d): %v", n, err)
		}
		if int(got) != n {
			t.Fatalf("partNumber32(%d) = %d", n, got)
		}
	}
	for _, n := range []int{0, -1, common.MaxParts + 1, common.MaxParts * 1000} {
		if _, err := partNumber32(n); !errors.Is(err, common.ErrInvalidArgument) {
			t.Fatalf("partNumber32(%d) err = %v, want ErrInvalidArgument", n, err)
		}
	}
}
