// Command gencerts writes throwaway test TLS material into the directory given
// as its only argument (used by the server integration target).
package main

import (
	"fmt"
	"os"

	"github.com/jeremyhahn/go-objstore/internal/testutil/testcerts"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: gencerts <dir>")
		os.Exit(2)
	}
	if _, err := testcerts.Write(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, "gencerts:", err)
		os.Exit(1)
	}
}
