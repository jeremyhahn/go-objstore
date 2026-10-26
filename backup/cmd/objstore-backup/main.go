// Command objstore-backup is the go-objstore backup agent. It backs a system up
// to a bucket on an objstore server, compressed, encrypted and incremental.
//
// This build carries the agent's cryptographic core only; it has no commands
// yet and prints its usage.
package main

import (
	"fmt"
	"io"
	"os"

	objstore "github.com/jeremyhahn/go-objstore/api/sdks/go"
)

const usage = `Usage: objstore-backup <command> [arguments]

objstore-backup backs a system up to a bucket on an objstore server.

Commands:
  help      Show this help

No backup commands are available in this build.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run executes the command line in args and returns the process exit code:
// 0 for help, 2 for a missing or unknown command.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "help", "-h", "-help", "--help":
		fmt.Fprint(stdout, usage)
		fmt.Fprintf(stdout, "\nBuilt with the go-objstore Go SDK %s.\n", objstore.Version)
		return 0
	default:
		fmt.Fprintf(stderr, "objstore-backup: unknown command %q\n\n", args[0])
		fmt.Fprint(stderr, usage)
		return 2
	}
}
