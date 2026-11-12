// Command objstore-backup is the go-objstore backup agent. It backs
// directories up to a repository on an objstore server, encrypted,
// deduplicated and incremental, and restores, verifies and prunes that
// repository.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	objstore "github.com/jeremyhahn/go-objstore/api/sdks/go"
	"github.com/jeremyhahn/go-objstore/backup/internal/schedule"
	"github.com/jeremyhahn/go-objstore/backup/pkg/srcsnap"
)

// Exit codes.
const (
	exitOK       = 0
	exitFailure  = 1
	exitUsage    = 2
	exitProblems = 3
)

const usage = `Usage: objstore-backup <command> [flags] [arguments]

objstore-backup backs directories up to a repository on an objstore server.
Every object it stores is encrypted under the repository's data key, which a
passphrase wraps.

Commands:
  init        Create a repository
  backup      Back up one or more directories
  snapshots   List the snapshots
  restore     Restore a snapshot into a directory
  verify      Check the repository's data
  prune       Forget snapshots by a retention policy and remove unused data
  key         List, add, remove or change the repository's passphrases
  service     Back up on a schedule from a configuration file
  help        Show this help, or a command's

Run "objstore-backup help <command>" or "objstore-backup <command> -h" for a
command's flags.

Exit status: 0 success, 1 failure, 2 usage error, 3 verify found problems.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// run executes the command line in args against an objstore server and
// returns the process exit code.
func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runWith(ctx, &env{stdin: stdin, stdout: stdout, stderr: stderr, open: openSDK}, args)
}

// env is what a command runs with. open is the store seam: it connects to
// the repository's server, and tests replace it with an in-memory store.
type env struct {
	stdin          io.Reader
	stdout, stderr io.Writer
	open           opener
	// clock is the service's time source; nil means the system clock.
	clock schedule.Clock
	// ran, if set, is called after each scheduled run of the service.
	ran func()
	// snap is the source snapshot environment; the zero value runs the
	// platform's tools.
	snap srcsnap.Env
}

// command is one subcommand. run receives the arguments after its name.
type command struct {
	run  func(ctx context.Context, e *env, args []string) int
	help string
}

var commands = map[string]command{
	"init":      {cmdInit, initHelp},
	"backup":    {cmdBackup, backupHelp},
	"snapshots": {cmdSnapshots, snapshotsHelp},
	"restore":   {cmdRestore, restoreHelp},
	"verify":    {cmdVerify, verifyHelp},
	"prune":     {cmdPrune, pruneHelp},
	"key":       {cmdKey, keyHelp},
	"service":   {cmdService, serviceHelp},
}

// runWith dispatches args to its command.
func runWith(ctx context.Context, e *env, args []string) int {
	if len(args) == 0 {
		fmt.Fprint(e.stderr, usage)
		return exitUsage
	}
	switch name := args[0]; name {
	case "help", "-h", "-help", "--help":
		if len(args) > 1 {
			if c, ok := commands[args[1]]; ok {
				return c.run(ctx, e, []string{"-h"})
			}
			return e.unknown(args[1])
		}
		fmt.Fprint(e.stdout, usage)
		fmt.Fprintf(e.stdout, "\nBuilt with the go-objstore Go SDK %s.\n", objstore.Version)
		return exitOK
	default:
		if c, ok := commands[name]; ok {
			return c.run(ctx, e, args[1:])
		}
		return e.unknown(name)
	}
}

func (e *env) unknown(name string) int {
	fmt.Fprintf(e.stderr, "objstore-backup: unknown command %q\n\n", name)
	fmt.Fprint(e.stderr, usage)
	return exitUsage
}

// usageError prints a usage error for command name and returns exitUsage.
func (e *env) usageError(name string, err error) int {
	fmt.Fprintf(e.stderr, "objstore-backup %s: %v\nRun \"objstore-backup %s -h\" for usage.\n", name, err, name)
	return exitUsage
}

// fail prints err, a typed error from the library or this command, and
// returns exitFailure. The library's errors name operations and objects and
// never carry key material or file content; this command's name the source
// of a secret and never its bytes.
func (e *env) fail(name string, err error) int {
	msg := strings.ReplaceAll(err.Error(), "\n", "\n  ")
	fmt.Fprintf(e.stderr, "objstore-backup %s: %s\n", name, msg)
	return exitFailure
}
