package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jeremyhahn/go-objstore/backup/pkg/keyseam"
	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
)

const keyHelp = `Usage: objstore-backup key <subcommand> [flags] [arguments]

Manage the repository's key slots. Each slot is the data key wrapped under
one passphrase (with its own salt); any slot opens the repository. Snapshots
are never re-encrypted: only the wrap of the data key changes. The store is
write-once, so a key change writes a new key list and removes nothing: a
removed slot's object stays, but it no longer opens the repository.

Subcommands:
  list            List the key slots; * marks the one the passphrase opened
  add             Add a slot for the new passphrase
  remove <slot>   Remove a slot, named by its ID or a unique prefix of it;
                  the last slot cannot be removed
  passwd          Wrap the data key under the new passphrase and remove the
                  slot the current passphrase opened

The new passphrase of add and passwd comes from --new-passphrase-file or
--new-passphrase-stdin, never from the command line.
` + connHelp

var (
	errKeySubcommand = errors.New("a subcommand is required: list, add, remove or passwd")
	errNewPassphrase = errors.New("a new passphrase is required: --new-passphrase-file or --new-passphrase-stdin")
	errNewExclusive  = errors.New("--new-passphrase-file and --new-passphrase-stdin are exclusive")
	errBothStdin     = errors.New("--passphrase-stdin and --new-passphrase-stdin cannot both read standard input")
	errNoSlot        = errors.New("no key slot has that ID")
	errAmbiguousSlot = errors.New("more than one key slot has that ID prefix")
	errBadSlot       = errors.New("a key slot ID is hexadecimal")
)

// keyError names the subcommand argument a failure is about.
type keyError struct {
	arg string
	err error
}

func (e *keyError) Error() string { return "key slot " + strconv.Quote(e.arg) + ": " + e.err.Error() }

func (e *keyError) Unwrap() error { return e.err }

// unknownSubcommandError is a key subcommand that does not exist.
type unknownSubcommandError struct{ name string }

func (e *unknownSubcommandError) Error() string {
	return "unknown subcommand " + strconv.Quote(e.name) + ": not list, add, remove or passwd"
}

// newPassFlags are the flags of the passphrase a key change adds.
type newPassFlags struct {
	file       string
	stdin      bool
	iterations int
}

func (n *newPassFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&n.file, "new-passphrase-file", "",
		"file holding the new passphrase: a regular file with no group or other permissions")
	fs.BoolVar(&n.stdin, "new-passphrase-stdin", false, "read the new passphrase from standard input")
	fs.IntVar(&n.iterations, "iterations", keyseam.MinIterations,
		"PBKDF2 iterations deriving the key that wraps the data key under the new passphrase")
}

func (n *newPassFlags) check(r *repoFlags) error {
	switch {
	case n.file != "" && n.stdin:
		return errNewExclusive
	case n.file == "" && !n.stdin:
		return errNewPassphrase
	case n.stdin && r.passStdin:
		return errBothStdin
	case n.iterations < keyseam.MinIterations || n.iterations > keyseam.MaxIterations:
		return keyseam.ErrIterations
	}
	return nil
}

// provider builds the provider of the new passphrase, through the same
// --key-provider as the current one.
func (n *newPassFlags) provider(stdin io.Reader, r *repoFlags) (keyseam.ProviderCloser, error) {
	h, err := lockSecret(stdin, n.file, n.stdin, "new passphrase")
	if err != nil {
		return nil, err
	}
	p, err := keyseam.NewProvider(r.providerName(), keyseam.ProviderConfig{Secret: h, Iterations: n.iterations})
	if err != nil {
		return nil, errors.Join(err, h.Destroy())
	}
	return p, nil
}

func cmdKey(ctx context.Context, e *env, args []string) int {
	const name = "key"
	if len(args) == 0 {
		return e.usageError(name, errKeySubcommand)
	}
	sub, rest := args[0], args[1:]
	if sub == "-h" || sub == "-help" || sub == "--help" {
		fmt.Fprint(e.stdout, keyHelp)
		return exitOK
	}
	switch sub {
	case "list":
		return keyList(ctx, e, rest)
	case "add", "passwd":
		return keyChange(ctx, e, sub, rest)
	case "remove":
		return keyRemove(ctx, e, rest)
	}
	return e.usageError(name, &unknownSubcommandError{name: sub})
}

func keyList(ctx context.Context, e *env, args []string) int {
	const name = "key list"
	fs := newFlags(name)
	var o repoOpts
	o.register(fs)
	if _, code, ok := e.parse(name, keyHelp, fs, args, 0, 0); !ok {
		return code
	}
	if err := o.check(); err != nil {
		return e.usageError(name, err)
	}
	return e.withRepo(ctx, name, &o, func(r *repo.Repository, w io.Writer) (int, error) {
		keys, err := r.Keys(ctx)
		if err != nil {
			return exitFailure, err
		}
		for _, k := range keys {
			mark := " "
			if k == r.Slot() {
				mark = "*"
			}
			fmt.Fprintf(w, "%s %s\n", mark, k)
		}
		return exitOK, nil
	})
}

// keyChange runs add and passwd: both wrap the data key under a new
// passphrase; passwd also drops the slot that opened the repository.
func keyChange(ctx context.Context, e *env, sub string, args []string) int {
	name := "key " + sub
	fs := newFlags(name)
	var (
		o repoOpts
		n newPassFlags
	)
	o.register(fs)
	n.register(fs)
	if _, code, ok := e.parse(name, keyHelp, fs, args, 0, 0); !ok {
		return code
	}
	if err := errors.Join(o.check(), n.check(&o.repo)); err != nil {
		return e.usageError(name, err)
	}
	np, err := n.provider(e.stdin, &o.repo)
	if err != nil {
		return e.fail(name, err)
	}
	code := e.withRepo(ctx, name, &o, func(r *repo.Repository, w io.Writer) (int, error) {
		var (
			id  seal.ID
			err error
		)
		if sub == "passwd" {
			old := r.Slot()
			if id, err = r.ChangeKey(ctx, np); err != nil {
				return exitFailure, err
			}
			fmt.Fprintf(w, "added key slot %s, removed key slot %s\n", id, old)
			return exitOK, nil
		}
		if id, err = r.AddKey(ctx, np); err != nil {
			return exitFailure, err
		}
		fmt.Fprintf(w, "added key slot %s\n", id)
		return exitOK, nil
	})
	if err := np.Close(); err != nil && code == exitOK {
		return e.fail(name, err)
	}
	return code
}

func keyRemove(ctx context.Context, e *env, args []string) int {
	const name = "key remove"
	fs := newFlags(name)
	var o repoOpts
	o.register(fs)
	pos, code, ok := e.parse(name, keyHelp, fs, args, 1, 1)
	if !ok {
		return code
	}
	if err := o.check(); err != nil {
		return e.usageError(name, err)
	}
	return e.withRepo(ctx, name, &o, func(r *repo.Repository, w io.Writer) (int, error) {
		keys, err := r.Keys(ctx)
		if err != nil {
			return exitFailure, err
		}
		id, err := resolveSlot(keys, pos[0])
		if err != nil {
			return exitFailure, err
		}
		if err := r.RemoveKey(ctx, id); err != nil {
			return exitFailure, err
		}
		fmt.Fprintf(w, "removed key slot %s\n", id)
		return exitOK, nil
	})
}

// resolveSlot finds the one slot whose ID is s or begins with it.
func resolveSlot(keys []seal.ID, s string) (seal.ID, error) {
	p := strings.ToLower(s)
	if p == "" || strings.Trim(p, "0123456789abcdef") != "" {
		return seal.ID{}, &keyError{arg: s, err: errBadSlot}
	}
	var (
		found seal.ID
		n     int
	)
	for _, k := range keys {
		if strings.HasPrefix(k.String(), p) {
			found = k
			n++
		}
	}
	switch n {
	case 0:
		return seal.ID{}, &keyError{arg: s, err: errNoSlot}
	case 1:
		return found, nil
	}
	return seal.ID{}, &keyError{arg: s, err: errAmbiguousSlot}
}
