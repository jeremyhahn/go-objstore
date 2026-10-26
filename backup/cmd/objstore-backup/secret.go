package main

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// maxSecret bounds a passphrase or token read from a file or standard input.
const maxSecret = 4096

var (
	errSecretMode  = errors.New("must be a regular file with no group or other permissions")
	errSecretEmpty = errors.New("is empty")
	errSecretSize  = errors.New("is longer than 4096 bytes")
)

// secretError is a failure to read a secret. It names the secret's source,
// never its content.
type secretError struct {
	what   string
	source string
	err    error
}

func (e *secretError) Error() string {
	return e.what + " " + e.source + ": " + e.err.Error()
}

func (e *secretError) Unwrap() error { return e.err }

// readSecretFile reads a secret from the file at path, which must be a
// regular file with no group or other permission bits. The caller clears
// the returned buffer.
func readSecretFile(path string) ([]byte, error) {
	path = filepath.Clean(path)
	// Stat before opening, so a FIFO or device is refused without blocking.
	info, err := os.Stat(path)
	if err != nil {
		return nil, pathless(err)
	}
	if !owned(info) {
		return nil, errSecretMode
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, pathless(err)
	}
	defer func() { _ = f.Close() }()
	// Check again on the open file, in case path changed in between.
	if info, err = f.Stat(); err != nil {
		return nil, pathless(err)
	}
	if !owned(info) {
		return nil, errSecretMode
	}
	return readSecret(f)
}

func owned(info fs.FileInfo) bool {
	return info.Mode().IsRegular() && info.Mode().Perm()&0o077 == 0
}

// pathless drops the path a file error repeats; the caller names it.
func pathless(err error) error {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return pe.Err
	}
	return err
}

// readSecret reads a secret of at most maxSecret bytes from r and strips one
// trailing line ending. The returned slice is the only copy the read makes:
// the caller clears it.
func readSecret(r io.Reader) ([]byte, error) {
	buf := make([]byte, maxSecret+1)
	n, err := io.ReadFull(r, buf)
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
	case err != nil:
		clear(buf)
		return nil, pathless(err)
	default:
		clear(buf)
		return nil, errSecretSize
	}
	s := bytes.TrimSuffix(buf[:n], []byte("\n"))
	s = bytes.TrimSuffix(s, []byte("\r"))
	if len(s) == 0 {
		clear(buf)
		return nil, errSecretEmpty
	}
	return s, nil
}
