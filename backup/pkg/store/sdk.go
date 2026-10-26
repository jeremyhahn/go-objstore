package store

import (
	"context"
	"errors"
	"io"
	"sort"

	objstore "github.com/jeremyhahn/go-objstore/api/sdks/go"
)

// SDK is a Store backed by a go-objstore server through the Go client SDK,
// over any protocol the SDK speaks.
//
// The SDK has no conditional put, so Put checks Exists first. The backup
// identity is a repository's only writer, so nothing races that check; a
// server-side WORM policy or a 409 from the server is reported as ErrExists
// too.
type SDK struct {
	c objstore.Client
}

// NewSDK returns a Store over c. The caller still owns and closes c.
func NewSDK(c objstore.Client) (*SDK, error) {
	if c == nil {
		return nil, newError("new", "", ErrBackend, nil)
	}
	return &SDK{c: c}, nil
}

// Put writes data under name, once.
func (s *SDK) Put(ctx context.Context, name string, data []byte) error {
	const op = "put"
	if err := check(ctx, op, name); err != nil {
		return err
	}
	exists, err := s.c.Exists(ctx, name)
	if err != nil {
		return mapError(op, name, err)
	}
	if exists {
		return newError(op, name, ErrExists, nil)
	}
	if _, err := s.c.Put(ctx, name, data, nil); err != nil {
		return mapError(op, name, err)
	}
	return nil
}

// Get returns the object's bytes.
func (s *SDK) Get(ctx context.Context, name string) ([]byte, error) {
	const op = "get"
	if err := check(ctx, op, name); err != nil {
		return nil, err
	}
	res, err := s.c.Get(ctx, name)
	if err != nil {
		return nil, mapError(op, name, err)
	}
	if res == nil {
		return nil, newError(op, name, ErrBackend, nil)
	}
	if res.Reader == nil {
		return res.Data, nil
	}
	data, err := io.ReadAll(res.Reader)
	if cerr := res.Reader.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, newError(op, name, ErrBackend, err)
	}
	return data, nil
}

// List returns the names that begin with prefix, sorted, reading every page.
func (s *SDK) List(ctx context.Context, prefix string) ([]string, error) {
	const op = "list"
	var names []string
	opts := &objstore.ListOptions{Prefix: prefix}
	for {
		res, err := s.c.List(ctx, opts)
		if err != nil {
			return nil, mapError(op, prefix, err)
		}
		if res == nil {
			return nil, newError(op, prefix, ErrBackend, nil)
		}
		for _, o := range res.Objects {
			if o != nil {
				names = append(names, o.Key)
			}
		}
		if !res.Truncated || res.NextToken == "" || res.NextToken == opts.ContinueFrom {
			break
		}
		opts = &objstore.ListOptions{Prefix: prefix, ContinueFrom: res.NextToken}
	}
	sort.Strings(names)
	return names, nil
}

// Exists reports whether name exists.
func (s *SDK) Exists(ctx context.Context, name string) (bool, error) {
	const op = "exists"
	if err := check(ctx, op, name); err != nil {
		return false, err
	}
	ok, err := s.c.Exists(ctx, name)
	if err != nil {
		return false, mapError(op, name, err)
	}
	return ok, nil
}

// Remove deletes name on the server.
func (s *SDK) Remove(ctx context.Context, name string) error {
	const op = "remove"
	if err := check(ctx, op, name); err != nil {
		return err
	}
	if err := s.c.Delete(ctx, name); err != nil {
		return mapError(op, name, err)
	}
	return nil
}

var _ PruneStore = (*SDK)(nil)

// mapError maps the SDK's sentinels onto the store's.
func mapError(op, name string, err error) *Error {
	switch {
	case errors.Is(err, objstore.ErrObjectNotFound):
		return newError(op, name, ErrNotFound, err)
	case errors.Is(err, objstore.ErrAlreadyExists):
		return newError(op, name, ErrExists, err)
	default:
		return newError(op, name, ErrBackend, err)
	}
}
