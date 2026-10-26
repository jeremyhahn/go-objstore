//go:build !windows

package main

import "context"

// serve runs the service in the foreground; systemd and launchd stop it
// with SIGTERM, which cancels ctx.
func serve(ctx context.Context, run func(ctx context.Context) error) error {
	return run(ctx)
}
