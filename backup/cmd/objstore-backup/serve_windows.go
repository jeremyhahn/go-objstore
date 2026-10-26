//go:build windows

package main

import (
	"context"

	"golang.org/x/sys/windows/svc"
)

// serviceName is the name the service is installed under.
const serviceName = "objstore-backup"

// serve runs the service under the Windows service control manager when
// the process was started by it, and in the foreground otherwise.
func serve(ctx context.Context, run func(ctx context.Context) error) error {
	isService, err := svc.IsWindowsService()
	if err != nil {
		return err
	}
	if !isService {
		return run(ctx)
	}
	h := &handler{ctx: ctx, run: run}
	if err := svc.Run(serviceName, h); err != nil {
		return err
	}
	return h.err
}

// handler answers the service control manager: a stop or shutdown request
// cancels the run's context, and the service stops once run returns.
type handler struct {
	ctx context.Context
	run func(ctx context.Context) error
	err error
}

func (h *handler) Execute(_ []string, req <-chan svc.ChangeRequest, status chan<- svc.Status) (bool, uint32) {
	status <- svc.Status{State: svc.StartPending}
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.run(ctx) }()
	running := svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptShutdown}
	status <- running
	for {
		select {
		case err := <-done:
			h.err = err
			status <- svc.Status{State: svc.StopPending}
			if err != nil {
				return true, 1
			}
			return false, 0
		case c := <-req:
			switch c.Cmd {
			case svc.Interrogate:
				status <- c.CurrentStatus
			case svc.Stop, svc.Shutdown:
				status <- svc.Status{State: svc.StopPending}
				cancel()
			default:
			}
		}
	}
}
