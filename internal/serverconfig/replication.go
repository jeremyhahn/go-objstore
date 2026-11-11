package serverconfig

import (
	"context"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
)

// ReplicationEnabler has the signature of objstore.EnableReplication; tests
// pass a fake so no file is opened.
// ReplicationConfig holds the server's replication settings.
type ReplicationConfig struct {
	// AllowClientLocalBackends permits replication policies whose source or
	// destination backend type takes a filesystem path (such as "local").
	// A policy's backend settings come from the client, so such a backend
	// could open any directory the service user can read. Default false.
	AllowClientLocalBackends bool `mapstructure:"allow_client_local_backends"`

	// Interval, when positive, runs every replication policy in the
	// background at this interval until the server shuts down. Zero (the
	// default) keeps replication trigger-only. Negative is refused.
	Interval time.Duration `mapstructure:"interval"`
}

type ReplicationEnabler func(backendName string, cfg *objstore.ReplicationConfig) error

// ReplicationPolicyPath returns the replication policy file of the default
// backend, filepath.Join(c.Path, common.ReplicationPolicyFile), or "" when no
// path is configured: the policy file is never built from an empty path, which
// would put it in the process's working directory.
func (c *Config) ReplicationPolicyPath() string {
	if c.Path == "" {
		return ""
	}
	return filepath.Join(c.Path, common.ReplicationPolicyFile)
}

// EnableReplication enables replication on the default backend with the policy
// file under c.Path, through enable (objstore.EnableReplication when nil). With
// no path it enables nothing, opens no file and logs why. It reports whether
// replication was enabled; a failure is logged, not returned, as backends
// without a replication manager are not an error for a server.
func (c *Config) EnableReplication(enable ReplicationEnabler) bool {
	return c.EnableReplicationContext(context.Background(), enable)
}

// EnableReplicationContext is EnableReplication with the context that bounds
// scheduled replication: when replication.interval is positive, the manager
// runs every policy in the background at that interval until ctx is done.
// With no interval nothing runs in the background and ctx is unused.
func (c *Config) EnableReplicationContext(ctx context.Context, enable ReplicationEnabler) bool {
	policyPath := c.ReplicationPolicyPath()
	if policyPath == "" {
		slog.Info("Replication not enabled: no path configured for the replication policy file", "backend", c.Backend)
		return false
	}
	if enable == nil {
		enable = objstore.EnableReplication
	}
	rc := &objstore.ReplicationConfig{
		PolicyFilePath:           policyPath,
		AllowClientLocalBackends: c.Replication.AllowClientLocalBackends,
		RunInBackground:          false,
		Logger:                   newSlogLogger(),
	}
	if c.Replication.Interval > 0 {
		rc.RunInBackground = true
		rc.Interval = c.Replication.Interval
		rc.Context = ctx
	}
	if err := enable("", rc); err != nil {
		slog.Warn("Failed to enable replication", "error", err)
		return false
	}
	if rc.RunInBackground {
		slog.Info("Replication enabled", "policy_file", policyPath, "interval", rc.Interval.String())
	} else {
		slog.Info("Replication enabled", "policy_file", policyPath)
	}
	return true
}
