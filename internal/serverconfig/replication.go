package serverconfig

import (
	"log/slog"
	"path/filepath"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
)

// ReplicationEnabler has the signature of objstore.EnableReplication; tests
// pass a fake so no file is opened.
// ReplicationConfig holds the server's replication settings.
type ReplicationConfig struct {
	// AllowClientLocalBackends permits replication policies with a filesystem-path backend; default false.
	AllowClientLocalBackends bool `mapstructure:"allow_client_local_backends"`
}

type ReplicationEnabler func(backendName string, cfg *objstore.ReplicationConfig) error

// ReplicationPolicyPath returns the default backend's policy file under c.Path, or "" when no path is set.
func (c *Config) ReplicationPolicyPath() string {
	if c.Path == "" {
		return ""
	}
	return filepath.Join(c.Path, common.ReplicationPolicyFile)
}

// EnableReplication enables replication with the policy file under c.Path and reports whether it did; failures are logged, not returned.
func (c *Config) EnableReplication(enable ReplicationEnabler) bool {
	policyPath := c.ReplicationPolicyPath()
	if policyPath == "" {
		slog.Info("Replication not enabled: no path configured for the replication policy file", "backend", c.Backend)
		return false
	}
	if enable == nil {
		enable = objstore.EnableReplication
	}
	if err := enable("", &objstore.ReplicationConfig{
		PolicyFilePath:           policyPath,
		AllowClientLocalBackends: c.Replication.AllowClientLocalBackends,
		RunInBackground:          false,
	}); err != nil {
		slog.Warn("Failed to enable replication", "error", err)
		return false
	}
	slog.Info("Replication enabled", "policy_file", policyPath)
	return true
}
