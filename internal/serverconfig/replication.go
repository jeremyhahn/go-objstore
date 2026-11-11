package serverconfig

import (
	"log/slog"
	"path/filepath"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
)

// ReplicationEnabler has the signature of objstore.EnableReplication; tests
// pass a fake so no file is opened.
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
	policyPath := c.ReplicationPolicyPath()
	if policyPath == "" {
		slog.Info("Replication not enabled: no path configured for the replication policy file", "backend", c.Backend)
		return false
	}
	if enable == nil {
		enable = objstore.EnableReplication
	}
	if err := enable("", &objstore.ReplicationConfig{
		PolicyFilePath:  policyPath,
		RunInBackground: false,
	}); err != nil {
		slog.Warn("Failed to enable replication", "error", err)
		return false
	}
	slog.Info("Replication enabled", "policy_file", policyPath)
	return true
}
