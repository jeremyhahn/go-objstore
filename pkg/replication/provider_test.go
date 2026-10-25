// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed:
//
// 1. GNU Affero General Public License v3.0 (AGPL-3.0)
//    See LICENSE file or visit https://www.gnu.org/licenses/agpl-3.0.html
//
// 2. Commercial License
//    Contact licensing@automatethethings.com for commercial licensing options.

package replication

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-enclave"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/auditlog"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

const (
	providerSourceBackend = "provider-probe-source"
	providerDestBackend   = "provider-probe-destination"
)

// configured is what a backend saw in one Configure call.
type configured struct {
	key, secret       string
	hasKey, hasSecret bool
	settings          map[string]string
}

// backendRecorder registers a backend that records each Configure call and
// then behaves as a memory backend.
type backendRecorder struct {
	mu    sync.Mutex
	calls []configured
}

func registerRecorder(name string) *backendRecorder {
	r := &backendRecorder{}
	factory.RegisterStorage(name, func(settings map[string]string, creds common.Credentials) (common.Storage, error) {
		c := configured{hasKey: creds.Key != nil, hasSecret: creds.Secret != nil, settings: map[string]string{}}
		for k, v := range settings {
			c.settings[k] = v
		}
		err := creds.Open(func(key, secret []byte) error {
			c.key, c.secret = string(key), string(secret)
			return nil
		})
		r.mu.Lock()
		r.calls = append(r.calls, c)
		r.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return factory.NewStorage("memory", nil, common.Credentials{})
	})
	return r
}

func (r *backendRecorder) snapshot() []configured {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]configured(nil), r.calls...)
}

func providerPolicy(id string) common.ReplicationPolicy {
	return common.ReplicationPolicy{
		ID:                  id,
		SourceBackend:       providerSourceBackend,
		SourceSettings:      map[string]string{"region": "eu-1"},
		DestinationBackend:  providerDestBackend,
		DestinationSettings: map[string]string{"region": "eu-2"},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeTransparent,
	}
}

func newHandle(t *testing.T, value string) enclave.ByteAccess {
	t.Helper()
	h, err := enclave.NewMemory([]byte(value))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	return h
}

// recordingProvider issues a fresh handle pair per call and remembers the
// targets it was asked about and the handles it issued.
type recordingProvider struct {
	t       *testing.T
	mu      sync.Mutex
	targets []CredentialTarget
	issued  []common.Credentials
	failOn  *CredentialTarget
	failErr error
}

func (p *recordingProvider) Credentials(_ context.Context, target CredentialTarget) (common.Credentials, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.targets = append(p.targets, target)
	if p.failOn != nil && *p.failOn == target {
		return common.Credentials{}, p.failErr
	}
	creds := common.Credentials{
		Key:    newHandle(p.t, "AK-"+string(target.Side)),
		Secret: newHandle(p.t, "SK-"+string(target.Side)),
	}
	p.issued = append(p.issued, creds)
	return creds, nil
}

func newProviderManager(t *testing.T, fs FileSystem, opts ...ManagerOption) *PersistentReplicationManager {
	t.Helper()
	mgr, err := NewPersistentReplicationManager(fs, "provider-policies.json", 5*time.Minute,
		adapters.NewNoOpLogger(), auditlog.NewNoOpAuditLogger(), opts...)
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	return mgr
}

func handleDestroyed(creds common.Credentials) bool {
	var unavailable *common.CredentialUnavailableError
	return errors.As(creds.Open(func(_, _ []byte) error { return nil }), &unavailable)
}

func TestProviderHandleReachesBackendForSourceAndDestination(t *testing.T) {
	src, dst := registerRecorder(providerSourceBackend), registerRecorder(providerDestBackend)
	provider := &recordingProvider{t: t}
	mgr := newProviderManager(t, newMockFileSystem(), WithCredentialProvider(provider))
	if err := mgr.AddPolicy(providerPolicy("p1")); err != nil {
		t.Fatalf("AddPolicy: %v", err)
	}

	if _, err := mgr.SyncPolicy(context.Background(), "p1"); err != nil {
		t.Fatalf("SyncPolicy: %v", err)
	}

	wantTargets := []CredentialTarget{{PolicyID: "p1", Side: SourceSide}, {PolicyID: "p1", Side: DestinationSide}}
	if len(provider.targets) != 2 || provider.targets[0] != wantTargets[0] || provider.targets[1] != wantTargets[1] {
		t.Errorf("provider asked for %v, want %v", provider.targets, wantTargets)
	}
	for side, rec := range map[TargetSide]*backendRecorder{SourceSide: src, DestinationSide: dst} {
		calls := rec.snapshot()
		if len(calls) != 1 {
			t.Fatalf("%s backend configured %d times, want 1", side, len(calls))
		}
		got := calls[0]
		if !got.hasKey || !got.hasSecret || got.key != "AK-"+string(side) || got.secret != "SK-"+string(side) {
			t.Errorf("%s credential did not reach Configure through the handles (key handle %t, secret handle %t)",
				side, got.hasKey, got.hasSecret)
		}
	}
	if len(src.snapshot()[0].settings) != 1 || src.snapshot()[0].settings["region"] != "eu-1" {
		t.Errorf("source settings = %v, want only region", src.snapshot()[0].settings)
	}
}

func TestProviderHandlesDestroyedAfterConfigure(t *testing.T) {
	registerRecorder(providerSourceBackend)
	registerRecorder(providerDestBackend)
	provider := &recordingProvider{t: t}
	mgr := newProviderManager(t, newMockFileSystem(), WithCredentialProvider(provider))
	if err := mgr.AddPolicy(providerPolicy("p1")); err != nil {
		t.Fatalf("AddPolicy: %v", err)
	}

	if _, err := mgr.SyncPolicy(context.Background(), "p1"); err != nil {
		t.Fatalf("SyncPolicy: %v", err)
	}
	if _, err := mgr.SyncPolicyParallel(context.Background(), "p1", 2); err != nil {
		t.Fatalf("SyncPolicyParallel: %v", err)
	}

	if len(provider.issued) != 4 {
		t.Fatalf("provider issued %d credentials, want 4 (one per target per sync)", len(provider.issued))
	}
	for i, creds := range provider.issued {
		if !handleDestroyed(creds) {
			t.Errorf("credential %d was not destroyed after its backend was configured", i)
		}
	}
}

func TestProviderHandlesDestroyedWhenBackendFails(t *testing.T) {
	provider := &recordingProvider{t: t}
	policy := providerPolicy("p1")
	policy.SourceBackend = "provider-probe-unregistered"

	_, err := NewSyncerWithProvider(context.Background(), provider, policy, nil, nil, nil,
		adapters.NewNoOpLogger(), auditlog.NewNoOpAuditLogger())

	if !errors.Is(err, factory.ErrUnknownBackend) {
		t.Fatalf("want ErrUnknownBackend, got %v", err)
	}
	if len(provider.issued) != 1 || !handleDestroyed(provider.issued[0]) {
		t.Errorf("the handle of a backend that failed to configure must still be destroyed")
	}
}

func TestProviderErrorFailsSyncWithTypedErrorWithoutCredential(t *testing.T) {
	for _, side := range []TargetSide{SourceSide, DestinationSide} {
		t.Run(string(side), func(t *testing.T) {
			registerRecorder(providerSourceBackend)
			dst := registerRecorder(providerDestBackend)
			cause := errors.New("vault said SK-leak")
			provider := &recordingProvider{t: t, failOn: &CredentialTarget{PolicyID: "p1", Side: side}, failErr: cause}
			mgr := newProviderManager(t, newMockFileSystem(), WithCredentialProvider(provider))
			if err := mgr.AddPolicy(providerPolicy("p1")); err != nil {
				t.Fatalf("AddPolicy: %v", err)
			}

			_, err := mgr.SyncPolicy(context.Background(), "p1")

			var provErr *CredentialProviderError
			if !errors.As(err, &provErr) || !errors.Is(err, ErrCredentialProvider) {
				t.Fatalf("want *CredentialProviderError, got %v", err)
			}
			if provErr.Target.Side != side || provErr.Target.PolicyID != "p1" {
				t.Errorf("error names target %v, want policy p1 %s", provErr.Target, side)
			}
			if !errors.Is(err, cause) {
				t.Error("the provider's error must stay reachable through Unwrap")
			}
			if strings.Contains(err.Error(), "SK-") || strings.Contains(err.Error(), "vault") {
				t.Errorf("error text carries provider content: %q", err.Error())
			}
			if side == SourceSide && len(dst.snapshot()) != 0 {
				t.Error("destination must not be configured after the source failed")
			}
		})
	}
}

func TestNilProviderKeepsDefaultChain(t *testing.T) {
	src, dst := registerRecorder(providerSourceBackend), registerRecorder(providerDestBackend)
	mgr := newProviderManager(t, newMockFileSystem(), WithCredentialProvider(nil))
	if err := mgr.AddPolicy(providerPolicy("p1")); err != nil {
		t.Fatalf("AddPolicy: %v", err)
	}

	if _, err := mgr.SyncPolicy(context.Background(), "p1"); err != nil {
		t.Fatalf("SyncPolicy: %v", err)
	}

	for name, rec := range map[string]*backendRecorder{"source": src, "destination": dst} {
		calls := rec.snapshot()
		if len(calls) != 1 || calls[0].hasKey || calls[0].hasSecret {
			t.Errorf("%s backend must be configured with no credential handle, got %+v", name, calls)
		}
	}
}

func TestPersistedPolicyFileHoldsNoProviderCredential(t *testing.T) {
	registerRecorder(providerSourceBackend)
	registerRecorder(providerDestBackend)
	fs := newMockFileSystem()
	provider := &recordingProvider{t: t}
	mgr := newProviderManager(t, fs, WithCredentialProvider(provider))
	if err := mgr.AddPolicy(providerPolicy("p1")); err != nil {
		t.Fatalf("AddPolicy: %v", err)
	}
	if _, err := mgr.SyncPolicy(context.Background(), "p1"); err != nil {
		t.Fatalf("SyncPolicy: %v", err)
	}

	data, err := fs.readFile("provider-policies.json")
	if err != nil {
		t.Fatalf("read policy file: %v", err)
	}
	for _, secret := range []string{"AK-source", "SK-source", "AK-destination", "SK-destination"} {
		if strings.Contains(string(data), secret) {
			t.Errorf("credential %q was persisted", secret)
		}
	}
	policy, err := mgr.GetPolicy("p1")
	if err != nil {
		t.Fatalf("GetPolicy: %v", err)
	}
	if len(policy.SourceSettings) != 1 || len(policy.DestinationSettings) != 1 {
		t.Errorf("a policy must hold only the settings it was added with, got %v and %v",
			policy.SourceSettings, policy.DestinationSettings)
	}
}

func TestCredentialProviderFuncAdaptsFunction(t *testing.T) {
	want := CredentialTarget{PolicyID: "p", Side: DestinationSide}
	var got CredentialTarget
	var p CredentialProvider = CredentialProviderFunc(func(_ context.Context, target CredentialTarget) (common.Credentials, error) {
		got = target
		return common.Credentials{}, nil
	})

	if _, err := p.Credentials(context.Background(), want); err != nil || got != want {
		t.Errorf("got target %v, err %v; want %v", got, err, want)
	}
}
