package keyseam

import (
	"slices"
	"sync"

	enclave "github.com/jeremyhahn/go-enclave"
)

// PassphraseName is the name the passphrase provider is registered under,
// and the default provider.
const PassphraseName = "passphrase"

// ProviderCloser is a Provider that holds secrets it destroys on Close.
type ProviderCloser interface {
	Provider
	Close() error
}

// ProviderConfig is what a Factory builds a provider from.
type ProviderConfig struct {
	// Secret is the secret the operator supplied, such as a passphrase,
	// or nil. On success the provider owns it and destroys it on Close;
	// on failure the caller still owns it.
	Secret enclave.ByteAccess
	// Iterations is the key-derivation work factor, or 0 for the
	// provider's default. Providers that derive no key ignore it.
	Iterations int
}

// Factory builds a provider from its configuration.
type Factory func(cfg ProviderConfig) (ProviderCloser, error)

var (
	registryMu sync.RWMutex
	registry   = map[string]Factory{
		PassphraseName: func(cfg ProviderConfig) (ProviderCloser, error) {
			p, err := NewPassphraseProvider(cfg.Secret, cfg.Iterations)
			if err != nil {
				return nil, err
			}
			return p, nil
		},
	}
)

// Register adds a provider under name. A name already registered, an empty
// name or a nil factory fails with ErrProviderRegistered or ErrInvalidKey.
// Providers that live outside this module (such as a key-management
// service) register themselves through it; only the passphrase provider is
// built in.
func Register(name string, f Factory) error {
	const op = "register provider"
	if name == "" || f == nil {
		return newError(op, ErrInvalidKey, nil)
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, ok := registry[name]; ok {
		return newError(op, ErrProviderRegistered, nil)
	}
	registry[name] = f
	return nil
}

// NewProvider builds the provider registered under name. An unknown name
// fails with ErrUnknownProvider.
func NewProvider(name string, cfg ProviderConfig) (ProviderCloser, error) {
	registryMu.RLock()
	f, ok := registry[name]
	registryMu.RUnlock()
	if !ok {
		return nil, newError("new provider "+name, ErrUnknownProvider, nil)
	}
	return f(cfg)
}

// Providers returns the registered provider names, sorted.
func Providers() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
