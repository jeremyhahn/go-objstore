// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQUICClient_NewClient(t *testing.T) {
	config := &ClientConfig{
		Protocol:           ProtocolQUIC,
		Address:            "localhost:4433",
		UseTLS:             true,
		InsecureSkipVerify: true,
		ConnectionTimeout:  10 * time.Second,
	}

	client, err := newQUICClient(config)
	require.NoError(t, err)
	assert.NotNil(t, client)
	assert.Equal(t, "https://localhost:4433", client.baseURL)
}

func TestQUICClient_NewClient_InvalidConfig(t *testing.T) {
	_, err := newQUICClient(nil)
	assert.Equal(t, ErrInvalidConfig, err)
}

func TestQUICClient_Close(t *testing.T) {
	config := &ClientConfig{
		Protocol:           ProtocolQUIC,
		Address:            "localhost:4433",
		UseTLS:             true,
		InsecureSkipVerify: true,
	}

	client, err := newQUICClient(config)
	require.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)
}

func TestQUICClient_Interface(t *testing.T) {
	// Verify QUICClient implements Client interface at compile time
	var _ Client = (*QUICClient)(nil)
}
