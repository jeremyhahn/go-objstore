// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRESTClient_NewClient(t *testing.T) {
	config := &ClientConfig{
		Protocol: ProtocolREST,
		Address:  "localhost:8080",
	}

	client, err := newRESTClient(config)
	require.NoError(t, err)
	assert.NotNil(t, client)
	assert.Equal(t, "http://localhost:8080", client.baseURL)
}

func TestRESTClient_NewClient_WithTLS(t *testing.T) {
	config := &ClientConfig{
		Protocol:           ProtocolREST,
		Address:            "localhost:8443",
		UseTLS:             true,
		InsecureSkipVerify: true,
	}

	client, err := newRESTClient(config)
	require.NoError(t, err)
	assert.NotNil(t, client)
	assert.Equal(t, "https://localhost:8443", client.baseURL)
}

func TestRESTClient_NewClient_InvalidConfig(t *testing.T) {
	_, err := newRESTClient(nil)
	assert.Equal(t, ErrInvalidConfig, err)
}

func TestRESTClient_Close(t *testing.T) {
	config := &ClientConfig{
		Protocol: ProtocolREST,
		Address:  "localhost:8080",
	}

	client, err := newRESTClient(config)
	require.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)
}

func TestRESTClient_Interface(t *testing.T) {
	// Verify RESTClient implements Client interface at compile time
	var _ Client = (*RESTClient)(nil)
}
