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

func TestClientConfig_Defaults(t *testing.T) {
	config := &ClientConfig{
		Protocol: ProtocolGRPC,
		Address:  "localhost:50051",
	}

	assert.Equal(t, ProtocolGRPC, config.Protocol)
	assert.Equal(t, "localhost:50051", config.Address)
	assert.False(t, config.UseTLS)
}

func TestClientConfig_WithTLS(t *testing.T) {
	config := &ClientConfig{
		Protocol:           ProtocolGRPC,
		Address:            "localhost:50051",
		UseTLS:             true,
		InsecureSkipVerify: true,
	}

	assert.True(t, config.UseTLS)
	assert.True(t, config.InsecureSkipVerify)
}

func TestNewClient_InvalidProtocol(t *testing.T) {
	config := &ClientConfig{
		Protocol: "invalid",
		Address:  "localhost:50051",
	}

	client, err := NewClient(config)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Equal(t, ErrInvalidProtocol, err)
}

func TestMetadataConversion(t *testing.T) {
	now := time.Now()
	metadata := &Metadata{
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		Size:            1024,
		LastModified:    now,
		ETag:            "abc123",
		Custom: map[string]string{
			"author": "test",
		},
	}

	proto := metadataToProto(metadata)
	assert.NotNil(t, proto)
	assert.Equal(t, "application/json", proto.ContentType)
	assert.Equal(t, "gzip", proto.ContentEncoding)
	assert.Equal(t, int64(1024), proto.Size)
	assert.Equal(t, "abc123", proto.Etag)
	assert.Equal(t, "test", proto.Custom["author"])

	converted := metadataFromProto(proto)
	assert.NotNil(t, converted)
	assert.Equal(t, metadata.ContentType, converted.ContentType)
	assert.Equal(t, metadata.ContentEncoding, converted.ContentEncoding)
	assert.Equal(t, metadata.Size, converted.Size)
	assert.Equal(t, metadata.ETag, converted.ETag)
	assert.Equal(t, metadata.Custom["author"], converted.Custom["author"])
}

func TestMetadataConversion_Nil(t *testing.T) {
	proto := metadataToProto(nil)
	assert.Nil(t, proto)

	converted := metadataFromProto(nil)
	assert.Nil(t, converted)
}

func TestLifecyclePolicyConversion(t *testing.T) {
	policy := &LifecyclePolicy{
		ID:               "policy1",
		Prefix:           "archive/",
		RetentionSeconds: 86400,
		Action:           "delete",
		DestinationType:  "glacier",
		DestinationSettings: map[string]string{
			"vault": "my-vault",
		},
	}

	proto := lifecyclePolicyToProto(policy)
	require.NotNil(t, proto)
	assert.Equal(t, "policy1", proto.Id)
	assert.Equal(t, "archive/", proto.Prefix)
	assert.Equal(t, int64(86400), proto.RetentionSeconds)
	assert.Equal(t, "delete", proto.Action)
	assert.Equal(t, "glacier", proto.DestinationType)
	assert.Equal(t, "my-vault", proto.DestinationSettings["vault"])

	converted := lifecyclePolicyFromProto(proto)
	require.NotNil(t, converted)
	assert.Equal(t, policy.ID, converted.ID)
	assert.Equal(t, policy.Prefix, converted.Prefix)
	assert.Equal(t, policy.RetentionSeconds, converted.RetentionSeconds)
	assert.Equal(t, policy.Action, converted.Action)
	assert.Equal(t, policy.DestinationType, converted.DestinationType)
	assert.Equal(t, policy.DestinationSettings["vault"], converted.DestinationSettings["vault"])
}

func TestEncryptionPolicyConversion(t *testing.T) {
	policy := &EncryptionPolicy{
		Backend: &EncryptionConfig{
			Enabled:    true,
			Provider:   "custom",
			DefaultKey: "backend-key",
		},
		Source: &EncryptionConfig{
			Enabled:    true,
			Provider:   "custom",
			DefaultKey: "source-key",
		},
		Destination: &EncryptionConfig{
			Enabled:    false,
			Provider:   "noop",
			DefaultKey: "",
		},
	}

	proto := encryptionPolicyToProto(policy)
	require.NotNil(t, proto)
	require.NotNil(t, proto.Backend)
	assert.True(t, proto.Backend.Enabled)
	assert.Equal(t, "custom", proto.Backend.Provider)
	assert.Equal(t, "backend-key", proto.Backend.DefaultKey)

	require.NotNil(t, proto.Source)
	assert.True(t, proto.Source.Enabled)
	assert.Equal(t, "custom", proto.Source.Provider)
	assert.Equal(t, "source-key", proto.Source.DefaultKey)

	require.NotNil(t, proto.Destination)
	assert.False(t, proto.Destination.Enabled)
	assert.Equal(t, "noop", proto.Destination.Provider)

	converted := encryptionPolicyFromProto(proto)
	require.NotNil(t, converted)
	require.NotNil(t, converted.Backend)
	assert.True(t, converted.Backend.Enabled)
	assert.Equal(t, "custom", converted.Backend.Provider)
	assert.Equal(t, "backend-key", converted.Backend.DefaultKey)
}

func TestReplicationPolicyConversion(t *testing.T) {
	policy := &ReplicationPolicy{
		ID:            "repl1",
		SourceBackend: "local",
		SourceSettings: map[string]string{
			"path": "/tmp/source",
		},
		SourcePrefix:       "data/",
		DestinationBackend: "s3",
		DestinationSettings: map[string]string{
			"bucket": "my-bucket",
		},
		CheckIntervalSeconds: 3600,
		Enabled:              true,
		ReplicationMode:      ReplicationModeTransparent,
	}

	proto := replicationPolicyToProto(policy)
	require.NotNil(t, proto)
	assert.Equal(t, "repl1", proto.Id)
	assert.Equal(t, "local", proto.SourceBackend)
	assert.Equal(t, "/tmp/source", proto.SourceSettings["path"])
	assert.Equal(t, "data/", proto.SourcePrefix)
	assert.Equal(t, "s3", proto.DestinationBackend)
	assert.Equal(t, "my-bucket", proto.DestinationSettings["bucket"])
	assert.Equal(t, int64(3600), proto.CheckIntervalSeconds)
	assert.True(t, proto.Enabled)

	converted := replicationPolicyFromProto(proto)
	require.NotNil(t, converted)
	assert.Equal(t, policy.ID, converted.ID)
	assert.Equal(t, policy.SourceBackend, converted.SourceBackend)
	assert.Equal(t, policy.SourceSettings["path"], converted.SourceSettings["path"])
	assert.Equal(t, policy.DestinationBackend, converted.DestinationBackend)
	assert.Equal(t, policy.CheckIntervalSeconds, converted.CheckIntervalSeconds)
	assert.True(t, converted.Enabled)
}

func TestReplicationMode(t *testing.T) {
	assert.Equal(t, ReplicationMode(0), ReplicationModeTransparent)
	assert.Equal(t, ReplicationMode(1), ReplicationModeOpaque)
}

func TestProtocol(t *testing.T) {
	assert.Equal(t, Protocol("rest"), ProtocolREST)
	assert.Equal(t, Protocol("grpc"), ProtocolGRPC)
	assert.Equal(t, Protocol("quic"), ProtocolQUIC)
}
