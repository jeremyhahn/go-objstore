// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"testing"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/stretchr/testify/assert"
)

func TestEncryptionConfigConversion_Nil(t *testing.T) {
	proto := encryptionConfigToProto(nil)
	assert.Nil(t, proto)

	converted := encryptionConfigFromProto(nil)
	assert.Nil(t, converted)
}

func TestEncryptionPolicyConversion_Nil(t *testing.T) {
	proto := encryptionPolicyToProto(nil)
	assert.Nil(t, proto)

	converted := encryptionPolicyFromProto(nil)
	assert.Nil(t, converted)
}

func TestLifecyclePolicyConversion_Nil(t *testing.T) {
	proto := lifecyclePolicyToProto(nil)
	assert.Nil(t, proto)

	converted := lifecyclePolicyFromProto(nil)
	assert.Nil(t, converted)
}

func TestReplicationPolicyConversion_Nil(t *testing.T) {
	proto := replicationPolicyToProto(nil)
	assert.Nil(t, proto)

	converted := replicationPolicyFromProto(nil)
	assert.Nil(t, converted)
}

func TestReplicationPolicyConversion_WithEncryption(t *testing.T) {
	policy := &ReplicationPolicy{
		ID: "test-policy",
		Encryption: &EncryptionPolicy{
			Backend: &EncryptionConfig{
				Enabled:    true,
				Provider:   "aes",
				DefaultKey: "key1",
			},
		},
		ReplicationMode: ReplicationModeOpaque,
	}

	proto := replicationPolicyToProto(policy)
	assert.NotNil(t, proto)
	assert.NotNil(t, proto.Encryption)
	assert.NotNil(t, proto.Encryption.Backend)
	assert.True(t, proto.Encryption.Backend.Enabled)
	assert.Equal(t, objstorepb.ReplicationMode_OPAQUE, proto.ReplicationMode)

	converted := replicationPolicyFromProto(proto)
	assert.NotNil(t, converted)
	assert.NotNil(t, converted.Encryption)
	assert.NotNil(t, converted.Encryption.Backend)
	assert.True(t, converted.Encryption.Backend.Enabled)
	assert.Equal(t, ReplicationModeOpaque, converted.ReplicationMode)
}

func TestProtoToMetadata(t *testing.T) {
	meta := &objstorepb.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		Size:            100,
		Etag:            "abc",
	}

	result := protoToMetadata(meta)
	assert.NotNil(t, result)
	assert.Equal(t, "text/plain", result.ContentType)
}
