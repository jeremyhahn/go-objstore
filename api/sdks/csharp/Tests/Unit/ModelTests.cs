using FluentAssertions;
using ObjStore.SDK.Models;
using Xunit;

namespace ObjStore.SDK.Tests.Unit;

public class ModelTests
{
    [Fact]
    public void ObjectMetadata_ShouldInitializeWithDefaults()
    {
        // Act
        var metadata = new ObjectMetadata();

        // Assert
        metadata.ContentType.Should().BeNull();
        metadata.Size.Should().Be(0);
        metadata.Custom.Should().BeNull();
    }

    [Fact]
    public void ObjectMetadata_ShouldSetProperties_Successfully()
    {
        // Arrange
        var now = DateTime.UtcNow;
        var custom = new Dictionary<string, string> { ["key"] = "value" };

        // Act
        var metadata = new ObjectMetadata
        {
            ContentType = "application/json",
            ContentEncoding = "gzip",
            Size = 1024,
            LastModified = now,
            ETag = "test-etag",
            Custom = custom
        };

        // Assert
        metadata.ContentType.Should().Be("application/json");
        metadata.ContentEncoding.Should().Be("gzip");
        metadata.Size.Should().Be(1024);
        metadata.LastModified.Should().Be(now);
        metadata.ETag.Should().Be("test-etag");
        metadata.Custom.Should().BeEquivalentTo(custom);
    }

    [Fact]
    public void ObjectInfo_ShouldSetProperties_Successfully()
    {
        // Arrange
        var metadata = new ObjectMetadata { Size = 100 };

        // Act
        var info = new ObjectInfo
        {
            Key = "test/file.txt",
            Metadata = metadata
        };

        // Assert
        info.Key.Should().Be("test/file.txt");
        info.Metadata.Should().Be(metadata);
    }

    [Fact]
    public void ListObjectsResponse_ShouldInitializeWithEmptyCollections()
    {
        // Act
        var response = new ListObjectsResponse();

        // Assert
        response.Objects.Should().NotBeNull().And.BeEmpty();
        response.CommonPrefixes.Should().NotBeNull().And.BeEmpty();
        response.NextToken.Should().BeNull();
        response.Truncated.Should().BeFalse();
    }

    [Fact]
    public void LifecyclePolicy_ShouldSetProperties_Successfully()
    {
        // Arrange & Act
        var policy = new LifecyclePolicy
        {
            Id = "policy-1",
            Prefix = "archive/",
            RetentionSeconds = 86400,
            Action = "delete",
            DestinationType = "glacier",
            DestinationSettings = new Dictionary<string, string> { ["bucket"] = "archive-bucket" }
        };

        // Assert
        policy.Id.Should().Be("policy-1");
        policy.Prefix.Should().Be("archive/");
        policy.RetentionSeconds.Should().Be(86400);
        policy.Action.Should().Be("delete");
        policy.DestinationType.Should().Be("glacier");
        policy.DestinationSettings.Should().ContainKey("bucket");
    }

    [Fact]
    public void ReplicationPolicy_ShouldSetProperties_Successfully()
    {
        // Arrange
        var encryption = new EncryptionPolicy
        {
            Backend = new EncryptionConfig
            {
                Enabled = true,
                Provider = "custom",
                DefaultKey = "key-1"
            }
        };

        // Act
        var policy = new ReplicationPolicy
        {
            Id = "repl-1",
            SourceBackend = "s3",
            DestinationBackend = "gcs",
            CheckIntervalSeconds = 300,
            Enabled = true,
            ReplicationMode = ReplicationMode.Transparent,
            Encryption = encryption
        };

        // Assert
        policy.Id.Should().Be("repl-1");
        policy.SourceBackend.Should().Be("s3");
        policy.DestinationBackend.Should().Be("gcs");
        policy.CheckIntervalSeconds.Should().Be(300);
        policy.Enabled.Should().BeTrue();
        policy.ReplicationMode.Should().Be(ReplicationMode.Transparent);
        policy.Encryption.Should().Be(encryption);
    }

    [Fact]
    public void ReplicationStatus_ShouldSetProperties_Successfully()
    {
        // Arrange
        var now = DateTime.UtcNow;

        // Act
        var status = new ReplicationStatus
        {
            PolicyId = "repl-1",
            SourceBackend = "s3",
            DestinationBackend = "gcs",
            Enabled = true,
            TotalObjectsSynced = 100,
            TotalObjectsDeleted = 10,
            TotalBytesSynced = 1048576,
            TotalErrors = 2,
            LastSyncTime = now,
            AverageSyncDurationMs = 5000,
            SyncCount = 50
        };

        // Assert
        status.PolicyId.Should().Be("repl-1");
        status.TotalObjectsSynced.Should().Be(100);
        status.TotalBytesSynced.Should().Be(1048576);
        status.LastSyncTime.Should().Be(now);
    }

    [Fact]
    public void HealthResponse_ShouldSetStatus_Successfully()
    {
        // Act
        var response = new HealthResponse
        {
            Status = HealthStatus.Serving,
            Message = "Healthy"
        };

        // Assert
        response.Status.Should().Be(HealthStatus.Serving);
        response.Message.Should().Be("Healthy");
    }

    [Fact]
    public void EncryptionConfig_ShouldSetProperties_Successfully()
    {
        // Act
        var config = new EncryptionConfig
        {
            Enabled = true,
            Provider = "aws-kms",
            DefaultKey = "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
        };

        // Assert
        config.Enabled.Should().BeTrue();
        config.Provider.Should().Be("aws-kms");
        config.DefaultKey.Should().NotBeEmpty();
    }

    [Fact]
    public void ReplicationMode_ShouldHaveCorrectValues()
    {
        // Assert
        ((int)ReplicationMode.Transparent).Should().Be(0);
        ((int)ReplicationMode.Opaque).Should().Be(1);
    }
}
