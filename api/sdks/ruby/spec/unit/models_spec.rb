require "spec_helper"

RSpec.describe ObjectStore::Models do
  describe ObjectStore::Models::Metadata do
    it "initializes with hash attributes" do
      metadata = described_class.new(
        content_type: "application/json",
        size: 1024,
        etag: "abc123"
      )

      expect(metadata.content_type).to eq("application/json")
      expect(metadata.size).to eq(1024)
      expect(metadata.etag).to eq("abc123")
    end

    it "handles string keys" do
      metadata = described_class.new(
        "content_type" => "text/plain",
        "size" => 512
      )

      expect(metadata.content_type).to eq("text/plain")
      expect(metadata.size).to eq(512)
    end

    it "parses time strings" do
      time_str = "2025-11-23T10:00:00Z"
      metadata = described_class.new(last_modified: time_str)

      expect(metadata.last_modified).to be_a(Time)
    end

    it "converts to hash" do
      metadata = described_class.new(content_type: "image/png", size: 2048)
      hash = metadata.to_h

      expect(hash[:content_type]).to eq("image/png")
      expect(hash[:size]).to eq(2048)
    end

    it "converts to JSON" do
      metadata = described_class.new(content_type: "video/mp4")
      json = metadata.to_json

      expect(json).to include("content_type")
      expect(json).to include("video/mp4")
    end
  end

  describe ObjectStore::Models::ObjectInfo do
    it "initializes with key and metadata" do
      obj = described_class.new(
        key: "test/file.txt",
        metadata: { content_type: "text/plain" }
      )

      expect(obj.key).to eq("test/file.txt")
      expect(obj.metadata).to be_a(ObjectStore::Models::Metadata)
      expect(obj.metadata.content_type).to eq("text/plain")
    end

    it "converts to hash" do
      obj = described_class.new(key: "data.json")
      hash = obj.to_h

      expect(hash[:key]).to eq("data.json")
      expect(hash[:metadata]).to be_a(Hash)
    end
  end

  describe ObjectStore::Models::PutResponse do
    it "has success predicate" do
      response = described_class.new(success: true, etag: "xyz789")

      expect(response.success?).to be true
      expect(response.etag).to eq("xyz789")
    end

    it "handles failure" do
      response = described_class.new(success: false, message: "Error occurred")

      expect(response.success?).to be false
      expect(response.message).to eq("Error occurred")
    end
  end

  describe ObjectStore::Models::ListResponse do
    it "initializes with objects array" do
      list = described_class.new(
        objects: [
          { key: "file1.txt", metadata: {} },
          { key: "file2.txt", metadata: {} }
        ],
        truncated: false
      )

      expect(list.objects.size).to eq(2)
      expect(list.objects.first).to be_a(ObjectStore::Models::ObjectInfo)
      expect(list.truncated).to be false
    end

    it "handles common prefixes" do
      list = described_class.new(
        objects: [],
        common_prefixes: ["dir1/", "dir2/"]
      )

      expect(list.common_prefixes).to eq(["dir1/", "dir2/"])
    end
  end

  describe ObjectStore::Models::ExistsResponse do
    it "has exists predicate" do
      response = described_class.new(exists: true)

      expect(response.exists?).to be true
    end
  end

  describe ObjectStore::Models::HealthResponse do
    it "has healthy predicate for SERVING status" do
      response = described_class.new(status: "SERVING")

      expect(response.healthy?).to be true
    end

    it "has healthy predicate for healthy status" do
      response = described_class.new(status: "healthy")

      expect(response.healthy?).to be true
    end

    it "returns false for other statuses" do
      response = described_class.new(status: "NOT_SERVING")

      expect(response.healthy?).to be false
    end
  end

  describe ObjectStore::Models::LifecyclePolicy do
    it "initializes with all attributes" do
      policy = described_class.new(
        id: "policy1",
        prefix: "archive/",
        retention_seconds: 86400,
        action: "delete"
      )

      expect(policy.id).to eq("policy1")
      expect(policy.prefix).to eq("archive/")
      expect(policy.retention_seconds).to eq(86400)
      expect(policy.action).to eq("delete")
    end

    it "converts to hash" do
      policy = described_class.new(id: "p1", prefix: "test/")
      hash = policy.to_h

      expect(hash[:id]).to eq("p1")
      expect(hash[:prefix]).to eq("test/")
    end
  end

  describe ObjectStore::Models::ReplicationPolicy do
    it "initializes with all attributes" do
      policy = described_class.new(
        id: "rep1",
        source_backend: "local",
        destination_backend: "s3",
        enabled: true
      )

      expect(policy.id).to eq("rep1")
      expect(policy.source_backend).to eq("local")
      expect(policy.destination_backend).to eq("s3")
      expect(policy.enabled).to be true
    end

    it "defaults replication_mode to TRANSPARENT" do
      policy = described_class.new(id: "rep2")

      expect(policy.replication_mode).to eq("TRANSPARENT")
    end
  end

  describe ObjectStore::Models::ReplicationStatus do
    it "initializes with metrics" do
      status = described_class.new(
        policy_id: "rep1",
        total_objects_synced: 100,
        total_bytes_synced: 1024000,
        enabled: true
      )

      expect(status.policy_id).to eq("rep1")
      expect(status.total_objects_synced).to eq(100)
      expect(status.total_bytes_synced).to eq(1024000)
      expect(status.enabled).to be true
    end
  end
end
