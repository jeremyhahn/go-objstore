require "spec_helper"

RSpec.describe ObjectStore::Clients::QuicClient do
  let(:client) { described_class.new(host: "localhost", port: 4433, use_ssl: false) }
  let(:base_url) { "http://localhost:4433" }

  describe "#initialize" do
    it "uses default values" do
      client = described_class.new
      expect(client.host).to eq("localhost")
      expect(client.port).to eq(4433)
      expect(client.use_ssl).to be true
      expect(client.timeout).to eq(30)
    end

    it "accepts custom values" do
      client = described_class.new(host: "example.com", port: 9443, use_ssl: false, timeout: 60)
      expect(client.host).to eq("example.com")
      expect(client.port).to eq(9443)
      expect(client.use_ssl).to be false
      expect(client.timeout).to eq(60)
    end
  end

  describe "#put" do
    it "uploads object successfully" do
      stub_request(:put, "#{base_url}/objects/test.txt")
        .to_return(status: 201, body: { message: "success" }.to_json, headers: { "etag" => "abc123" })

      response = client.put("test.txt", "hello world")

      expect(response).to be_a(ObjectStore::Models::PutResponse)
      expect(response.success?).to be true
      expect(response.etag).to eq("abc123")
    end

    it "includes metadata in upload" do
      stub_request(:put, "#{base_url}/objects/doc.pdf")
        .to_return(status: 201, body: {}.to_json)

      metadata = ObjectStore::Models::Metadata.new(content_type: "application/pdf")
      client.put("doc.pdf", "pdf content", metadata)

      expect(WebMock).to have_requested(:put, "#{base_url}/objects/doc.pdf")
    end

    it "raises error on failure" do
      stub_request(:put, "#{base_url}/objects/fail.txt")
        .to_return(status: 500, body: { message: "error" }.to_json)

      expect { client.put("fail.txt", "data") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#put_stream" do
    it "uploads from IO object" do
      stub_request(:put, "#{base_url}/objects/stream.txt")
        .to_return(status: 201, body: { message: "success" }.to_json, headers: { "etag" => "xyz789" })

      io = StringIO.new("stream data")
      response = client.put_stream("stream.txt", io)

      expect(response).to be_a(ObjectStore::Models::PutResponse)
      expect(response.success?).to be true
    end

    it "includes metadata in streaming upload" do
      stub_request(:put, "#{base_url}/objects/doc.pdf")
        .to_return(status: 201, body: {}.to_json)

      io = StringIO.new("PDF content")
      metadata = ObjectStore::Models::Metadata.new(content_type: "application/pdf")
      client.put_stream("doc.pdf", io, metadata: metadata)

      expect(WebMock).to have_requested(:put, "#{base_url}/objects/doc.pdf")
    end
  end

  describe "#get" do
    it "retrieves object successfully" do
      stub_request(:get, "#{base_url}/objects/test.txt")
        .to_return(
          status: 200,
          body: "hello world",
          headers: {
            "content-type" => "text/plain",
            "content-length" => "11",
            "etag" => "xyz"
          }
        )

      response = client.get("test.txt")

      expect(response).to be_a(ObjectStore::Models::GetResponse)
      expect(response.data).to eq("hello world")
      expect(response.metadata.content_type).to eq("text/plain")
      expect(response.metadata.size).to eq(11)
    end

    it "raises NotFoundError for missing object" do
      stub_request(:get, "#{base_url}/objects/missing.txt")
        .to_return(status: 404)

      expect { client.get("missing.txt") }.to raise_error(ObjectStore::NotFoundError)
    end
  end

  describe "#get_stream" do
    it "yields chunks to block" do
      stub_request(:get, "#{base_url}/objects/stream.txt")
        .to_return(
          status: 200,
          body: "streamed content",
          headers: { "content-type" => "text/plain", "etag" => "abc" }
        )

      chunks = []
      metadata = client.get_stream("stream.txt") { |chunk| chunks << chunk }

      expect(chunks.join).to eq("streamed content")
      expect(metadata).to be_a(ObjectStore::Models::Metadata)
      expect(metadata.content_type).to eq("text/plain")
    end
  end

  describe "#delete" do
    it "deletes object successfully" do
      stub_request(:delete, "#{base_url}/objects/test.txt")
        .to_return(status: 200, body: { message: "deleted" }.to_json)

      response = client.delete("test.txt")

      expect(response).to be_a(ObjectStore::Models::DeleteResponse)
      expect(response.success?).to be true
    end

    it "raises error for non-existent object" do
      stub_request(:delete, "#{base_url}/objects/missing.txt")
        .to_return(status: 404)

      expect { client.delete("missing.txt") }.to raise_error(ObjectStore::NotFoundError)
    end
  end

  describe "#list" do
    it "lists objects with prefix" do
      stub_request(:get, /#{Regexp.escape(base_url)}\/objects/)
        .to_return(
          status: 200,
          body: {
            objects: [
              { key: "test/file1.txt", metadata: {} },
              { key: "test/file2.txt", metadata: {} }
            ],
            truncated: false
          }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      response = client.list(prefix: "test/")

      expect(response).to be_a(ObjectStore::Models::ListResponse)
      expect(response.objects.size).to eq(2)
      expect(response.truncated).to be false
    end

    it "handles pagination" do
      stub_request(:get, /#{Regexp.escape(base_url)}\/objects/)
        .to_return(
          status: 200,
          body: {
            objects: [],
            next_token: "def456",
            truncated: true
          }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      response = client.list(max_results: 10, continue_from: "abc123")

      expect(response.next_token).to eq("def456")
      expect(response.truncated).to be true
    end
  end

  describe "#exists?" do
    it "returns true when object exists" do
      stub_request(:head, "#{base_url}/objects/test.txt")
        .to_return(status: 200)

      expect(client.exists?("test.txt")).to be true
    end

    it "returns false when object does not exist" do
      stub_request(:head, "#{base_url}/objects/missing.txt")
        .to_return(status: 404)

      expect(client.exists?("missing.txt")).to be false
    end
  end

  describe "#get_metadata" do
    it "retrieves metadata successfully" do
      stub_request(:get, "#{base_url}/metadata/test.txt")
        .to_return(
          status: 200,
          body: {
            key: "test.txt",
            content_type: "text/plain",
            size: 1024,
            etag: "abc123",
            modified: "2025-11-25T12:00:00Z",
            metadata: { "author" => "test" }
          }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      response = client.get_metadata("test.txt")

      expect(response).to be_a(ObjectStore::Models::MetadataResponse)
      expect(response.success?).to be true
      expect(response.metadata.content_type).to eq("text/plain")
      expect(response.metadata.size).to eq(1024)
    end
  end

  describe "#update_metadata" do
    it "updates metadata successfully" do
      stub_request(:put, "#{base_url}/metadata/test.txt")
        .to_return(status: 200, body: { message: "updated" }.to_json)

      metadata = ObjectStore::Models::Metadata.new(content_type: "application/json")
      response = client.update_metadata("test.txt", metadata)

      expect(response).to be_a(ObjectStore::Models::UpdateMetadataResponse)
      expect(response.success?).to be true
    end
  end

  describe "#health" do
    it "checks health successfully" do
      stub_request(:get, /#{Regexp.escape(base_url)}\/health/)
        .to_return(status: 200, body: { status: "healthy" }.to_json, headers: { "Content-Type" => "application/json" })

      response = client.health

      expect(response).to be_a(ObjectStore::Models::HealthResponse)
      expect(response.healthy?).to be true
    end

    it "includes service parameter" do
      stub_request(:get, /#{Regexp.escape(base_url)}\/health/)
        .to_return(status: 200, body: { status: "healthy" }.to_json, headers: { "Content-Type" => "application/json" })

      response = client.health(service: "storage")

      expect(response).to be_a(ObjectStore::Models::HealthResponse)
    end
  end

  describe "#archive" do
    it "archives object successfully" do
      stub_request(:post, "#{base_url}/archive")
        .to_return(status: 200, body: { message: "archived" }.to_json)

      response = client.archive("test.txt", destination_type: "glacier")

      expect(response).to be_a(ObjectStore::Models::ArchiveResponse)
      expect(response.success?).to be true
    end

    it "includes destination settings" do
      stub_request(:post, "#{base_url}/archive")
        .to_return(status: 200, body: { message: "archived" }.to_json)

      response = client.archive("test.txt", destination_type: "s3", destination_settings: { bucket: "archive" })

      expect(response).to be_a(ObjectStore::Models::ArchiveResponse)
    end
  end

  describe "#add_policy" do
    it "adds lifecycle policy" do
      stub_request(:post, "#{base_url}/policies")
        .to_return(status: 201, body: { message: "created" }.to_json)

      policy = ObjectStore::Models::LifecyclePolicy.new(
        id: "p1",
        prefix: "archive/",
        retention_seconds: 86400,
        action: "delete"
      )

      response = client.add_policy(policy)

      expect(response[:success]).to be true
    end
  end

  describe "#remove_policy" do
    it "removes lifecycle policy" do
      stub_request(:delete, "#{base_url}/policies/p1")
        .to_return(status: 200, body: { message: "deleted" }.to_json)

      response = client.remove_policy("p1")

      expect(response[:success]).to be true
    end
  end

  describe "#get_policies" do
    it "retrieves all policies" do
      stub_request(:get, /#{Regexp.escape(base_url)}\/policies/)
        .to_return(
          status: 200,
          body: {
            policies: [
              { id: "p1", prefix: "test/", action: "delete" }
            ]
          }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      response = client.get_policies

      expect(response[:policies]).to be_an(Array)
      expect(response[:policies].first).to be_a(ObjectStore::Models::LifecyclePolicy)
    end
  end

  describe "#apply_policies" do
    it "applies all policies" do
      stub_request(:post, "#{base_url}/policies/apply")
        .to_return(
          status: 200,
          body: {
            policies_count: 5,
            objects_processed: 100
          }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      response = client.apply_policies

      expect(response[:success]).to be true
      expect(response[:policies_count]).to eq(5)
      expect(response[:objects_processed]).to eq(100)
    end
  end

  describe "#add_replication_policy" do
    it "adds replication policy" do
      stub_request(:post, "#{base_url}/replication/policies")
        .to_return(status: 201, body: { message: "created" }.to_json)

      policy = ObjectStore::Models::ReplicationPolicy.new(
        id: "rep1",
        source_backend: "local",
        destination_backend: "s3"
      )

      response = client.add_replication_policy(policy)

      expect(response[:success]).to be true
    end
  end

  describe "#remove_replication_policy" do
    it "removes replication policy" do
      stub_request(:delete, "#{base_url}/replication/policies/rep1")
        .to_return(status: 200, body: { message: "deleted" }.to_json)

      response = client.remove_replication_policy("rep1")

      expect(response[:success]).to be true
    end
  end

  describe "#get_replication_policies" do
    it "retrieves all replication policies" do
      stub_request(:get, "#{base_url}/replication/policies")
        .to_return(
          status: 200,
          body: {
            policies: [
              { id: "rep1", source_backend: "local", destination_backend: "s3" }
            ]
          }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      response = client.get_replication_policies

      expect(response[:policies]).to be_an(Array)
      expect(response[:policies].first).to be_a(ObjectStore::Models::ReplicationPolicy)
    end
  end

  describe "#get_replication_policy" do
    it "retrieves single replication policy" do
      stub_request(:get, "#{base_url}/replication/policies/rep1")
        .to_return(
          status: 200,
          body: { id: "rep1", source_backend: "local", destination_backend: "s3" }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      response = client.get_replication_policy("rep1")

      expect(response[:policy]).to be_a(ObjectStore::Models::ReplicationPolicy)
    end
  end

  describe "#trigger_replication" do
    it "triggers replication sync" do
      stub_request(:post, "#{base_url}/replication/trigger")
        .to_return(
          status: 200,
          body: {
            success: true,
            result: { synced: 10, deleted: 2 }
          }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      response = client.trigger_replication(parallel: true, worker_count: 8)

      expect(response[:success]).to be true
      expect(response[:result]).to be_a(Hash)
    end
  end

  describe "#get_replication_status" do
    it "retrieves replication status" do
      stub_request(:get, "#{base_url}/replication/policies/rep1/status")
        .to_return(
          status: 200,
          body: {
            success: true,
            status: {
              policy_id: "rep1",
              total_objects_synced: 1000
            }
          }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      response = client.get_replication_status("rep1")

      expect(response[:success]).to be true
      expect(response[:status]).to be_a(ObjectStore::Models::ReplicationStatus)
    end
  end

  describe "error handling" do
    it "handles 400 Bad Request" do
      stub_request(:put, "#{base_url}/objects/bad.txt")
        .to_return(status: 400, body: { message: "Invalid request" }.to_json)

      expect { client.put("bad.txt", "data") }.to raise_error(ObjectStore::ValidationError)
    end

    it "handles 413 Request Entity Too Large" do
      stub_request(:put, "#{base_url}/objects/large.bin")
        .to_return(status: 413, body: {}.to_json)

      expect { client.put("large.bin", "x" * 1000) }.to raise_error(ObjectStore::ValidationError)
    end

    it "handles 500 Server Error" do
      stub_request(:get, "#{base_url}/objects/error.txt")
        .to_return(status: 500, body: { message: "Internal error" }.to_json)

      expect { client.get("error.txt") }.to raise_error(ObjectStore::ServerError)
    end

    it "handles unexpected status codes" do
      stub_request(:get, "#{base_url}/objects/redirect.txt")
        .to_return(status: 302)

      expect { client.get("redirect.txt") }.to raise_error(ObjectStore::Error)
    end
  end
end
