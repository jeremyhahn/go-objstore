require "spec_helper"
require "ostruct"
require "objstore/proto/objstore_services_pb"

RSpec.describe ObjectStore::Clients::GrpcClient do
  let(:client) { described_class.new(host: "localhost", port: 50051) }
  let(:mock_stub) { double("GrpcStub") }

  describe "#initialize" do
    it "uses default values" do
      client = described_class.new
      expect(client.host).to eq("localhost")
      expect(client.port).to eq(50051)
      expect(client.use_ssl).to be false
      expect(client.timeout).to eq(30)
    end

    it "accepts custom values" do
      client = described_class.new(host: "example.com", port: 9000, use_ssl: true, timeout: 60)
      expect(client.host).to eq("example.com")
      expect(client.port).to eq(9000)
      expect(client.use_ssl).to be true
      expect(client.timeout).to eq(60)
    end
  end

  describe "gRPC operations" do
    before do
      client.instance_variable_set(:@stub, mock_stub)
    end

    describe "#put" do
      let(:mock_response) { OpenStruct.new(success: true, message: "OK", etag: "abc123") }

      before do
        allow(mock_stub).to receive(:put).and_return(mock_response)
      end

      it "returns a PutResponse" do
        response = client.put("test.txt", "hello world")
        expect(response).to be_a(ObjectStore::Models::PutResponse)
        expect(response.success).to be true
        expect(response.etag).to eq("abc123")
      end

      it "accepts metadata" do
        metadata = ObjectStore::Models::Metadata.new(content_type: "text/plain")
        response = client.put("test.txt", "hello world", metadata)
        expect(response).to be_a(ObjectStore::Models::PutResponse)
      end

      it "accepts hash metadata" do
        response = client.put("test.txt", "hello world", { content_type: "text/plain" })
        expect(response).to be_a(ObjectStore::Models::PutResponse)
      end
    end

    describe "#put_stream" do
      let(:mock_response) { OpenStruct.new(success: true, message: "OK", etag: "abc123") }

      before do
        allow(mock_stub).to receive(:put).and_return(mock_response)
      end

      it "reads from IO and uploads" do
        io = StringIO.new("stream data")
        response = client.put_stream("stream.txt", io)
        expect(response).to be_a(ObjectStore::Models::PutResponse)
      end

      it "accepts metadata option" do
        io = StringIO.new("stream data")
        metadata = ObjectStore::Models::Metadata.new(content_type: "application/octet-stream")
        response = client.put_stream("stream.txt", io, metadata: metadata)
        expect(response).to be_a(ObjectStore::Models::PutResponse)
      end

      it "respects chunk_size parameter" do
        io = StringIO.new("x" * 100)
        response = client.put_stream("large.bin", io, chunk_size: 10)
        expect(response).to be_a(ObjectStore::Models::PutResponse)
      end
    end

    describe "#get" do
      let(:mock_metadata) { OpenStruct.new(content_type: "text/plain", size: 11) }
      let(:mock_responses) do
        [OpenStruct.new(data: "hello world", metadata: mock_metadata)]
      end

      before do
        allow(mock_stub).to receive(:get).and_return(mock_responses)
      end

      it "returns a GetResponse" do
        response = client.get("test.txt")
        expect(response).to be_a(ObjectStore::Models::GetResponse)
        expect(response.data).to eq("hello world")
      end
    end

    describe "#get_stream" do
      let(:mock_metadata) { OpenStruct.new(content_type: "text/plain", size: 11) }
      let(:mock_responses) do
        [OpenStruct.new(data: "hello ", metadata: mock_metadata),
         OpenStruct.new(data: "world", metadata: nil)]
      end

      before do
        allow(mock_stub).to receive(:get).and_return(mock_responses)
      end

      it "yields chunks to block" do
        chunks = []
        metadata = client.get_stream("test.txt") { |chunk| chunks << chunk }
        expect(metadata).to be_a(ObjectStore::Models::Metadata)
        expect(chunks).to eq(["hello ", "world"])
      end

      it "returns metadata without block" do
        metadata = client.get_stream("test.txt")
        expect(metadata).to be_a(ObjectStore::Models::Metadata)
      end
    end

    describe "#delete" do
      let(:mock_response) { OpenStruct.new(success: true, message: "Deleted") }

      before do
        allow(mock_stub).to receive(:delete).and_return(mock_response)
      end

      it "returns a DeleteResponse" do
        response = client.delete("test.txt")
        expect(response).to be_a(ObjectStore::Models::DeleteResponse)
        expect(response.success).to be true
      end
    end

    describe "#list" do
      let(:mock_response) do
        OpenStruct.new(
          objects: [OpenStruct.new(key: "file1.txt", metadata: nil)],
          common_prefixes: ["dir/"],
          next_token: nil,
          truncated: false
        )
      end

      before do
        allow(mock_stub).to receive(:list).and_return(mock_response)
      end

      it "returns a ListResponse" do
        response = client.list
        expect(response).to be_a(ObjectStore::Models::ListResponse)
        expect(response.objects.length).to eq(1)
      end

      it "accepts prefix parameter" do
        response = client.list(prefix: "test/")
        expect(response).to be_a(ObjectStore::Models::ListResponse)
      end

      it "accepts all parameters" do
        response = client.list(
          prefix: "test/",
          delimiter: "/",
          max_results: 50,
          continue_from: "token123"
        )
        expect(response).to be_a(ObjectStore::Models::ListResponse)
      end
    end

    describe "#exists?" do
      before do
        allow(mock_stub).to receive(:exists).and_return(OpenStruct.new(exists: true))
      end

      it "returns boolean" do
        result = client.exists?("test.txt")
        expect(result).to be true
      end
    end

    describe "#get_metadata" do
      let(:mock_metadata) { OpenStruct.new(content_type: "text/plain", size: 100) }
      let(:mock_response) { OpenStruct.new(metadata: mock_metadata, success: true, message: "OK") }

      before do
        allow(mock_stub).to receive(:get_metadata).and_return(mock_response)
      end

      it "returns a MetadataResponse" do
        response = client.get_metadata("test.txt")
        expect(response).to be_a(ObjectStore::Models::MetadataResponse)
        expect(response.success).to be true
      end
    end

    describe "#update_metadata" do
      let(:mock_response) { OpenStruct.new(success: true, message: "Updated") }

      before do
        allow(mock_stub).to receive(:update_metadata).and_return(mock_response)
      end

      it "returns an UpdateMetadataResponse" do
        metadata = ObjectStore::Models::Metadata.new(content_type: "application/json")
        response = client.update_metadata("test.txt", metadata)
        expect(response).to be_a(ObjectStore::Models::UpdateMetadataResponse)
      end

      it "accepts hash metadata" do
        response = client.update_metadata("test.txt", { content_type: "application/json" })
        expect(response).to be_a(ObjectStore::Models::UpdateMetadataResponse)
      end
    end

    describe "#health" do
      let(:mock_response) { OpenStruct.new(status: :SERVING, message: "OK") }

      before do
        allow(mock_stub).to receive(:health).and_return(mock_response)
      end

      it "returns a HealthResponse" do
        response = client.health
        expect(response).to be_a(ObjectStore::Models::HealthResponse)
        expect(response.healthy?).to be true
      end

      it "accepts service parameter" do
        response = client.health(service: "storage")
        expect(response).to be_a(ObjectStore::Models::HealthResponse)
      end
    end

    describe "#archive" do
      let(:mock_response) { OpenStruct.new(success: true, message: "Archived") }

      before do
        allow(mock_stub).to receive(:archive).and_return(mock_response)
      end

      it "returns an ArchiveResponse" do
        response = client.archive("test.txt", destination_type: "glacier")
        expect(response).to be_a(ObjectStore::Models::ArchiveResponse)
      end

      it "accepts destination_settings" do
        response = client.archive(
          "test.txt",
          destination_type: "s3",
          destination_settings: { bucket: "archive-bucket" }
        )
        expect(response).to be_a(ObjectStore::Models::ArchiveResponse)
      end
    end

    describe "#add_policy" do
      let(:mock_response) { OpenStruct.new(success: true, message: "Added") }

      before do
        allow(mock_stub).to receive(:add_policy).and_return(mock_response)
      end

      it "returns success hash" do
        policy = ObjectStore::Models::LifecyclePolicy.new(
          id: "p1",
          prefix: "logs/",
          retention_seconds: 86400,
          action: "delete"
        )
        response = client.add_policy(policy)
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true
      end

      it "accepts hash policy" do
        response = client.add_policy(
          id: "p1",
          prefix: "logs/",
          retention_seconds: 86400,
          action: "delete"
        )
        expect(response).to be_a(Hash)
      end
    end

    describe "#remove_policy" do
      let(:mock_response) { OpenStruct.new(success: true, message: "Removed") }

      before do
        allow(mock_stub).to receive(:remove_policy).and_return(mock_response)
      end

      it "returns success hash" do
        response = client.remove_policy("p1")
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true
      end
    end

    describe "#get_policies" do
      let(:mock_response) { OpenStruct.new(policies: [], success: true) }

      before do
        allow(mock_stub).to receive(:get_policies).and_return(mock_response)
      end

      it "returns policies hash" do
        response = client.get_policies
        expect(response).to be_a(Hash)
        expect(response).to have_key(:policies)
      end

      it "accepts prefix parameter" do
        response = client.get_policies(prefix: "logs/")
        expect(response).to be_a(Hash)
      end
    end

    describe "#apply_policies" do
      let(:mock_response) do
        OpenStruct.new(success: true, policies_count: 2, objects_processed: 10, message: "OK")
      end

      before do
        allow(mock_stub).to receive(:apply_policies).and_return(mock_response)
      end

      it "returns result hash" do
        response = client.apply_policies
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true
        expect(response[:policies_count]).to eq(2)
      end
    end

    describe "#add_replication_policy" do
      let(:mock_response) { OpenStruct.new(success: true, message: "Added") }

      before do
        allow(mock_stub).to receive(:add_replication_policy).and_return(mock_response)
      end

      it "returns success hash" do
        policy = ObjectStore::Models::ReplicationPolicy.new(
          id: "rep1",
          source_backend: "local",
          destination_backend: "s3"
        )
        response = client.add_replication_policy(policy)
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true
      end

      it "accepts hash policy" do
        response = client.add_replication_policy(
          id: "rep1",
          source_backend: "local",
          destination_backend: "s3"
        )
        expect(response).to be_a(Hash)
      end
    end

    describe "#remove_replication_policy" do
      let(:mock_response) { OpenStruct.new(success: true, message: "Removed") }

      before do
        allow(mock_stub).to receive(:remove_replication_policy).and_return(mock_response)
      end

      it "returns success hash" do
        response = client.remove_replication_policy("rep1")
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true
      end
    end

    describe "#get_replication_policies" do
      let(:mock_response) { OpenStruct.new(policies: []) }

      before do
        allow(mock_stub).to receive(:get_replication_policies).and_return(mock_response)
      end

      it "returns policies hash" do
        response = client.get_replication_policies
        expect(response).to be_a(Hash)
        expect(response).to have_key(:policies)
      end
    end

    describe "#get_replication_policy" do
      let(:mock_policy) do
        OpenStruct.new(
          id: "rep1",
          source_backend: "local",
          source_settings: {},
          source_prefix: "",
          destination_backend: "s3",
          destination_settings: {},
          check_interval_seconds: 3600,
          enabled: true,
          replication_mode: :TRANSPARENT
        )
      end
      let(:mock_response) { OpenStruct.new(policy: mock_policy) }

      before do
        allow(mock_stub).to receive(:get_replication_policy).and_return(mock_response)
      end

      it "returns policy hash" do
        response = client.get_replication_policy("rep1")
        expect(response).to be_a(Hash)
        expect(response).to have_key(:policy)
      end
    end

    describe "#trigger_replication" do
      let(:mock_result) do
        OpenStruct.new(
          policy_id: "rep1",
          synced: 10,
          deleted: 0,
          failed: 0,
          bytes_total: 1024,
          duration_ms: 100,
          errors: []
        )
      end
      let(:mock_response) { OpenStruct.new(success: true, result: mock_result, message: "OK") }

      before do
        allow(mock_stub).to receive(:trigger_replication).and_return(mock_response)
      end

      it "returns result hash" do
        response = client.trigger_replication
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true
      end

      it "accepts all parameters" do
        response = client.trigger_replication(
          policy_id: "rep1",
          parallel: true,
          worker_count: 8
        )
        expect(response).to be_a(Hash)
      end
    end

    describe "#get_replication_status" do
      let(:mock_status) do
        OpenStruct.new(
          policy_id: "rep1",
          source_backend: "local",
          destination_backend: "s3",
          enabled: true,
          total_objects_synced: 100,
          total_objects_deleted: 5,
          total_bytes_synced: 1024000,
          total_errors: 0,
          average_sync_duration_ms: 150,
          sync_count: 10
        )
      end
      let(:mock_response) { OpenStruct.new(success: true, status: mock_status, message: "OK") }

      before do
        allow(mock_stub).to receive(:get_replication_status).and_return(mock_response)
      end

      it "returns status hash" do
        response = client.get_replication_status("rep1")
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true
        expect(response).to have_key(:status)
      end
    end
  end

  describe "error handling" do
    before do
      client.instance_variable_set(:@stub, mock_stub)
    end

    it "handles NOT_FOUND error" do
      error = GRPC::NotFound.new("Object not found")
      allow(mock_stub).to receive(:get).and_raise(error)

      expect { client.get("missing.txt") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "handles INVALID_ARGUMENT error" do
      error = GRPC::InvalidArgument.new("Invalid key")
      allow(mock_stub).to receive(:put).and_raise(error)

      expect { client.put("", "data") }.to raise_error(ObjectStore::ValidationError)
    end

    it "handles DEADLINE_EXCEEDED error" do
      error = GRPC::DeadlineExceeded.new("Request timed out")
      allow(mock_stub).to receive(:get).and_raise(error)

      expect { client.get("slow.txt") }.to raise_error(ObjectStore::TimeoutError)
    end

    it "handles generic gRPC error" do
      error = GRPC::Internal.new("Internal error")
      allow(mock_stub).to receive(:delete).and_raise(error)

      expect { client.delete("test.txt") }.to raise_error(ObjectStore::Error)
    end
  end
end
