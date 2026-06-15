# frozen_string_literal: true

require "spec_helper"
require "stringio"

# Canonical SDK unit-test matrix for the unified client: delegation to each
# protocol client and resource cleanup via close. Also covers construction,
# validation, and protocol switching specific to the Ruby SDK.
RSpec.describe ObjectStore::Client do
  describe "construction" do
    it "defaults to the REST protocol client" do
      client = described_class.new
      expect(client.protocol).to eq(:rest)
      expect(client.client).to be_a(ObjectStore::Clients::RestClient)
    end

    it "rejects an invalid protocol" do
      expect { described_class.new(protocol: :ftp) }.to raise_error(ArgumentError, /Invalid protocol/)
    end

    it "selects the port default for each protocol" do
      expect(described_class.new(protocol: :rest).client.port).to eq(8080)
      expect(described_class.new(protocol: :grpc).client.port).to eq(50_051)
      expect(described_class.new(protocol: :quic).client.port).to eq(4433)
      expect(described_class.new(protocol: :mcp).client.port).to eq(8081)
    end

    it "unix protocol uses UnixClient (no port)" do
      client = described_class.new(protocol: :unix)
      expect(client.client).to be_a(ObjectStore::Clients::UnixClient)
    end

    it "accepts a custom port override" do
      expect(described_class.new(protocol: :rest, port: 9000).client.port).to eq(9000)
    end

    it "passes token to REST client" do
      client = described_class.new(protocol: :rest, token: "tok")
      expect(client.client.instance_variable_get(:@token)).to eq("tok")
    end

    it "passes tenant_id to REST client" do
      client = described_class.new(protocol: :rest, tenant_id: "acme")
      expect(client.client.instance_variable_get(:@tenant_id)).to eq("acme")
    end

    it "passes token to MCP client" do
      client = described_class.new(protocol: :mcp, token: "tok")
      expect(client.client.instance_variable_get(:@token)).to eq("tok")
    end
  end

  describe "delegation" do
    {
      rest: ObjectStore::Clients::RestClient,
      grpc: ObjectStore::Clients::GrpcClient,
      quic: ObjectStore::Clients::QuicClient,
      mcp: ObjectStore::Clients::McpClient,
      unix: ObjectStore::Clients::UnixClient
    }.each do |protocol, klass|
      it "unified_delegates_#{protocol}" do
        client = described_class.new(protocol: protocol)
        inner = client.client
        expect(inner).to be_a(klass)

        allow(inner).to receive(:health).and_return(
          ObjectStore::Models::HealthResponse.new(status: "SERVING")
        )
        allow(inner).to receive(:put).and_return(
          ObjectStore::Models::PutResponse.new(success: true, etag: "e")
        )

        expect(client.health).to be_healthy
        expect(client.put("k", "data").etag).to eq("e")
        expect(inner).to have_received(:health)
        expect(inner).to have_received(:put).with("k", "data", nil)
      end
    end
  end

  # Verifies the facade forwards every operation to the underlying protocol
  # client with the expected arguments. The inner client is a verifying double,
  # so signature drift is caught and the full delegation surface of client.rb is
  # exercised.
  describe "full delegation surface" do
    let(:inner) { instance_double(ObjectStore::Clients::RestClient) }
    let(:client) { described_class.new(protocol: :rest) }

    before do
      allow(ObjectStore::Clients::RestClient).to receive(:new).and_return(inner)
    end

    it "delegates object operations" do
      io = StringIO.new("data")
      allow(inner).to receive(:put_stream)
      allow(inner).to receive(:get_stream)
      allow(inner).to receive(:get)
      allow(inner).to receive(:delete)
      allow(inner).to receive(:list)
      allow(inner).to receive(:exists?)

      client.put_stream("k", io)
      client.get_stream("k") { |_c| }
      client.get("k")
      client.delete("k")
      client.list(prefix: "p/")
      client.exists?("k")

      expect(inner).to have_received(:put_stream).with("k", io, metadata: nil, chunk_size: 8192)
      expect(inner).to have_received(:get_stream).with("k")
      expect(inner).to have_received(:get).with("k")
      expect(inner).to have_received(:delete).with("k")
      expect(inner).to have_received(:list).with(prefix: "p/", delimiter: nil,
                                                 max_results: 100, continue_from: nil)
      expect(inner).to have_received(:exists?).with("k")
    end

    it "delegates metadata, health and archive operations" do
      meta = ObjectStore::Models::Metadata.new
      allow(inner).to receive(:get_metadata)
      allow(inner).to receive(:update_metadata)
      allow(inner).to receive(:health)
      allow(inner).to receive(:archive)

      client.get_metadata("k")
      client.update_metadata("k", meta)
      client.health(service: "s")
      client.archive("k", destination_type: "glacier")

      expect(inner).to have_received(:get_metadata).with("k")
      expect(inner).to have_received(:update_metadata).with("k", meta)
      expect(inner).to have_received(:health).with(service: "s")
      expect(inner).to have_received(:archive).with("k", destination_type: "glacier",
                                                    destination_settings: {})
    end

    it "delegates lifecycle-policy operations" do
      policy = ObjectStore::Models::LifecyclePolicy.new(id: "p1")
      allow(inner).to receive(:add_policy)
      allow(inner).to receive(:remove_policy)
      allow(inner).to receive(:get_policies)
      allow(inner).to receive(:apply_policies)

      client.add_policy(policy)
      client.remove_policy("p1")
      client.get_policies(prefix: "logs/")
      client.apply_policies

      expect(inner).to have_received(:add_policy).with(policy)
      expect(inner).to have_received(:remove_policy).with("p1")
      expect(inner).to have_received(:get_policies).with(prefix: "logs/")
      expect(inner).to have_received(:apply_policies)
    end

    it "delegates replication operations" do
      policy = ObjectStore::Models::ReplicationPolicy.new(id: "r1")
      allow(inner).to receive(:add_replication_policy)
      allow(inner).to receive(:remove_replication_policy)
      allow(inner).to receive(:get_replication_policies)
      allow(inner).to receive(:get_replication_policy)
      allow(inner).to receive(:trigger_replication)
      allow(inner).to receive(:get_replication_status)

      client.add_replication_policy(policy)
      client.remove_replication_policy("r1")
      client.get_replication_policies
      client.get_replication_policy("r1")
      client.trigger_replication(policy_id: "r1")
      client.get_replication_status("r1")

      expect(inner).to have_received(:add_replication_policy).with(policy)
      expect(inner).to have_received(:remove_replication_policy).with("r1")
      expect(inner).to have_received(:get_replication_policies)
      expect(inner).to have_received(:get_replication_policy).with("r1")
      expect(inner).to have_received(:trigger_replication).with(policy_id: "r1",
                                                               parallel: false, worker_count: 4)
      expect(inner).to have_received(:get_replication_status).with("r1")
    end
  end

  describe "close" do
    it "unified_close releases the underlying client resources" do
      client = described_class.new(protocol: :grpc)
      expect(client.client).to receive(:close)
      client.close
    end

    it "unified_close is safe to call repeatedly" do
      client = described_class.new(protocol: :rest)
      expect { client.close }.not_to raise_error
      expect { client.close }.not_to raise_error
    end
  end

  describe "client-side validation" do
    let(:client) { described_class.new }

    it "rejects an empty object key" do
      expect { client.get("") }.to raise_error(ArgumentError)
    end

    it "rejects nil put data" do
      expect { client.put("k", nil) }.to raise_error(ArgumentError)
    end

    it "rejects a non-IO stream argument" do
      expect { client.put_stream("k", "not-io") }.to raise_error(ArgumentError)
    end

    it "requires a block for get_stream" do
      expect { client.get_stream("k") }.to raise_error(ArgumentError)
    end

    it "rejects an empty policy id" do
      expect { client.remove_policy("") }.to raise_error(ArgumentError)
    end
  end

  describe "switch_protocol" do
    it "switches the active protocol client and port" do
      client = described_class.new(protocol: :rest)
      client.switch_protocol(:grpc)
      expect(client.protocol).to eq(:grpc)
      expect(client.client).to be_a(ObjectStore::Clients::GrpcClient)
      expect(client.client.port).to eq(50_051)
    end

    it "accepts a custom port when switching" do
      client = described_class.new(protocol: :rest)
      client.switch_protocol(:grpc, port: 60_000)
      expect(client.client.port).to eq(60_000)
    end

    it "switches to mcp with default port" do
      client = described_class.new(protocol: :rest)
      client.switch_protocol(:mcp)
      expect(client.protocol).to eq(:mcp)
      expect(client.client).to be_a(ObjectStore::Clients::McpClient)
      expect(client.client.port).to eq(8081)
    end

    it "switches to unix" do
      client = described_class.new(protocol: :rest)
      client.switch_protocol(:unix)
      expect(client.protocol).to eq(:unix)
      expect(client.client).to be_a(ObjectStore::Clients::UnixClient)
    end

    it "rejects switching to an invalid protocol" do
      client = described_class.new
      expect { client.switch_protocol(:ftp) }.to raise_error(ArgumentError)
    end
  end
end
