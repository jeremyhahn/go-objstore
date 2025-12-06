require "spec_helper"

RSpec.describe ObjectStore::Client do
  describe "#initialize" do
    it "creates a REST client by default" do
      client = described_class.new

      expect(client.protocol).to eq(:rest)
      expect(client.client).to be_a(ObjectStore::Clients::RestClient)
    end

    it "creates a gRPC client when specified" do
      client = described_class.new(protocol: :grpc)

      expect(client.protocol).to eq(:grpc)
      expect(client.client).to be_a(ObjectStore::Clients::GrpcClient)
    end

    it "creates a QUIC client when specified" do
      client = described_class.new(protocol: :quic)

      expect(client.protocol).to eq(:quic)
      expect(client.client).to be_a(ObjectStore::Clients::QuicClient)
    end

    it "uses default port for REST" do
      client = described_class.new(protocol: :rest)

      expect(client.client.port).to eq(8080)
    end

    it "uses default port for gRPC" do
      client = described_class.new(protocol: :grpc)

      expect(client.client.port).to eq(50051)
    end

    it "uses default port for QUIC" do
      client = described_class.new(protocol: :quic)

      expect(client.client.port).to eq(4433)
    end

    it "accepts custom port" do
      client = described_class.new(protocol: :rest, port: 9000)

      expect(client.client.port).to eq(9000)
    end

    it "raises error for invalid protocol" do
      expect { described_class.new(protocol: :invalid) }.to raise_error(ArgumentError, /Invalid protocol/)
    end
  end

  describe "#switch_protocol" do
    it "switches from REST to gRPC" do
      client = described_class.new(protocol: :rest)

      expect(client.protocol).to eq(:rest)

      client.switch_protocol(:grpc)

      expect(client.protocol).to eq(:grpc)
      expect(client.client).to be_a(ObjectStore::Clients::GrpcClient)
      expect(client.client.port).to eq(50051)
    end

    it "switches from gRPC to QUIC" do
      client = described_class.new(protocol: :grpc)

      client.switch_protocol(:quic)

      expect(client.protocol).to eq(:quic)
      expect(client.client).to be_a(ObjectStore::Clients::QuicClient)
    end

    it "accepts custom port when switching" do
      client = described_class.new(protocol: :rest)

      client.switch_protocol(:grpc, port: 60000)

      expect(client.client.port).to eq(60000)
    end

    it "raises error for invalid protocol" do
      client = described_class.new(protocol: :rest)

      expect { client.switch_protocol(:invalid) }.to raise_error(ArgumentError)
    end
  end

  describe "delegated methods" do
    let(:mock_client) { instance_double(ObjectStore::Clients::RestClient) }
    let(:client) { described_class.new(protocol: :rest) }

    before do
      allow(ObjectStore::Clients::RestClient).to receive(:new).and_return(mock_client)
    end

    it "delegates put to underlying client" do
      expect(mock_client).to receive(:put).with("key", "data", nil)
      client.put("key", "data")
    end

    it "delegates get to underlying client" do
      expect(mock_client).to receive(:get).with("key")
      client.get("key")
    end

    it "delegates delete to underlying client" do
      expect(mock_client).to receive(:delete).with("key")
      client.delete("key")
    end

    it "delegates list to underlying client" do
      expect(mock_client).to receive(:list).with(
        prefix: "test/",
        delimiter: nil,
        max_results: 100,
        continue_from: nil
      )
      client.list(prefix: "test/")
    end

    it "delegates exists? to underlying client" do
      expect(mock_client).to receive(:exists?).with("key")
      client.exists?("key")
    end

    it "delegates get_metadata to underlying client" do
      expect(mock_client).to receive(:get_metadata).with("key")
      client.get_metadata("key")
    end

    it "delegates update_metadata to underlying client" do
      metadata = ObjectStore::Models::Metadata.new
      expect(mock_client).to receive(:update_metadata).with("key", metadata)
      client.update_metadata("key", metadata)
    end

    it "delegates health to underlying client" do
      expect(mock_client).to receive(:health).with(service: nil)
      client.health
    end

    it "delegates archive to underlying client" do
      expect(mock_client).to receive(:archive).with(
        "key",
        destination_type: "glacier",
        destination_settings: {}
      )
      client.archive("key", destination_type: "glacier")
    end

    it "delegates add_policy to underlying client" do
      policy = ObjectStore::Models::LifecyclePolicy.new(id: "p1")
      expect(mock_client).to receive(:add_policy).with(policy)
      client.add_policy(policy)
    end

    it "delegates remove_policy to underlying client" do
      expect(mock_client).to receive(:remove_policy).with("p1")
      client.remove_policy("p1")
    end

    it "delegates get_policies to underlying client" do
      expect(mock_client).to receive(:get_policies).with(prefix: nil)
      client.get_policies
    end

    it "delegates apply_policies to underlying client" do
      expect(mock_client).to receive(:apply_policies)
      client.apply_policies
    end

    it "delegates add_replication_policy to underlying client" do
      policy = ObjectStore::Models::ReplicationPolicy.new(id: "rep1")
      expect(mock_client).to receive(:add_replication_policy).with(policy)
      client.add_replication_policy(policy)
    end

    it "delegates remove_replication_policy to underlying client" do
      expect(mock_client).to receive(:remove_replication_policy).with("rep1")
      client.remove_replication_policy("rep1")
    end

    it "delegates get_replication_policies to underlying client" do
      expect(mock_client).to receive(:get_replication_policies)
      client.get_replication_policies
    end

    it "delegates get_replication_policy to underlying client" do
      expect(mock_client).to receive(:get_replication_policy).with("rep1")
      client.get_replication_policy("rep1")
    end

    it "delegates trigger_replication to underlying client" do
      expect(mock_client).to receive(:trigger_replication).with(
        policy_id: nil,
        parallel: false,
        worker_count: 4
      )
      client.trigger_replication
    end

    it "delegates get_replication_status to underlying client" do
      expect(mock_client).to receive(:get_replication_status).with("rep1")
      client.get_replication_status("rep1")
    end
  end
end
