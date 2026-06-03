# frozen_string_literal: true

require "spec_helper"
require "objstore/proto/objstore_services_pb"

# Canonical SDK unit-test matrix for the gRPC protocol client.
#
# The transport is mocked by replacing the gRPC stub with a test double, so no
# live server is required. Each operation gets success + error; the 9 not_found
# ops also get a not_found case. Plus metadata_round_trip and validation_empty_key.
RSpec.describe ObjectStore::Clients::GrpcClient do
  subject(:client) { described_class.new(host: "localhost", port: 50_051) }

  let(:stub) { double("ObjectStore::Stub") }

  before do
    # Inject the test double in place of the real stub; bypass ensure_stub's
    # channel creation entirely.
    allow(client).to receive(:ensure_stub)
    client.instance_variable_set(:@stub, stub)
  end

  # Build a gRPC NOT_FOUND / generic error.
  def bad_status(code)
    GRPC::BadStatus.new(code, "boom")
  end

  def not_found_error
    bad_status(GRPC::Core::StatusCodes::NOT_FOUND)
  end

  def server_error
    bad_status(GRPC::Core::StatusCodes::UNIMPLEMENTED)
  end

  describe "put" do
    it "grpc_put_success" do
      resp = double(success: true, message: "ok", etag: "e1")
      allow(stub).to receive(:put).and_return(resp)

      res = client.put("k", "data")
      expect(res).to be_a(ObjectStore::Models::PutResponse)
      expect(res.etag).to eq("e1")
    end

    it "grpc_put_error" do
      allow(stub).to receive(:put).and_raise(server_error)
      expect { client.put("k", "data") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get" do
    it "grpc_get_success" do
      meta = double(content_type: "text/plain", content_encoding: nil, size: 4,
                    etag: "e", custom: {})
      chunk = double(data: "data", metadata: meta)
      allow(stub).to receive(:get).and_return([chunk])

      res = client.get("k")
      expect(res.data).to eq("data")
      expect(res.metadata.content_type).to eq("text/plain")
    end

    it "grpc_get_not_found" do
      allow(stub).to receive(:get).and_raise(not_found_error)
      expect { client.get("k") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "grpc_get_error" do
      allow(stub).to receive(:get).and_raise(server_error)
      expect { client.get("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "delete" do
    it "grpc_delete_success" do
      allow(stub).to receive(:delete).and_return(double(success: true, message: "ok"))
      expect(client.delete("k").success).to be(true)
    end

    it "grpc_delete_not_found" do
      allow(stub).to receive(:delete).and_raise(not_found_error)
      expect { client.delete("k") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "grpc_delete_error" do
      allow(stub).to receive(:delete).and_raise(server_error)
      expect { client.delete("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "list" do
    it "grpc_list_success" do
      obj = double(key: "a", metadata: nil)
      resp = double(objects: [obj], common_prefixes: [], next_token: "", truncated: false)
      allow(stub).to receive(:list).and_return(resp)

      res = client.list
      expect(res.objects.first.key).to eq("a")
    end

    it "grpc_list_error" do
      allow(stub).to receive(:list).and_raise(server_error)
      expect { client.list }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "exists?" do
    it "grpc_exists_success" do
      allow(stub).to receive(:exists).and_return(double(exists: true))
      expect(client.exists?("k")).to be(true)
    end

    it "grpc_exists_not_found" do
      # NOT_FOUND surfaces as the SDK's NotFoundError (exists has no false fallback).
      allow(stub).to receive(:exists).and_raise(not_found_error)
      expect { client.exists?("k") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "grpc_exists_error" do
      allow(stub).to receive(:exists).and_raise(server_error)
      expect { client.exists?("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_metadata" do
    it "grpc_get_metadata_success" do
      meta = double(content_type: "text/plain", content_encoding: nil, size: 1,
                    etag: "e", custom: {})
      allow(stub).to receive(:get_metadata)
        .and_return(double(metadata: meta, success: true, message: "ok"))

      res = client.get_metadata("k")
      expect(res.metadata.content_type).to eq("text/plain")
    end

    it "grpc_get_metadata_not_found" do
      allow(stub).to receive(:get_metadata).and_raise(not_found_error)
      expect { client.get_metadata("k") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "grpc_get_metadata_error" do
      allow(stub).to receive(:get_metadata).and_raise(server_error)
      expect { client.get_metadata("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "update_metadata" do
    it "grpc_update_metadata_success" do
      allow(stub).to receive(:update_metadata).and_return(double(success: true, message: "ok"))
      expect(client.update_metadata("k", {}).success).to be(true)
    end

    it "grpc_update_metadata_not_found" do
      allow(stub).to receive(:update_metadata).and_raise(not_found_error)
      expect { client.update_metadata("k", {}) }.to raise_error(ObjectStore::NotFoundError)
    end

    it "grpc_update_metadata_error" do
      allow(stub).to receive(:update_metadata).and_raise(server_error)
      expect { client.update_metadata("k", {}) }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "health" do
    it "grpc_health_success" do
      allow(stub).to receive(:health).and_return(double(status: :SERVING, message: "ok"))
      expect(client.health).to be_healthy
    end

    it "grpc_health_error" do
      allow(stub).to receive(:health).and_raise(server_error)
      expect { client.health }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "archive" do
    it "grpc_archive_success" do
      allow(stub).to receive(:archive).and_return(double(success: true, message: "ok"))
      expect(client.archive("k", destination_type: "glacier").success).to be(true)
    end

    it "grpc_archive_error" do
      allow(stub).to receive(:archive).and_raise(server_error)
      expect { client.archive("k", destination_type: "glacier") }
        .to raise_error(ObjectStore::ServerError)
    end
  end

  describe "add_policy" do
    it "grpc_add_policy_success" do
      allow(stub).to receive(:add_policy).and_return(double(success: true, message: "ok"))
      expect(client.add_policy(id: "p1")[:success]).to be(true)
    end

    it "grpc_add_policy_error" do
      allow(stub).to receive(:add_policy).and_raise(server_error)
      expect { client.add_policy(id: "p1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "remove_policy" do
    it "grpc_remove_policy_success" do
      allow(stub).to receive(:remove_policy).and_return(double(success: true, message: "ok"))
      expect(client.remove_policy("p1")[:success]).to be(true)
    end

    it "grpc_remove_policy_not_found" do
      allow(stub).to receive(:remove_policy).and_raise(not_found_error)
      expect { client.remove_policy("p1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "grpc_remove_policy_error" do
      allow(stub).to receive(:remove_policy).and_raise(server_error)
      expect { client.remove_policy("p1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_policies" do
    it "grpc_get_policies_success" do
      policy = double(id: "p1", prefix: "", retention_seconds: 0, action: "",
                      destination_type: "", destination_settings: nil)
      allow(stub).to receive(:get_policies).and_return(double(policies: [policy], success: true))
      expect(client.get_policies[:policies].first.id).to eq("p1")
    end

    it "grpc_get_policies_error" do
      allow(stub).to receive(:get_policies).and_raise(server_error)
      expect { client.get_policies }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "apply_policies" do
    it "grpc_apply_policies_success" do
      allow(stub).to receive(:apply_policies)
        .and_return(double(success: true, policies_count: 1, objects_processed: 2, message: "ok"))
      expect(client.apply_policies[:objects_processed]).to eq(2)
    end

    it "grpc_apply_policies_error" do
      allow(stub).to receive(:apply_policies).and_raise(server_error)
      expect { client.apply_policies }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "add_replication_policy" do
    it "grpc_add_replication_policy_success" do
      allow(stub).to receive(:add_replication_policy)
        .and_return(double(success: true, message: "ok"))
      expect(client.add_replication_policy(id: "r1")[:success]).to be(true)
    end

    it "grpc_add_replication_policy_error" do
      allow(stub).to receive(:add_replication_policy).and_raise(server_error)
      expect { client.add_replication_policy(id: "r1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "remove_replication_policy" do
    it "grpc_remove_replication_policy_success" do
      allow(stub).to receive(:remove_replication_policy)
        .and_return(double(success: true, message: "ok"))
      expect(client.remove_replication_policy("r1")[:success]).to be(true)
    end

    it "grpc_remove_replication_policy_not_found" do
      allow(stub).to receive(:remove_replication_policy).and_raise(not_found_error)
      expect { client.remove_replication_policy("r1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "grpc_remove_replication_policy_error" do
      allow(stub).to receive(:remove_replication_policy).and_raise(server_error)
      expect { client.remove_replication_policy("r1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_replication_policies" do
    let(:policy) do
      double(id: "r1", source_backend: "", source_settings: nil, source_prefix: "",
             destination_backend: "", destination_settings: nil, check_interval_seconds: 0,
             enabled: false, replication_mode: :TRANSPARENT)
    end

    it "grpc_get_replication_policies_success" do
      allow(stub).to receive(:get_replication_policies).and_return(double(policies: [policy]))
      expect(client.get_replication_policies[:policies].first.id).to eq("r1")
    end

    it "grpc_get_replication_policies_error" do
      allow(stub).to receive(:get_replication_policies).and_raise(server_error)
      expect { client.get_replication_policies }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_replication_policy" do
    let(:policy) do
      double(id: "r1", source_backend: "", source_settings: nil, source_prefix: "",
             destination_backend: "", destination_settings: nil, check_interval_seconds: 0,
             enabled: false, replication_mode: :TRANSPARENT)
    end

    it "grpc_get_replication_policy_success" do
      allow(stub).to receive(:get_replication_policy).and_return(double(policy: policy))
      expect(client.get_replication_policy("r1")[:policy].id).to eq("r1")
    end

    it "grpc_get_replication_policy_not_found" do
      allow(stub).to receive(:get_replication_policy).and_raise(not_found_error)
      expect { client.get_replication_policy("r1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "grpc_get_replication_policy_error" do
      allow(stub).to receive(:get_replication_policy).and_raise(server_error)
      expect { client.get_replication_policy("r1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "trigger_replication" do
    it "grpc_trigger_replication_success" do
      result = double(policy_id: "r1", synced: 1, deleted: 0, failed: 0,
                      bytes_total: 10, duration_ms: 5, errors: nil)
      allow(stub).to receive(:trigger_replication)
        .and_return(double(success: true, result: result, message: "ok"))
      expect(client.trigger_replication[:success]).to be(true)
    end

    it "grpc_trigger_replication_error" do
      allow(stub).to receive(:trigger_replication).and_raise(server_error)
      expect { client.trigger_replication }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_replication_status" do
    let(:status) do
      double(policy_id: "r1", source_backend: "", destination_backend: "", enabled: true,
             total_objects_synced: 5, total_objects_deleted: 0, total_bytes_synced: 0,
             total_errors: 0, average_sync_duration_ms: 0, sync_count: 1)
    end

    it "grpc_get_replication_status_success" do
      allow(stub).to receive(:get_replication_status)
        .and_return(double(success: true, status: status, message: "ok"))
      expect(client.get_replication_status("r1")[:status].total_objects_synced).to eq(5)
    end

    it "grpc_get_replication_status_not_found" do
      allow(stub).to receive(:get_replication_status).and_raise(not_found_error)
      expect { client.get_replication_status("r1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "grpc_get_replication_status_error" do
      allow(stub).to receive(:get_replication_status).and_raise(server_error)
      expect { client.get_replication_status("r1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "cross-cutting" do
    it "grpc_metadata_round_trip" do
      custom = { "owner" => "alice" }
      meta = ObjectStore::Models::Metadata.new(
        content_type: "text/plain", content_encoding: "gzip", custom: custom
      )

      # put: assert the proto message carries content_type/encoding/custom.
      captured = nil
      allow(stub).to receive(:put) do |req, **_|
        captured = req
        double(success: true, message: "ok", etag: "e")
      end
      client.put("k", "body", meta)
      expect(captured.metadata.content_type).to eq("text/plain")
      expect(captured.metadata.content_encoding).to eq("gzip")
      expect(captured.metadata.custom.to_h).to eq(custom)

      # get + get_metadata: metadata travels back in proto fields.
      proto_meta = double(content_type: "text/plain", content_encoding: "gzip",
                          size: 4, etag: "e", custom: custom)
      allow(stub).to receive(:get).and_return([double(data: "body", metadata: proto_meta)])
      got = client.get("k")
      expect(got.metadata.content_type).to eq("text/plain")
      expect(got.metadata.custom).to eq(custom)

      allow(stub).to receive(:get_metadata)
        .and_return(double(metadata: proto_meta, success: true, message: "ok"))
      md = client.get_metadata("k")
      expect(md.metadata.content_encoding).to eq("gzip")
      expect(md.metadata.custom).to eq(custom)
    end

    it "grpc_validation_empty_key" do
      unified = ObjectStore::Client.new(protocol: :grpc)
      expect { unified.get("") }.to raise_error(ArgumentError)
    end
  end

  # Language-specific extras: gRPC-native streaming and connection lifecycle,
  # plus the replication-mode proto mapping. Not part of the canonical matrix
  # but real code paths in this SDK.
  describe "streaming and lifecycle" do
    require "stringio"

    it "grpc_put_stream_success" do
      captured = nil
      allow(stub).to receive(:put) do |req, **_|
        captured = req
        double(success: true, message: "ok", etag: "e")
      end

      res = client.put_stream("k", StringIO.new("hello stream"), chunk_size: 4)
      expect(res).to be_a(ObjectStore::Models::PutResponse)
      expect(captured.data).to eq("hello stream")
    end

    it "grpc_get_stream_success" do
      meta = double(content_type: "text/plain", content_encoding: nil, size: 11,
                    etag: "e", custom: {})
      responses = [double(data: "hello ", metadata: meta),
                   double(data: "world", metadata: nil)]
      allow(stub).to receive(:get).and_return(responses)

      chunks = []
      metadata = client.get_stream("k") { |c| chunks << c }
      expect(chunks).to eq(["hello ", "world"])
      expect(metadata).to be_a(ObjectStore::Models::Metadata)
      expect(metadata.content_type).to eq("text/plain")
    end

    it "grpc_add_replication_policy_maps_opaque_mode" do
      captured = nil
      allow(stub).to receive(:add_replication_policy) do |req, **_|
        captured = req
        double(success: true, message: "ok")
      end

      client.add_replication_policy(
        id: "r1", source_backend: "local", destination_backend: "s3",
        source_settings: { "a" => "1" }, destination_settings: { "b" => "2" },
        check_interval_seconds: 60, enabled: true, replication_mode: "opaque"
      )
      expect(captured.policy.replication_mode).to eq(:OPAQUE)
      expect(captured.policy.source_settings.to_h).to eq("a" => "1")
    end

    it "grpc_close_releases_channel_and_is_idempotent" do
      channel = double("channel")
      allow(channel).to receive(:respond_to?).with(:close).and_return(true)
      allow(channel).to receive(:close)
      allow(stub).to receive(:respond_to?).with(:instance_variable_get).and_return(true)
      allow(stub).to receive(:instance_variable_get).with(:@ch).and_return(channel)

      client.close
      expect(channel).to have_received(:close)
      expect { client.close }.not_to raise_error
    end
  end
end
