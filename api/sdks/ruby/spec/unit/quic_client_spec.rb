# frozen_string_literal: true

require "spec_helper"

# Canonical SDK unit-test matrix for the QUIC protocol client.
#
# Ruby has no native HTTP/3, so the QUIC client uses Net::HTTP over TLS; the
# transport is mocked with WebMock. Each operation gets success + error; the 9
# not_found ops also get a not_found case. Plus metadata_round_trip and
# validation_empty_key.
RSpec.describe ObjectStore::Clients::QuicClient do
  subject(:client) { described_class.new(host: host, port: port) }

  let(:host) { "localhost" }
  let(:port) { 4433 }
  let(:base) { "https://#{host}:#{port}" }
  let(:json) { { "content-type" => "application/json" } }

  def stub_error(verb, path, status: 500)
    stub_request(verb, "#{base}#{path}")
      .to_return(status: status, body: { message: "boom" }.to_json, headers: json)
  end

  describe "put" do
    it "quic_put_success" do
      stub_request(:put, "#{base}/objects/k")
        .to_return(status: 200, body: { message: "stored" }.to_json,
                   headers: json.merge("etag" => "abc"))

      res = client.put("k", "data")
      expect(res).to be_a(ObjectStore::Models::PutResponse)
      expect(res.etag).to eq("abc")
    end

    it "quic_put_error" do
      stub_error(:put, "/objects/k")
      expect { client.put("k", "data") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get" do
    it "quic_get_success" do
      stub_request(:get, "#{base}/objects/k")
        .to_return(status: 200, body: "hello",
                   headers: { "content-type" => "text/plain", "etag" => "e1" })

      res = client.get("k")
      expect(res.data).to eq("hello")
      expect(res.metadata.etag).to eq("e1")
    end

    it "quic_get_reads_content_encoding_and_custom_metadata" do
      stub_request(:get, "#{base}/objects/k")
        .to_return(status: 200, body: "hello",
                   headers: { "content-type" => "text/plain",
                              "content-encoding" => "gzip",
                              "X-Meta-owner" => "alice",
                              "X-Meta-project" => "objstore" })

      res = client.get("k")
      expect(res.metadata.content_encoding).to eq("gzip")
      expect(res.metadata.custom).to eq("owner" => "alice", "project" => "objstore")
    end

    it "quic_get_not_found" do
      stub_request(:get, "#{base}/objects/k").to_return(status: 404)
      expect { client.get("k") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "quic_get_error" do
      stub_error(:get, "/objects/k")
      expect { client.get("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "delete" do
    it "quic_delete_success" do
      # The server returns 204 No Content with an empty body.
      stub_request(:delete, "#{base}/objects/k").to_return(status: 204)
      res = client.delete("k")
      expect(res.success).to be(true)
      expect(res.message).to eq("Object deleted successfully")
    end

    it "quic_delete_tolerates_legacy_200" do
      stub_request(:delete, "#{base}/objects/k")
        .to_return(status: 200, body: { message: "deleted" }.to_json, headers: json)
      expect(client.delete("k").success).to be(true)
    end

    it "quic_delete_not_found" do
      stub_request(:delete, "#{base}/objects/k").to_return(status: 404)
      expect { client.delete("k") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "quic_delete_error" do
      stub_error(:delete, "/objects/k")
      expect { client.delete("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "list" do
    it "quic_list_success" do
      body = { objects: [{ key: "a" }], truncated: false }.to_json
      stub_request(:get, "#{base}/objects?max=100").to_return(status: 200, body: body, headers: json)
      expect(client.list.objects.first.key).to eq("a")
    end

    it "quic_list_error" do
      stub_request(:get, "#{base}/objects?max=100")
        .to_return(status: 500, body: { message: "boom" }.to_json, headers: json)
      expect { client.list }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "exists?" do
    it "quic_exists_success" do
      stub_request(:head, "#{base}/objects/k").to_return(status: 200)
      expect(client.exists?("k")).to be(true)
    end

    it "quic_exists_not_found" do
      stub_request(:head, "#{base}/objects/k").to_return(status: 404)
      expect(client.exists?("k")).to be(false)
    end

    it "quic_exists_error" do
      stub_request(:head, "#{base}/objects/k").to_return(status: 500)
      expect { client.exists?("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_metadata" do
    it "quic_get_metadata_success" do
      stub_request(:head, "#{base}/objects/k")
        .to_return(status: 200,
                   headers: { "content-type" => "text/plain", "X-Meta-owner" => "alice" })

      res = client.get_metadata("k")
      expect(res.metadata.content_type).to eq("text/plain")
      expect(res.metadata.custom).to eq("owner" => "alice")
    end

    it "quic_get_metadata_not_found" do
      stub_request(:head, "#{base}/objects/k").to_return(status: 404)
      expect { client.get_metadata("k") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "quic_get_metadata_error" do
      stub_request(:head, "#{base}/objects/k").to_return(status: 500)
      expect { client.get_metadata("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "update_metadata" do
    it "quic_update_metadata_success" do
      stub_request(:patch, "#{base}/objects/k")
        .to_return(status: 200, body: { message: "ok" }.to_json, headers: json)
      expect(client.update_metadata("k", content_type: "text/plain").success).to be(true)
    end

    it "quic_update_metadata_not_found" do
      stub_request(:patch, "#{base}/objects/k").to_return(status: 404)
      expect { client.update_metadata("k", {}) }.to raise_error(ObjectStore::NotFoundError)
    end

    it "quic_update_metadata_error" do
      stub_error(:patch, "/objects/k")
      expect { client.update_metadata("k", {}) }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "health" do
    it "quic_health_success" do
      stub_request(:get, "#{base}/health")
        .to_return(status: 200, body: { status: "SERVING" }.to_json, headers: json)
      expect(client.health).to be_healthy
    end

    it "quic_health_error" do
      stub_error(:get, "/health")
      expect { client.health }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "archive" do
    it "quic_archive_success" do
      stub_request(:post, "#{base}/archive")
        .to_return(status: 200, body: { message: "archived" }.to_json, headers: json)
      expect(client.archive("k", destination_type: "glacier").success).to be(true)
    end

    it "quic_archive_error" do
      stub_error(:post, "/archive")
      expect { client.archive("k", destination_type: "glacier") }
        .to raise_error(ObjectStore::ServerError)
    end
  end

  describe "add_policy" do
    it "quic_add_policy_success" do
      stub_request(:post, "#{base}/policies")
        .to_return(status: 200, body: { message: "added" }.to_json, headers: json)
      expect(client.add_policy(id: "p1")[:success]).to be(true)
    end

    it "quic_add_policy_error" do
      stub_error(:post, "/policies")
      expect { client.add_policy(id: "p1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "remove_policy" do
    it "quic_remove_policy_success" do
      stub_request(:delete, "#{base}/policies/p1")
        .to_return(status: 200, body: { message: "removed" }.to_json, headers: json)
      expect(client.remove_policy("p1")[:success]).to be(true)
    end

    it "quic_remove_policy_not_found" do
      stub_request(:delete, "#{base}/policies/p1").to_return(status: 404)
      expect { client.remove_policy("p1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "quic_remove_policy_error" do
      stub_error(:delete, "/policies/p1")
      expect { client.remove_policy("p1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_policies" do
    it "quic_get_policies_success" do
      stub_request(:get, "#{base}/policies")
        .to_return(status: 200, body: { policies: [{ id: "p1" }] }.to_json, headers: json)
      expect(client.get_policies[:policies].first.id).to eq("p1")
    end

    it "quic_get_policies_error" do
      stub_error(:get, "/policies")
      expect { client.get_policies }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "apply_policies" do
    it "quic_apply_policies_success" do
      stub_request(:post, "#{base}/policies/apply")
        .to_return(status: 200, body: { objects_processed: 3 }.to_json, headers: json)
      expect(client.apply_policies[:objects_processed]).to eq(3)
    end

    it "quic_apply_policies_error" do
      stub_error(:post, "/policies/apply")
      expect { client.apply_policies }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "add_replication_policy" do
    it "quic_add_replication_policy_success" do
      stub_request(:post, "#{base}/replication/policies")
        .to_return(status: 200, body: { message: "added" }.to_json, headers: json)
      expect(client.add_replication_policy(id: "r1")[:success]).to be(true)
    end

    it "quic_add_replication_policy_error" do
      stub_error(:post, "/replication/policies")
      expect { client.add_replication_policy(id: "r1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "remove_replication_policy" do
    it "quic_remove_replication_policy_success" do
      stub_request(:delete, "#{base}/replication/policies/r1")
        .to_return(status: 200, body: { message: "removed" }.to_json, headers: json)
      expect(client.remove_replication_policy("r1")[:success]).to be(true)
    end

    it "quic_remove_replication_policy_not_found" do
      stub_request(:delete, "#{base}/replication/policies/r1").to_return(status: 404)
      expect { client.remove_replication_policy("r1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "quic_remove_replication_policy_error" do
      stub_error(:delete, "/replication/policies/r1")
      expect { client.remove_replication_policy("r1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_replication_policies" do
    it "quic_get_replication_policies_success" do
      stub_request(:get, "#{base}/replication/policies")
        .to_return(status: 200, body: { policies: [{ id: "r1" }] }.to_json, headers: json)
      expect(client.get_replication_policies[:policies].first.id).to eq("r1")
    end

    it "quic_get_replication_policies_error" do
      stub_error(:get, "/replication/policies")
      expect { client.get_replication_policies }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_replication_policy" do
    it "quic_get_replication_policy_success" do
      stub_request(:get, "#{base}/replication/policies/r1")
        .to_return(status: 200, body: { id: "r1" }.to_json, headers: json)
      expect(client.get_replication_policy("r1")[:policy].id).to eq("r1")
    end

    it "quic_get_replication_policy_not_found" do
      stub_request(:get, "#{base}/replication/policies/r1").to_return(status: 404)
      expect { client.get_replication_policy("r1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "quic_get_replication_policy_error" do
      stub_error(:get, "/replication/policies/r1")
      expect { client.get_replication_policy("r1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "trigger_replication" do
    it "quic_trigger_replication_success" do
      stub_request(:post, "#{base}/replication/trigger")
        .to_return(status: 200, body: { message: "triggered" }.to_json, headers: json)
      expect(client.trigger_replication[:success]).to be(true)
    end

    it "quic_trigger_replication_error" do
      stub_error(:post, "/replication/trigger")
      expect { client.trigger_replication }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_replication_status" do
    it "quic_get_replication_status_success" do
      stub_request(:get, "#{base}/replication/status/r1")
        .to_return(status: 200, body: { policy_id: "r1", total_objects_synced: 5 }.to_json,
                   headers: json)
      expect(client.get_replication_status("r1")[:status].total_objects_synced).to eq(5)
    end

    it "quic_get_replication_status_not_found" do
      stub_request(:get, "#{base}/replication/status/r1").to_return(status: 404)
      expect { client.get_replication_status("r1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "quic_get_replication_status_error" do
      stub_error(:get, "/replication/status/r1")
      expect { client.get_replication_status("r1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "HTTP status code mapping" do
    {
      400 => ObjectStore::ValidationError,
      401 => ObjectStore::AuthenticationError,
      403 => ObjectStore::AuthorizationError,
      404 => ObjectStore::NotFoundError,
      409 => ObjectStore::AlreadyExistsError,
      429 => ObjectStore::RateLimitError,
      500 => ObjectStore::ServerError,
      503 => ObjectStore::ServerError
    }.each do |status, error_class|
      it "quic_status_#{status}_raises_#{error_class.name.split('::').last}" do
        stub_error(:put, "/objects/k", status: status)
        expect { client.put("k", "data") }.to raise_error(error_class)
      end
    end
  end

  describe "cross-cutting" do
    it "quic_metadata_round_trip" do
      custom = { "owner" => "alice" }
      put_stub = stub_request(:put, "#{base}/objects/k")
                 .with(headers: { "Content-Type" => "text/plain",
                                  "Content-Encoding" => "gzip",
                                  "X-Meta-owner" => "alice" })
                 .to_return(status: 200, body: {}.to_json, headers: json.merge("etag" => "e"))

      meta = ObjectStore::Models::Metadata.new(
        content_type: "text/plain", content_encoding: "gzip", custom: custom
      )
      client.put("k", "body", meta)
      expect(put_stub).to have_been_requested

      stub_request(:get, "#{base}/objects/k")
        .to_return(status: 200, body: "body", headers: { "content-type" => "text/plain" })
      got = client.get("k")
      expect(got.metadata.content_type).to eq("text/plain")

      # Custom metadata is read via HEAD response headers (X-Meta-*).
      stub_request(:head, "#{base}/objects/k")
        .to_return(status: 200,
                   headers: { "content-type" => "text/plain",
                              "content-encoding" => "gzip",
                              "X-Meta-owner" => "alice" })
      md = client.get_metadata("k")
      expect(md.metadata.content_type).to eq("text/plain")
      expect(md.metadata.content_encoding).to eq("gzip")
      expect(md.metadata.custom).to eq(custom)
    end

    it "quic_validation_empty_key" do
      unified = ObjectStore::Client.new(protocol: :quic, host: host, port: port)
      expect { unified.get("") }.to raise_error(ArgumentError)
      expect(a_request(:get, %r{#{base}/objects})).not_to have_been_made
    end
  end

  # Language-specific extra: the QUIC client streams uploads with chunked
  # transfer encoding (body_stream) and streams downloads via Net::HTTP. Not
  # part of the canonical matrix but real code paths in this SDK.
  describe "streaming" do
    require "stringio"

    it "quic_put_stream_success" do
      stub_request(:put, "#{base}/objects/k")
        .with(headers: { "Content-Type" => "text/plain",
                         "Content-Encoding" => "gzip",
                         "Transfer-Encoding" => "chunked",
                         "X-Meta-owner" => "alice" })
        .to_return(status: 201, body: { message: "ok" }.to_json,
                   headers: json.merge("etag" => "e1"))

      meta = ObjectStore::Models::Metadata.new(
        content_type: "text/plain", content_encoding: "gzip", custom: { "owner" => "alice" }
      )
      res = client.put_stream("k", StringIO.new("streamed body"), metadata: meta)
      expect(res).to be_a(ObjectStore::Models::PutResponse)
      expect(res.etag).to eq("e1")
    end

    it "quic_get_stream_success" do
      stub_request(:get, "#{base}/objects/k")
        .to_return(status: 200, body: "streamed content",
                   headers: { "content-type" => "text/plain", "etag" => "e2" })

      chunks = []
      metadata = client.get_stream("k") { |c| chunks << c }
      expect(chunks.join).to eq("streamed content")
      expect(metadata).to be_a(ObjectStore::Models::Metadata)
      expect(metadata.content_type).to eq("text/plain")
    end

    it "quic_get_stream_not_found" do
      stub_request(:get, "#{base}/objects/k").to_return(status: 404)
      expect { client.get_stream("k") { |_c| } }.to raise_error(ObjectStore::NotFoundError)
    end
  end
end
