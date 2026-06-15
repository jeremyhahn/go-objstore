# frozen_string_literal: true

require "spec_helper"

# Canonical SDK unit-test matrix for the REST protocol client.
#
# Covers, for each of the 19 operations: success + error. The 9 not_found ops
# additionally get a not_found case. Plus the two cross-cutting tests
# (metadata_round_trip, validation_empty_key).
#
# The transport (HTTP) is mocked with WebMock; no live server is required.
RSpec.describe ObjectStore::Clients::RestClient do
  subject(:client) { described_class.new(host: host, port: port) }

  let(:host) { "localhost" }
  let(:port) { 8080 }
  let(:base) { "http://#{host}:#{port}" }
  let(:json) { { "content-type" => "application/json" } }

  # Helper: stub a JSON 5xx error for any verb on a path.
  def stub_error(verb, path, status: 500)
    stub_request(verb, "#{base}#{path}")
      .to_return(status: status, body: { message: "boom" }.to_json, headers: json)
  end

  describe "put" do
    it "rest_put_success" do
      stub_request(:put, "#{base}/objects/k")
        .to_return(status: 200, body: { message: "stored" }.to_json,
                   headers: json.merge("etag" => "abc"))

      res = client.put("k", "data")
      expect(res).to be_a(ObjectStore::Models::PutResponse)
      expect(res.success).to be(true)
      expect(res.etag).to eq("abc")
    end

    it "rest_put_error" do
      stub_error(:put, "/objects/k")
      expect { client.put("k", "data") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get" do
    it "rest_get_success" do
      stub_request(:get, "#{base}/objects/k")
        .to_return(status: 200, body: "hello",
                   headers: { "content-type" => "text/plain", "etag" => "e1" })

      res = client.get("k")
      expect(res).to be_a(ObjectStore::Models::GetResponse)
      expect(res.data).to eq("hello")
      expect(res.metadata.etag).to eq("e1")
    end

    it "rest_get_not_found" do
      stub_request(:get, "#{base}/objects/k").to_return(status: 404)
      expect { client.get("k") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "rest_get_error" do
      stub_error(:get, "/objects/k")
      expect { client.get("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "delete" do
    it "rest_delete_success" do
      # The server returns 204 No Content with an empty body.
      stub_request(:delete, "#{base}/objects/k").to_return(status: 204)

      res = client.delete("k")
      expect(res).to be_a(ObjectStore::Models::DeleteResponse)
      expect(res.success).to be(true)
      expect(res.message).to eq("Object deleted successfully")
    end

    it "rest_delete_tolerates_legacy_200" do
      stub_request(:delete, "#{base}/objects/k")
        .to_return(status: 200, body: { message: "deleted" }.to_json, headers: json)

      res = client.delete("k")
      expect(res.success).to be(true)
      expect(res.message).to eq("deleted")
    end

    it "rest_delete_not_found" do
      stub_request(:delete, "#{base}/objects/k").to_return(status: 404)
      expect { client.delete("k") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "rest_delete_error" do
      stub_error(:delete, "/objects/k")
      expect { client.delete("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "list" do
    it "rest_list_success" do
      body = { objects: [{ key: "a" }, { key: "b" }], truncated: false }.to_json
      stub_request(:get, "#{base}/objects?limit=100")
        .to_return(status: 200, body: body, headers: json)

      res = client.list
      expect(res).to be_a(ObjectStore::Models::ListResponse)
      expect(res.objects.map(&:key)).to eq(%w[a b])
    end

    it "rest_list_error" do
      stub_request(:get, "#{base}/objects?limit=100")
        .to_return(status: 500, body: { message: "boom" }.to_json, headers: json)
      expect { client.list }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "exists?" do
    it "rest_exists_success" do
      stub_request(:head, "#{base}/objects/k").to_return(status: 200)
      expect(client.exists?("k")).to be(true)
    end

    it "rest_exists_not_found" do
      stub_request(:head, "#{base}/objects/k").to_return(status: 404)
      expect(client.exists?("k")).to be(false)
    end

    it "rest_exists_error" do
      # A 5xx on the HEAD must surface as a ServerError, not be swallowed into
      # "does not exist". Only a 404 maps to false; everything else non-2xx
      # routes through the shared error handler.
      stub_request(:head, "#{base}/objects/k").to_return(status: 500)
      expect { client.exists?("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_metadata" do
    it "rest_get_metadata_success" do
      body = { content_type: "text/plain", size: 5, metadata: { "x" => "1" } }.to_json
      stub_request(:get, "#{base}/metadata/k").to_return(status: 200, body: body, headers: json)

      res = client.get_metadata("k")
      expect(res).to be_a(ObjectStore::Models::MetadataResponse)
      expect(res.metadata.content_type).to eq("text/plain")
      expect(res.metadata.custom).to eq("x" => "1")
    end

    it "rest_get_metadata_not_found" do
      stub_request(:get, "#{base}/metadata/k").to_return(status: 404)
      expect { client.get_metadata("k") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "rest_get_metadata_error" do
      stub_error(:get, "/metadata/k")
      expect { client.get_metadata("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "update_metadata" do
    it "rest_update_metadata_success" do
      stub_request(:put, "#{base}/metadata/k")
        .to_return(status: 200, body: { message: "ok" }.to_json, headers: json)

      res = client.update_metadata("k", content_type: "text/plain")
      expect(res).to be_a(ObjectStore::Models::UpdateMetadataResponse)
      expect(res.success).to be(true)
    end

    it "rest_update_metadata_not_found" do
      stub_request(:put, "#{base}/metadata/k").to_return(status: 404)
      expect { client.update_metadata("k", {}) }.to raise_error(ObjectStore::NotFoundError)
    end

    it "rest_update_metadata_error" do
      stub_error(:put, "/metadata/k")
      expect { client.update_metadata("k", {}) }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "health" do
    it "rest_health_success" do
      stub_request(:get, "#{base}/health")
        .to_return(status: 200, body: { status: "SERVING" }.to_json, headers: json)

      res = client.health
      expect(res).to be_a(ObjectStore::Models::HealthResponse)
      expect(res).to be_healthy
    end

    it "rest_health_error" do
      stub_error(:get, "/health")
      expect { client.health }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "archive" do
    it "rest_archive_success" do
      stub_request(:post, "#{base}/archive")
        .to_return(status: 200, body: { message: "archived" }.to_json, headers: json)

      res = client.archive("k", destination_type: "glacier")
      expect(res).to be_a(ObjectStore::Models::ArchiveResponse)
      expect(res.success).to be(true)
    end

    it "rest_archive_error" do
      stub_error(:post, "/archive")
      expect { client.archive("k", destination_type: "glacier") }
        .to raise_error(ObjectStore::ServerError)
    end
  end

  describe "add_policy" do
    it "rest_add_policy_success" do
      stub_request(:post, "#{base}/policies")
        .to_return(status: 200, body: { message: "added" }.to_json, headers: json)

      res = client.add_policy(id: "p1")
      expect(res[:success]).to be(true)
    end

    it "rest_add_policy_error" do
      stub_error(:post, "/policies")
      expect { client.add_policy(id: "p1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "remove_policy" do
    it "rest_remove_policy_success" do
      stub_request(:delete, "#{base}/policies/p1")
        .to_return(status: 200, body: { message: "removed" }.to_json, headers: json)

      expect(client.remove_policy("p1")[:success]).to be(true)
    end

    it "rest_remove_policy_not_found" do
      stub_request(:delete, "#{base}/policies/p1").to_return(status: 404)
      expect { client.remove_policy("p1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "rest_remove_policy_error" do
      stub_error(:delete, "/policies/p1")
      expect { client.remove_policy("p1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_policies" do
    it "rest_get_policies_success" do
      stub_request(:get, "#{base}/policies")
        .to_return(status: 200, body: { policies: [{ id: "p1" }] }.to_json, headers: json)

      res = client.get_policies
      expect(res[:policies].first.id).to eq("p1")
    end

    it "rest_get_policies_error" do
      stub_error(:get, "/policies")
      expect { client.get_policies }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "apply_policies" do
    it "rest_apply_policies_success" do
      stub_request(:post, "#{base}/policies/apply")
        .to_return(status: 200, body: { objects_processed: 3 }.to_json, headers: json)

      res = client.apply_policies
      expect(res[:objects_processed]).to eq(3)
    end

    it "rest_apply_policies_error" do
      stub_error(:post, "/policies/apply")
      expect { client.apply_policies }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "add_replication_policy" do
    it "rest_add_replication_policy_success" do
      stub_request(:post, "#{base}/replication/policies")
        .to_return(status: 200, body: { message: "added" }.to_json, headers: json)

      expect(client.add_replication_policy(id: "r1")[:success]).to be(true)
    end

    it "rest_add_replication_policy_error" do
      stub_error(:post, "/replication/policies")
      expect { client.add_replication_policy(id: "r1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "remove_replication_policy" do
    it "rest_remove_replication_policy_success" do
      stub_request(:delete, "#{base}/replication/policies/r1")
        .to_return(status: 200, body: { message: "removed" }.to_json, headers: json)

      expect(client.remove_replication_policy("r1")[:success]).to be(true)
    end

    it "rest_remove_replication_policy_not_found" do
      stub_request(:delete, "#{base}/replication/policies/r1").to_return(status: 404)
      expect { client.remove_replication_policy("r1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "rest_remove_replication_policy_error" do
      stub_error(:delete, "/replication/policies/r1")
      expect { client.remove_replication_policy("r1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_replication_policies" do
    it "rest_get_replication_policies_success" do
      stub_request(:get, "#{base}/replication/policies")
        .to_return(status: 200, body: { policies: [{ id: "r1" }] }.to_json, headers: json)

      expect(client.get_replication_policies[:policies].first.id).to eq("r1")
    end

    it "rest_get_replication_policies_error" do
      stub_error(:get, "/replication/policies")
      expect { client.get_replication_policies }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_replication_policy" do
    it "rest_get_replication_policy_success" do
      stub_request(:get, "#{base}/replication/policies/r1")
        .to_return(status: 200, body: { id: "r1" }.to_json, headers: json)

      expect(client.get_replication_policy("r1")[:policy].id).to eq("r1")
    end

    it "rest_get_replication_policy_not_found" do
      stub_request(:get, "#{base}/replication/policies/r1").to_return(status: 404)
      expect { client.get_replication_policy("r1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "rest_get_replication_policy_error" do
      stub_error(:get, "/replication/policies/r1")
      expect { client.get_replication_policy("r1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "trigger_replication" do
    it "rest_trigger_replication_success" do
      stub_request(:post, "#{base}/replication/trigger")
        .to_return(status: 200, body: { message: "triggered" }.to_json, headers: json)

      expect(client.trigger_replication[:success]).to be(true)
    end

    it "rest_trigger_replication_error" do
      stub_error(:post, "/replication/trigger")
      expect { client.trigger_replication }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "get_replication_status" do
    it "rest_get_replication_status_success" do
      stub_request(:get, "#{base}/replication/status/r1")
        .to_return(status: 200, body: { policy_id: "r1", total_objects_synced: 5 }.to_json,
                   headers: json)

      res = client.get_replication_status("r1")
      expect(res[:status].total_objects_synced).to eq(5)
    end

    it "rest_get_replication_status_not_found" do
      stub_request(:get, "#{base}/replication/status/r1").to_return(status: 404)
      expect { client.get_replication_status("r1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "rest_get_replication_status_error" do
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
      it "rest_status_#{status}_raises_#{error_class.name.split('::').last}" do
        stub_error(:put, "/objects/k", status: status)
        expect { client.put("k", "data") }.to raise_error(error_class)
      end
    end
  end

  describe "cross-cutting" do
    it "rest_metadata_round_trip" do
      custom = { "owner" => "alice" }
      put_stub = stub_request(:put, "#{base}/objects/k")
                 .with(headers: { "Content-Type" => "text/plain",
                                  "Content-Encoding" => "gzip",
                                  "X-Object-Metadata" => custom.to_json })
                 .to_return(status: 200, body: {}.to_json, headers: json.merge("etag" => "e"))

      meta = ObjectStore::Models::Metadata.new(
        content_type: "text/plain", content_encoding: "gzip", custom: custom
      )
      client.put("k", "body", meta)
      expect(put_stub).to have_been_requested

      stub_request(:get, "#{base}/objects/k")
        .to_return(status: 200, body: "body",
                   headers: { "content-type" => "text/plain",
                              "content-encoding" => "gzip",
                              "x-object-metadata" => custom.to_json })
      got = client.get("k")
      expect(got.metadata.content_type).to eq("text/plain")
      expect(got.metadata.content_encoding).to eq("gzip")
      expect(got.metadata.custom).to eq(custom)

      mbody = { content_type: "text/plain", content_encoding: "gzip",
                metadata: custom }.to_json
      stub_request(:get, "#{base}/metadata/k").to_return(status: 200, body: mbody, headers: json)
      md = client.get_metadata("k")
      expect(md.metadata.content_type).to eq("text/plain")
      expect(md.metadata.content_encoding).to eq("gzip")
      expect(md.metadata.custom).to eq(custom)
    end

    it "rest_validation_empty_key" do
      # The unified client enforces client-side key validation before any
      # network call reaches the REST transport.
      unified = ObjectStore::Client.new(protocol: :rest, host: host, port: port)
      expect { unified.get("") }.to raise_error(ArgumentError)
      expect(a_request(:get, %r{#{base}/objects}))
        .not_to have_been_made
    end
  end

  # Language-specific extra: the REST client also exposes IO streaming upload
  # (reads the IO and PUTs it) and chunked streaming download, which are real
  # code paths in this SDK though not part of the canonical matrix.
  describe "streaming" do
    require "stringio"

    it "rest_put_stream_success" do
      stub_request(:put, "#{base}/objects/k")
        .to_return(status: 201, body: { message: "ok" }.to_json,
                   headers: json.merge("etag" => "e1"))

      res = client.put_stream("k", StringIO.new("streamed body"))
      expect(res).to be_a(ObjectStore::Models::PutResponse)
      expect(res.etag).to eq("e1")
    end

    it "rest_get_stream_success" do
      body = "Hello World from stream"
      stub_request(:get, "#{base}/objects/k")
        .to_return(status: 200, body: body,
                   headers: { "content-type" => "text/plain",
                              "content-length" => body.bytesize.to_s, "etag" => "e2" })

      chunks = []
      metadata = client.get_stream("k") { |c| chunks << c }
      expect(chunks.join).to eq(body)
      expect(metadata).to be_a(ObjectStore::Models::Metadata)
      expect(metadata.content_type).to eq("text/plain")
    end

    it "rest_get_stream_fallback_yields_in_chunks" do
      # When the streaming adapter raises, the client falls back to fetching the
      # whole body and slicing it into chunk_size pieces.
      connection = client.instance_variable_get(:@connection)
      body = "abcdefghij"
      fallback = instance_double(
        Faraday::Response,
        status: 200,
        body: body,
        headers: { "content-type" => "application/octet-stream",
                   "content-length" => body.bytesize.to_s }
      )

      calls = 0
      allow(connection).to receive(:get) do |*_args, &_blk|
        calls += 1
        raise StandardError, "streaming unsupported" if calls == 1

        fallback
      end

      chunks = []
      metadata = client.get_stream("k", chunk_size: 4) { |c| chunks << c }
      expect(chunks.join).to eq(body)
      expect(metadata.content_type).to eq("application/octet-stream")
    end
  end
end
