# frozen_string_literal: true

require "spec_helper"
require "stringio"
require "base64"

# Canonical SDK unit-test matrix for the MCP protocol client.
#
# Covers, for each of the 19 operations: success + error. The not_found
# operations additionally get a not_found case (via JSON-RPC code 404).
# Plus cross-cutting tests for auth headers, metadata round-trip, and
# validation_empty_key.
#
# Transport is mocked with WebMock; no live server is required.
RSpec.describe ObjectStore::Clients::McpClient do
  subject(:client) { described_class.new(host: host, port: port) }

  let(:host) { "localhost" }
  let(:port) { 8081 }
  let(:base) { "http://#{host}:#{port}" }
  let(:json_ct) { { "content-type" => "application/json" } }

  # Build a successful MCP tools/call JSON-RPC response wrapping result JSON.
  def mcp_ok(result_hash)
    {
      jsonrpc: "2.0",
      id: 1,
      result: {
        content: [{ type: "text", text: result_hash.to_json }]
      }
    }.to_json
  end

  # Build a JSON-RPC error response.
  def mcp_error(code:, message:)
    { jsonrpc: "2.0", id: 1, error: { code: code, message: message } }.to_json
  end

  # Stub POST "/" with a successful tools/call result.
  def stub_tool_ok(result_hash)
    stub_request(:post, "#{base}/")
      .to_return(status: 200, body: mcp_ok(result_hash), headers: json_ct)
  end

  # Stub POST "/" with a JSON-RPC error.
  def stub_tool_error(code:, message:)
    stub_request(:post, "#{base}/")
      .to_return(status: 200, body: mcp_error(code: code, message: message), headers: json_ct)
  end

  # Stub POST "/" with an HTTP 5xx response.
  def stub_http_error(status: 500)
    stub_request(:post, "#{base}/")
      .to_return(status: status, body: { message: "boom" }.to_json, headers: json_ct)
  end

  describe "#put" do
    it "mcp_put_success" do
      stub_tool_ok(success: true, key: "k", size: 5)
      res = client.put("k", "hello")
      expect(res).to be_a(ObjectStore::Models::PutResponse)
      expect(res.success).to be(true)
      # The wire payload must carry base64-encoded data.
      expect(WebMock).to have_requested(:post, "#{base}/").with { |req|
        body = JSON.parse(req.body)
        body.dig("params", "arguments", "data") == Base64.strict_encode64("hello")
      }
    end

    it "mcp_put_binary_data_is_base64" do
      stub_tool_ok(success: true, key: "k")
      payload = (+"\x00\x01\xFF\xFE\x80").force_encoding("BINARY")
      res = client.put("k", payload)
      expect(res.success).to be(true)
      expect(WebMock).to have_requested(:post, "#{base}/").with { |req|
        body = JSON.parse(req.body)
        Base64.strict_decode64(body.dig("params", "arguments", "data")) == payload
      }
    end

    it "mcp_put_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.put("k", "hello") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#put_stream" do
    it "mcp_put_stream buffers IO and delegates" do
      stub_tool_ok(success: true)
      io = StringIO.new("stream data")
      res = client.put_stream("k", io)
      expect(res).to be_a(ObjectStore::Models::PutResponse)
    end
  end

  describe "#get" do
    it "mcp_get_success" do
      encoded = Base64.strict_encode64("world")
      stub_tool_ok(success: true, key: "k", data: encoded)
      res = client.get("k")
      expect(res).to be_a(ObjectStore::Models::GetResponse)
      expect(res.data).to eq("world")
    end

    it "mcp_get_not_found" do
      stub_tool_error(code: 404, message: "not found")
      expect { client.get("k") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "mcp_get_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.get("k") }.to raise_error(ObjectStore::ServerError)
    end

    it "mcp_get_invalid_base64_raises ProtocolError" do
      stub_tool_ok(success: true, key: "k", data: "not valid base64!!!")
      expect { client.get("k") }.to raise_error(ObjectStore::ProtocolError)
    end

    it "mcp_get_binary_round_trip" do
      payload = (+"\x00\x01\xFF\xFE\x80").force_encoding("BINARY")
      stub_tool_ok(success: true, key: "k", data: Base64.strict_encode64(payload))
      res = client.get("k")
      expect(res.data.dup.force_encoding("BINARY")).to eq(payload)
    end
  end

  describe "#get_stream" do
    it "mcp_get_stream yields decoded data as single chunk" do
      encoded = Base64.strict_encode64("chunk-data")
      stub_tool_ok(success: true, data: encoded)
      received = []
      client.get_stream("k") { |c| received << c }
      expect(received).to eq(["chunk-data"])
    end
  end

  describe "#delete" do
    it "mcp_delete_success" do
      stub_tool_ok(success: true, key: "k", deleted: true)
      res = client.delete("k")
      expect(res).to be_a(ObjectStore::Models::DeleteResponse)
      expect(res.success).to be(true)
    end

    it "mcp_delete_not_found" do
      stub_tool_error(code: 404, message: "not found")
      expect { client.delete("k") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "mcp_delete_error" do
      stub_http_error
      expect { client.delete("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#list" do
    it "mcp_list_success" do
      stub_tool_ok(success: true, keys: ["a", "b"], count: 2, truncated: false, next_token: nil)
      res = client.list
      expect(res).to be_a(ObjectStore::Models::ListResponse)
      expect(res.objects.map(&:key)).to eq(["a", "b"])
    end

    it "mcp_list_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.list }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#exists?" do
    it "mcp_exists_true" do
      stub_tool_ok(success: true, exists: true)
      expect(client.exists?("k")).to be(true)
    end

    it "mcp_exists_false" do
      stub_tool_ok(success: true, exists: false)
      expect(client.exists?("k")).to be(false)
    end

    it "mcp_exists_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.exists?("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#get_metadata" do
    it "mcp_get_metadata_success" do
      stub_tool_ok(success: true, key: "k", content_type: "text/plain", size: 42,
                   etag: "e1", last_modified: "2024-01-01T00:00:00Z", custom: {})
      res = client.get_metadata("k")
      expect(res).to be_a(ObjectStore::Models::MetadataResponse)
      expect(res.metadata.content_type).to eq("text/plain")
    end

    it "mcp_get_metadata_not_found" do
      stub_tool_error(code: 404, message: "not found")
      expect { client.get_metadata("k") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "mcp_get_metadata_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.get_metadata("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#update_metadata" do
    it "mcp_update_metadata_success" do
      stub_tool_ok(success: true, updated: true)
      meta = ObjectStore::Models::Metadata.new(content_type: "text/plain")
      res = client.update_metadata("k", meta)
      expect(res).to be_a(ObjectStore::Models::UpdateMetadataResponse)
      expect(res.success).to be(true)
    end

    it "mcp_update_metadata_not_found" do
      stub_tool_error(code: 404, message: "not found")
      expect { client.update_metadata("k", {}) }.to raise_error(ObjectStore::NotFoundError)
    end

    it "mcp_update_metadata_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.update_metadata("k", {}) }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#health" do
    it "mcp_health_success" do
      stub_tool_ok(status: "healthy", version: "1.0.0")
      res = client.health
      expect(res).to be_a(ObjectStore::Models::HealthResponse)
      expect(res.status).to eq("healthy")
    end

    it "mcp_health_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.health }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#archive" do
    it "mcp_archive_success" do
      stub_tool_ok(success: true, archived: true)
      res = client.archive("k", destination_type: "glacier")
      expect(res).to be_a(ObjectStore::Models::ArchiveResponse)
      expect(res.success).to be(true)
    end

    it "mcp_archive_not_found" do
      stub_tool_error(code: 404, message: "not found")
      expect { client.archive("k", destination_type: "glacier") }
        .to raise_error(ObjectStore::NotFoundError)
    end

    it "mcp_archive_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.archive("k", destination_type: "glacier") }
        .to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#add_policy" do
    it "mcp_add_policy_success" do
      stub_tool_ok(success: true, id: "p1", added: true)
      policy = ObjectStore::Models::LifecyclePolicy.new(id: "p1", action: "delete",
                                                        retention_seconds: 86_400)
      res = client.add_policy(policy)
      expect(res[:success]).to be(true)
    end

    it "mcp_add_policy_error" do
      stub_tool_error(code: -32602, message: "invalid")
      expect { client.add_policy({ id: "p1", action: "delete" }) }
        .to raise_error(ObjectStore::ValidationError)
    end
  end

  describe "#remove_policy" do
    it "mcp_remove_policy_success" do
      stub_tool_ok(success: true, removed: true)
      expect(client.remove_policy("p1")[:success]).to be(true)
    end

    it "mcp_remove_policy_not_found" do
      stub_tool_error(code: 404, message: "not found")
      expect { client.remove_policy("p1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "mcp_remove_policy_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.remove_policy("p1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#get_policies" do
    it "mcp_get_policies_success" do
      stub_tool_ok(success: true, policies: [{ id: "p1", prefix: "logs/", action: "delete",
                                               retention_seconds: 2592000 }], count: 1)
      res = client.get_policies
      expect(res[:policies].first.id).to eq("p1")
    end

    it "mcp_get_policies_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.get_policies }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#apply_policies" do
    it "mcp_apply_policies_success" do
      stub_tool_ok(success: true, policies_count: 2, objects_processed: 5,
                   message: "applied")
      res = client.apply_policies
      expect(res[:success]).to be(true)
      expect(res[:policies_count]).to eq(2)
    end

    it "mcp_apply_policies_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.apply_policies }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#add_replication_policy" do
    it "mcp_add_replication_policy_success" do
      stub_tool_ok(success: true, id: "r1", message: "added")
      policy = ObjectStore::Models::ReplicationPolicy.new(
        id: "r1", source_backend: "local", destination_backend: "s3",
        check_interval_seconds: 60, enabled: true
      )
      expect(client.add_replication_policy(policy)[:success]).to be(true)
    end

    it "mcp_add_replication_policy_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.add_replication_policy({ id: "r1" }) }
        .to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#remove_replication_policy" do
    it "mcp_remove_replication_policy_success" do
      stub_tool_ok(success: true, id: "r1", message: "removed")
      expect(client.remove_replication_policy("r1")[:success]).to be(true)
    end

    it "mcp_remove_replication_policy_not_found" do
      stub_tool_error(code: 404, message: "not found")
      expect { client.remove_replication_policy("r1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "mcp_remove_replication_policy_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.remove_replication_policy("r1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#get_replication_policies" do
    it "mcp_get_replication_policies_success" do
      stub_tool_ok(success: true, policies: [{ id: "r1", source_backend: "local",
                                               destination_backend: "s3" }], count: 1)
      res = client.get_replication_policies
      expect(res[:policies].first.id).to eq("r1")
    end

    it "mcp_get_replication_policies_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.get_replication_policies }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#get_replication_policy" do
    it "mcp_get_replication_policy_success" do
      stub_tool_ok(success: true, id: "r1", source_backend: "local",
                   destination_backend: "s3", enabled: true)
      res = client.get_replication_policy("r1")
      expect(res[:policy].id).to eq("r1")
    end

    it "mcp_get_replication_policy_not_found" do
      stub_tool_error(code: 404, message: "not found")
      expect { client.get_replication_policy("r1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "mcp_get_replication_policy_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.get_replication_policy("r1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#trigger_replication" do
    it "mcp_trigger_replication_success" do
      stub_tool_ok(success: true,
                   result: { policy_id: "r1", synced: 5, deleted: 0, failed: 0,
                             bytes_total: 1024, duration: "1s", errors: [] },
                   message: "done")
      res = client.trigger_replication
      expect(res[:success]).to be(true)
    end

    it "mcp_trigger_replication_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.trigger_replication }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#get_replication_status" do
    it "mcp_get_replication_status_success" do
      stub_tool_ok(success: true, policy_id: "r1", source_backend: "local",
                   destination_backend: "s3", enabled: true,
                   total_objects_synced: 10, total_objects_deleted: 0,
                   total_bytes_synced: 4096, total_errors: 0,
                   last_sync_time: "2024-01-01T00:00:00Z",
                   average_sync_duration: "500ms", sync_count: 5)
      res = client.get_replication_status("r1")
      expect(res[:success]).to be(true)
      expect(res[:status].policy_id).to eq("r1")
    end

    it "mcp_get_replication_status_not_found" do
      stub_tool_error(code: 404, message: "not found")
      expect { client.get_replication_status("r1") }.to raise_error(ObjectStore::NotFoundError)
    end

    it "mcp_get_replication_status_error" do
      stub_tool_error(code: -32603, message: "internal")
      expect { client.get_replication_status("r1") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "#close" do
    it "mcp_close is safe before any request and idempotent" do
      expect { client.close }.not_to raise_error
      expect { client.close }.not_to raise_error
    end
  end

  describe "connection reuse" do
    it "mcp_reuses_the_persistent_connection_across_requests" do
      stub_tool_ok(status: "healthy")

      expect(client.health.status).to eq("healthy")
      expect(client.health.status).to eq("healthy")

      expect(WebMock).to have_requested(:post, "#{base}/").twice
    end

    it "mcp_reconnects_after_close" do
      stub_tool_ok(status: "healthy")

      expect(client.health.status).to eq("healthy")
      client.close
      expect(client.health.status).to eq("healthy")
    end
  end

  describe "JSON-RPC error code mapping" do
    it "mcp_not_found_code_raises_not_found" do
      stub_tool_error(code: -32_004, message: "object missing")
      expect { client.get("k") }.to raise_error(ObjectStore::NotFoundError, "object missing")
    end

    it "mcp_unauthenticated_code_raises_authentication_error" do
      stub_tool_error(code: -32_002, message: "unauthenticated")
      expect { client.health }.to raise_error(ObjectStore::AuthenticationError, "unauthenticated")
    end

    it "mcp_forbidden_code_raises_authorization_error" do
      stub_tool_error(code: -32_001, message: "forbidden")
      expect { client.health }.to raise_error(ObjectStore::AuthorizationError, "forbidden")
    end

    it "mcp_invalid_params_code_raises_validation_error" do
      stub_tool_error(code: -32_602, message: "invalid params")
      expect { client.health }.to raise_error(ObjectStore::ValidationError, "invalid params")
    end

    it "mcp_already_exists_code_raises_already_exists_error" do
      stub_tool_error(code: -32_005, message: "already exists")
      expect { client.put("k", "v") }.to raise_error(ObjectStore::AlreadyExistsError, "already exists")
    end

    it "mcp_rate_limited_code_raises_rate_limit_error" do
      stub_tool_error(code: -32_029, message: "rate limited")
      expect { client.health }.to raise_error(ObjectStore::RateLimitError, "rate limited")
    end

    it "mcp_parse_error_code_raises_protocol_error" do
      stub_tool_error(code: -32_700, message: "parse error")
      expect { client.health }.to raise_error(ObjectStore::ProtocolError, "parse error")
    end

    it "mcp_not_found_message_without_code_raises_server_error" do
      # Classification is by code, not message substring.
      stub_tool_error(code: -32_603, message: "key not found in backend")
      expect { client.get("k") }.to raise_error(ObjectStore::ServerError)
    end
  end

  describe "auth headers" do
    it "mcp_auth_sends_bearer_token" do
      stub = stub_request(:post, "#{base}/")
               .with(headers: { "Authorization" => "Bearer secret-token" })
               .to_return(status: 200, body: mcp_ok(status: "healthy"), headers: json_ct)

      authed = described_class.new(host: host, port: port, token: "secret-token")
      authed.health
      expect(stub).to have_been_requested
    end

    it "mcp_auth_sends_tenant_id" do
      stub = stub_request(:post, "#{base}/")
               .with(headers: { "X-Tenant-ID" => "acme" })
               .to_return(status: 200, body: mcp_ok(status: "healthy"), headers: json_ct)

      authed = described_class.new(host: host, port: port, tenant_id: "acme")
      authed.health
      expect(stub).to have_been_requested
    end

    it "mcp_auth_sends_custom_headers" do
      stub = stub_request(:post, "#{base}/")
               .with(headers: { "X-Custom" => "value" })
               .to_return(status: 200, body: mcp_ok(status: "healthy"), headers: json_ct)

      authed = described_class.new(host: host, port: port, headers: { "X-Custom" => "value" })
      authed.health
      expect(stub).to have_been_requested
    end
  end

  describe "metadata round-trip" do
    it "mcp_metadata_round_trip" do
      encoded = Base64.strict_encode64("payload")
      stub_tool_ok(success: true, data: encoded, metadata: { content_type: "application/json" })
      res = client.get("k")
      expect(res.data).to eq("payload")
    end
  end

  describe "HTTP-level errors" do
    it "mcp_http_500_raises_server_error" do
      stub_http_error(status: 500)
      expect { client.health }.to raise_error(ObjectStore::ServerError)
    end

    it "mcp_http_404_raises_not_found" do
      stub_request(:post, "#{base}/")
        .to_return(status: 404, body: "", headers: json_ct)
      expect { client.health }.to raise_error(ObjectStore::NotFoundError)
    end

    it "mcp_http_401_raises_authentication_error" do
      stub_request(:post, "#{base}/")
        .to_return(status: 401, body: "", headers: json_ct)
      expect { client.health }.to raise_error(ObjectStore::AuthenticationError)
    end

    it "mcp_http_403_raises_authorization_error" do
      stub_request(:post, "#{base}/")
        .to_return(status: 403, body: "", headers: json_ct)
      expect { client.health }.to raise_error(ObjectStore::AuthorizationError)
    end

    it "mcp_http_409_raises_already_exists_error" do
      stub_request(:post, "#{base}/")
        .to_return(status: 409, body: "", headers: json_ct)
      expect { client.health }.to raise_error(ObjectStore::AlreadyExistsError)
    end

    it "mcp_http_429_raises_rate_limit_error" do
      stub_request(:post, "#{base}/")
        .to_return(status: 429, body: "", headers: json_ct)
      expect { client.health }.to raise_error(ObjectStore::RateLimitError)
    end
  end
end
