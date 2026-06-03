# frozen_string_literal: true

require "spec_helper"
require "socket"
require "tempfile"
require "base64"
require "json"

# Unit tests for UnixClient. No live server is needed: a real Unix socket pair
# is created per example using UNIXServer/UNIXSocket so we control the exact
# bytes on the wire without stubbing Ruby internals.
RSpec.describe ObjectStore::Clients::UnixClient do
  # Build a temporary socket path backed by a tmp file.
  def tmp_socket_path
    f = Tempfile.new("objstore_test")
    path = f.path
    f.close
    f.unlink
    path
  end

  # Run a minimal fake server in a thread that:
  #   1. Accepts one connection.
  #   2. Reads one newline-terminated JSON-RPC request.
  #   3. Returns the caller-supplied response JSON (with the request id
  #      echoed back) followed by "\n".
  #   4. Closes the connection.
  #
  # Returns [thread, socket_path].
  def with_fake_server(response_hash)
    path = tmp_socket_path
    server = UNIXServer.new(path)
    t = Thread.new do
      conn = server.accept
      line = conn.readline
      id = JSON.parse(line)["id"]
      conn.write({ jsonrpc: "2.0", id: id }.merge(response_hash).to_json + "\n")
      conn.close
      server.close
    end
    [t, path]
  end

  # Variant that returns a JSON-RPC error response.
  def with_error_server(code:, message:)
    with_fake_server(error: { code: code, message: message })
  end

  def ok_response(result)
    { result: result }
  end

  describe "#put" do
    it "unix_put_success returns a PutResponse" do
      t, path = with_fake_server(ok_response(success: true, message: "stored"))
      client = described_class.new(socket_path: path)

      res = client.put("k", "hello")
      t.join

      expect(res).to be_a(ObjectStore::Models::PutResponse)
      expect(res.success).to be(true)
    end

    it "unix_put_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.put("k", "hello") }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#put_stream" do
    it "unix_put_stream buffers IO and delegates to put" do
      t, path = with_fake_server(ok_response(success: true))
      client = described_class.new(socket_path: path)
      io = StringIO.new("stream data")

      res = client.put_stream("k", io)
      t.join

      expect(res).to be_a(ObjectStore::Models::PutResponse)
    end
  end

  describe "#get" do
    it "unix_get_success decodes base64 data" do
      encoded = Base64.strict_encode64("world")
      t, path = with_fake_server(
        ok_response(data: encoded, metadata: { content_type: "text/plain", custom: {} })
      )
      client = described_class.new(socket_path: path)

      res = client.get("k")
      t.join

      expect(res).to be_a(ObjectStore::Models::GetResponse)
      expect(res.data).to eq("world")
    end

    it "unix_get_not_found raises NotFoundError" do
      t, path = with_error_server(code: 404, message: "not found")
      client = described_class.new(socket_path: path)

      expect { client.get("k") }.to raise_error(ObjectStore::NotFoundError)
      t.join
    end

    it "unix_get_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "internal")
      client = described_class.new(socket_path: path)

      expect { client.get("k") }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#get_stream" do
    it "unix_get_stream yields decoded data as single chunk" do
      encoded = Base64.strict_encode64("chunk")
      t, path = with_fake_server(
        ok_response(data: encoded, metadata: { custom: {} })
      )
      client = described_class.new(socket_path: path)
      received = []

      meta = client.get_stream("k") { |c| received << c }
      t.join

      expect(received).to eq(["chunk"])
      expect(meta).to be_a(ObjectStore::Models::Metadata)
    end
  end

  describe "#delete" do
    it "unix_delete_success" do
      t, path = with_fake_server(ok_response(success: true, message: "deleted"))
      client = described_class.new(socket_path: path)

      res = client.delete("k")
      t.join

      expect(res).to be_a(ObjectStore::Models::DeleteResponse)
      expect(res.success).to be(true)
    end

    it "unix_delete_not_found raises NotFoundError" do
      t, path = with_error_server(code: 404, message: "not found")
      client = described_class.new(socket_path: path)

      expect { client.delete("k") }.to raise_error(ObjectStore::NotFoundError)
      t.join
    end

    it "unix_delete_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.delete("k") }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#list" do
    it "unix_list_success returns ListResponse" do
      result = {
        objects: [{ key: "a", size: 10, last_modified: "2024-01-01T00:00:00Z", etag: "" }],
        next_cursor: nil,
        is_truncated: false
      }
      t, path = with_fake_server(ok_response(result))
      client = described_class.new(socket_path: path)

      res = client.list
      t.join

      expect(res).to be_a(ObjectStore::Models::ListResponse)
      expect(res.objects.first.key).to eq("a")
    end

    it "unix_list_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.list }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#exists?" do
    it "unix_exists_true" do
      t, path = with_fake_server(ok_response(exists: true))
      client = described_class.new(socket_path: path)

      expect(client.exists?("k")).to be(true)
      t.join
    end

    it "unix_exists_false" do
      t, path = with_fake_server(ok_response(exists: false))
      client = described_class.new(socket_path: path)

      expect(client.exists?("k")).to be(false)
      t.join
    end

    it "unix_exists_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.exists?("k") }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#get_metadata" do
    it "unix_get_metadata_success" do
      result = {
        metadata: { content_type: "text/plain", custom: {} },
        size: 42,
        etag: "e1"
      }
      t, path = with_fake_server(ok_response(result))
      client = described_class.new(socket_path: path)

      res = client.get_metadata("k")
      t.join

      expect(res).to be_a(ObjectStore::Models::MetadataResponse)
      expect(res.metadata.content_type).to eq("text/plain")
    end

    it "unix_get_metadata_not_found raises NotFoundError" do
      t, path = with_error_server(code: 404, message: "not found")
      client = described_class.new(socket_path: path)

      expect { client.get_metadata("k") }.to raise_error(ObjectStore::NotFoundError)
      t.join
    end

    it "unix_get_metadata_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.get_metadata("k") }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#update_metadata" do
    it "unix_update_metadata_success" do
      t, path = with_fake_server(ok_response(success: true))
      client = described_class.new(socket_path: path)
      meta = ObjectStore::Models::Metadata.new(content_type: "text/plain")

      res = client.update_metadata("k", meta)
      t.join

      expect(res).to be_a(ObjectStore::Models::UpdateMetadataResponse)
      expect(res.success).to be(true)
    end

    it "unix_update_metadata_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.update_metadata("k", {}) }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#health" do
    it "unix_health_success" do
      t, path = with_fake_server(ok_response(status: "healthy", version: "1.0.0"))
      client = described_class.new(socket_path: path)

      res = client.health
      t.join

      expect(res).to be_a(ObjectStore::Models::HealthResponse)
      expect(res.status).to eq("healthy")
    end

    it "unix_health_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.health }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#archive" do
    it "unix_archive_success" do
      t, path = with_fake_server(ok_response(success: true))
      client = described_class.new(socket_path: path)

      res = client.archive("k", destination_type: "glacier")
      t.join

      expect(res).to be_a(ObjectStore::Models::ArchiveResponse)
      expect(res.success).to be(true)
    end

    it "unix_archive_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.archive("k", destination_type: "glacier") }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#add_policy" do
    it "unix_add_policy_success" do
      t, path = with_fake_server(ok_response(success: true))
      client = described_class.new(socket_path: path)
      policy = ObjectStore::Models::LifecyclePolicy.new(
        id: "p1", action: "delete", retention_seconds: 86_400
      )

      res = client.add_policy(policy)
      t.join

      expect(res[:success]).to be(true)
    end

    it "unix_add_policy_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.add_policy({ id: "p1", action: "delete" }) }
        .to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#remove_policy" do
    it "unix_remove_policy_success" do
      t, path = with_fake_server(ok_response(success: true))
      client = described_class.new(socket_path: path)

      res = client.remove_policy("p1")
      t.join

      expect(res[:success]).to be(true)
    end

    it "unix_remove_policy_not_found raises NotFoundError" do
      t, path = with_error_server(code: 404, message: "not found")
      client = described_class.new(socket_path: path)

      expect { client.remove_policy("p1") }.to raise_error(ObjectStore::NotFoundError)
      t.join
    end

    it "unix_remove_policy_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.remove_policy("p1") }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#get_policies" do
    it "unix_get_policies_success parses the bare-array result" do
      # The Unix server returns a bare JSON array, not a wrapped hash.
      result = [
        { id: "p1", prefix: "logs/", action: "delete", after_days: 1,
          retention_seconds: 90_000 }
      ]
      t, path = with_fake_server(ok_response(result))
      client = described_class.new(socket_path: path)

      res = client.get_policies
      t.join

      expect(res[:policies].length).to eq(1)
      expect(res[:policies].first.id).to eq("p1")
      expect(res[:policies].first.retention_seconds).to eq(90_000)
    end

    it "unix_get_policies_falls_back_to_after_days" do
      result = [{ id: "p1", prefix: "logs/", action: "delete", after_days: 30 }]
      t, path = with_fake_server(ok_response(result))
      client = described_class.new(socket_path: path)

      res = client.get_policies
      t.join

      expect(res[:policies].length).to eq(1)
      expect(res[:policies].first.retention_seconds).to eq(30 * 86_400)
    end

    it "unix_get_policies_accepts_wrapped_hash defensively" do
      result = {
        policies: [{ id: "p1", prefix: "logs/", action: "delete", retention_seconds: 3600 }]
      }
      t, path = with_fake_server(ok_response(result))
      client = described_class.new(socket_path: path)

      res = client.get_policies
      t.join

      expect(res[:policies].length).to eq(1)
      expect(res[:policies].first.retention_seconds).to eq(3600)
    end

    it "unix_get_policies_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.get_policies }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#apply_policies" do
    it "unix_apply_policies_success" do
      t, path = with_fake_server(ok_response(success: true, policies_count: 2, objects_processed: 5))
      client = described_class.new(socket_path: path)

      res = client.apply_policies
      t.join

      expect(res[:success]).to be(true)
      expect(res[:policies_count]).to eq(2)
    end

    it "unix_apply_policies_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.apply_policies }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#add_replication_policy" do
    it "unix_add_replication_policy_success" do
      t, path = with_fake_server(ok_response(success: true))
      client = described_class.new(socket_path: path)
      policy = ObjectStore::Models::ReplicationPolicy.new(
        id: "r1", destination_backend: "s3", enabled: true
      )

      res = client.add_replication_policy(policy)
      t.join

      expect(res[:success]).to be(true)
    end

    it "unix_add_replication_policy_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.add_replication_policy({ id: "r1" }) }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#remove_replication_policy" do
    it "unix_remove_replication_policy_success" do
      t, path = with_fake_server(ok_response(success: true))
      client = described_class.new(socket_path: path)

      expect(client.remove_replication_policy("r1")[:success]).to be(true)
      t.join
    end

    it "unix_remove_replication_policy_not_found raises NotFoundError" do
      t, path = with_error_server(code: 404, message: "not found")
      client = described_class.new(socket_path: path)

      expect { client.remove_replication_policy("r1") }.to raise_error(ObjectStore::NotFoundError)
      t.join
    end

    it "unix_remove_replication_policy_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.remove_replication_policy("r1") }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#get_replication_policies" do
    it "unix_get_replication_policies_success parses the bare-array result" do
      # The Unix server returns a bare JSON array, not a wrapped hash.
      t, path = with_fake_server(ok_response([{ id: "r1", destination_type: "s3" }]))
      client = described_class.new(socket_path: path)

      res = client.get_replication_policies
      t.join

      expect(res[:policies].length).to eq(1)
      expect(res[:policies].first.id).to eq("r1")
      expect(res[:policies].first.destination_backend).to eq("s3")
    end

    it "unix_get_replication_policies_accepts_wrapped_hash defensively" do
      t, path = with_fake_server(ok_response(policies: [{ id: "r1", destination_type: "s3" }]))
      client = described_class.new(socket_path: path)

      res = client.get_replication_policies
      t.join

      expect(res[:policies].length).to eq(1)
      expect(res[:policies].first.id).to eq("r1")
    end

    it "unix_get_replication_policies_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.get_replication_policies }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#get_replication_policy" do
    it "unix_get_replication_policy_success" do
      t, path = with_fake_server(ok_response(id: "r1", destination_type: "s3", enabled: true))
      client = described_class.new(socket_path: path)

      res = client.get_replication_policy("r1")
      t.join

      expect(res[:policy].id).to eq("r1")
    end

    it "unix_get_replication_policy_not_found raises NotFoundError" do
      t, path = with_error_server(code: 404, message: "not found")
      client = described_class.new(socket_path: path)

      expect { client.get_replication_policy("r1") }.to raise_error(ObjectStore::NotFoundError)
      t.join
    end

    it "unix_get_replication_policy_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.get_replication_policy("r1") }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#trigger_replication" do
    it "unix_trigger_replication_success" do
      t, path = with_fake_server(ok_response(success: true, objects_synced: 3, objects_failed: 0,
                                             bytes_transferred: 1024, errors: []))
      client = described_class.new(socket_path: path)

      res = client.trigger_replication
      t.join

      expect(res[:success]).to be(true)
      expect(res[:result][:objects_synced]).to eq(3)
    end

    it "unix_trigger_replication_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.trigger_replication }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#get_replication_status" do
    it "unix_get_replication_status_success" do
      t, path = with_fake_server(ok_response(
                                   success: true, policy_id: "r1", enabled: true,
                                   objects_synced: 10, objects_failed: 0
                                 ))
      client = described_class.new(socket_path: path)

      res = client.get_replication_status("r1")
      t.join

      expect(res[:success]).to be(true)
      expect(res[:status].policy_id).to eq("r1")
    end

    it "unix_get_replication_status_not_found raises NotFoundError" do
      t, path = with_error_server(code: 404, message: "not found")
      client = described_class.new(socket_path: path)

      expect { client.get_replication_status("r1") }.to raise_error(ObjectStore::NotFoundError)
      t.join
    end

    it "unix_get_replication_status_error raises ServerError" do
      t, path = with_error_server(code: -32603, message: "boom")
      client = described_class.new(socket_path: path)

      expect { client.get_replication_status("r1") }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "#close" do
    it "unix_close releases the connection and is safe to repeat" do
      _, path = with_fake_server(ok_response(status: "healthy"))
      client = described_class.new(socket_path: path)

      # Should not raise even if never connected, and is idempotent.
      expect { client.close }.not_to raise_error
      expect { client.close }.not_to raise_error
    end
  end

  describe "persistent connection" do
    it "unix_reuses_one_connection_across_requests" do
      path = tmp_socket_path
      server = UNIXServer.new(path)
      accepts = Queue.new
      t = Thread.new do
        conn = server.accept
        accepts << true
        2.times do
          line = conn.readline
          id = JSON.parse(line)["id"]
          conn.write({ jsonrpc: "2.0", id: id, result: { status: "healthy" } }.to_json + "\n")
        end
        conn.close
        server.close
      end

      client = described_class.new(socket_path: path)
      expect(client.health.status).to eq("healthy")
      expect(client.health.status).to eq("healthy")
      t.join

      expect(accepts.size).to eq(1)
    end

    it "unix_reconnects_after_close" do
      path = tmp_socket_path
      server = UNIXServer.new(path)
      accepts = Queue.new
      t = Thread.new do
        2.times do
          conn = server.accept
          accepts << true
          line = conn.readline
          id = JSON.parse(line)["id"]
          conn.write({ jsonrpc: "2.0", id: id, result: { status: "healthy" } }.to_json + "\n")
          conn.close
        end
        server.close
      end

      client = described_class.new(socket_path: path)
      expect(client.health.status).to eq("healthy")
      client.close
      expect(client.health.status).to eq("healthy")
      t.join

      expect(accepts.size).to eq(2)
    end

    it "unix_response_id_mismatch raises ProtocolError and drops the connection" do
      path = tmp_socket_path
      server = UNIXServer.new(path)
      t = Thread.new do
        conn = server.accept
        conn.readline
        conn.write({ jsonrpc: "2.0", id: 999, result: {} }.to_json + "\n")
        conn.close
        server.close
      end

      client = described_class.new(socket_path: path)
      expect { client.health }
        .to raise_error(ObjectStore::ProtocolError, /does not match request id/)
      t.join
    end
  end

  describe "connection failure" do
    it "unix_no_socket raises ConnectionError" do
      client = described_class.new(socket_path: "/nonexistent/path/objstore.sock")
      expect { client.put("k", "data") }.to raise_error(ObjectStore::ConnectionError)
    end
  end

  describe "timeouts" do
    it "unix_unresponsive_server raises TimeoutError within the configured window" do
      path = tmp_socket_path
      server = UNIXServer.new(path)
      t = Thread.new do
        conn = server.accept
        sleep 5 # never respond within the client's timeout
        conn.close
        server.close
      end

      client = described_class.new(socket_path: path, timeout: 0.2)
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      expect { client.health }.to raise_error(ObjectStore::TimeoutError)
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      expect(elapsed).to be < 3

      t.kill
      t.join
    end

    it "unix_truncated_response raises ConnectionError" do
      path = tmp_socket_path
      server = UNIXServer.new(path)
      t = Thread.new do
        conn = server.accept
        conn.readline
        conn.write("{\"jsonrpc\":\"2.0\"") # no newline, then close
        conn.close
        server.close
      end

      client = described_class.new(socket_path: path, timeout: 1)
      expect { client.health }.to raise_error(ObjectStore::ConnectionError)
      t.join
    end
  end

  describe "error code mapping" do
    it "unix_forbidden_code raises AuthorizationError" do
      t, path = with_error_server(code: -32_001, message: "forbidden")
      client = described_class.new(socket_path: path)

      expect { client.health }.to raise_error(ObjectStore::AuthorizationError, "forbidden")
      t.join
    end

    it "unix_unauthenticated_code raises AuthenticationError" do
      t, path = with_error_server(code: -32_002, message: "unauthenticated")
      client = described_class.new(socket_path: path)

      expect { client.health }.to raise_error(ObjectStore::AuthenticationError, "unauthenticated")
      t.join
    end

    it "unix_not_found_code raises NotFoundError" do
      t, path = with_error_server(code: -32_004, message: "object missing")
      client = described_class.new(socket_path: path)

      expect { client.get("k") }.to raise_error(ObjectStore::NotFoundError, "object missing")
      t.join
    end

    it "unix_invalid_params_code raises ValidationError" do
      t, path = with_error_server(code: -32_602, message: "invalid params")
      client = described_class.new(socket_path: path)

      expect { client.health }.to raise_error(ObjectStore::ValidationError, "invalid params")
      t.join
    end

    it "unix_already_exists_code raises AlreadyExistsError" do
      t, path = with_error_server(code: -32_005, message: "already exists")
      client = described_class.new(socket_path: path)

      expect { client.put("k", "v") }.to raise_error(ObjectStore::AlreadyExistsError, "already exists")
      t.join
    end

    it "unix_rate_limited_code raises RateLimitError" do
      t, path = with_error_server(code: -32_029, message: "rate limited")
      client = described_class.new(socket_path: path)

      expect { client.health }.to raise_error(ObjectStore::RateLimitError, "rate limited")
      t.join
    end

    it "unix_parse_error_code raises ProtocolError" do
      t, path = with_error_server(code: -32_700, message: "parse error")
      client = described_class.new(socket_path: path)

      expect { client.health }.to raise_error(ObjectStore::ProtocolError, "parse error")
      t.join
    end

    it "unix_not_found_message_without_code raises ServerError (no substring matching)" do
      t, path = with_error_server(code: -32_603, message: "key not found in backend")
      client = described_class.new(socket_path: path)

      expect { client.get("k") }.to raise_error(ObjectStore::ServerError)
      t.join
    end
  end

  describe "protocol violations" do
    it "unix_malformed_json raises ProtocolError" do
      path = tmp_socket_path
      server = UNIXServer.new(path)
      t = Thread.new do
        conn = server.accept
        _line = conn.readline
        conn.write("this is not json\n")
        conn.close
        server.close
      end
      client = described_class.new(socket_path: path)

      expect { client.health }.to raise_error(ObjectStore::ProtocolError, /Invalid JSON/)
      t.join
    end
  end

  describe "retention round-trip" do
    # Capture the raw request line while serving a success response.
    # Returns [thread, socket_path, queue-with-captured-line].
    def with_capturing_server(result)
      path = tmp_socket_path
      server = UNIXServer.new(path)
      captured = Queue.new
      t = Thread.new do
        conn = server.accept
        line = conn.readline
        captured << line
        id = JSON.parse(line)["id"]
        conn.write({ jsonrpc: "2.0", id: id, result: result }.to_json + "\n")
        conn.close
        server.close
      end
      [t, path, captured]
    end

    it "unix_add_policy_sends_exact_retention_seconds" do
      t, path, captured = with_capturing_server(success: true)
      client = described_class.new(socket_path: path)
      # Sub-day retention is allowed; retention_seconds carries the exact
      # value and takes precedence over after_days server-side.
      policy = ObjectStore::Models::LifecyclePolicy.new(
        id: "p1", prefix: "tmp/", action: "delete", retention_seconds: 86_401
      )

      result = client.add_policy(policy)
      t.join

      expect(result[:success]).to be(true)
      request = JSON.parse(captured.pop)
      expect(request.dig("params", "retention_seconds")).to eq(86_401)
      expect(request.dig("params", "after_days")).to eq(1)
    end

    it "unix_add_policy_accepts_whole_days" do
      t, path, captured = with_capturing_server(success: true)
      client = described_class.new(socket_path: path)
      policy = ObjectStore::Models::LifecyclePolicy.new(
        id: "p1", prefix: "tmp/", action: "delete", retention_seconds: 172_800
      )

      result = client.add_policy(policy)
      t.join

      expect(result[:success]).to be(true)
      request = JSON.parse(captured.pop)
      expect(request.dig("params", "retention_seconds")).to eq(172_800)
      expect(request.dig("params", "after_days")).to eq(2)
    end
  end

  describe "metadata round-trip" do
    it "unix_metadata_round_trip sends and receives metadata" do
      encoded = Base64.strict_encode64("payload")
      t, path = with_fake_server(
        ok_response(
          data: encoded,
          metadata: { content_type: "application/json", content_encoding: nil, custom: { "x" => "y" } }
        )
      )
      client = described_class.new(socket_path: path)

      res = client.get("k")
      t.join

      expect(res.data).to eq("payload")
      expect(res.metadata.content_type).to eq("application/json")
    end
  end
end
