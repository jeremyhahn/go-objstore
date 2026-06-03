require "socket"
require "json"
require "base64"

module ObjectStore
  module Clients
    # Unix domain socket client for ObjectStore.
    #
    # Communicates over a local Unix socket using newline-delimited JSON-RPC 2.0.
    # A single persistent connection is established lazily and reused across
    # requests; the server keeps connections open between requests and closes
    # them after ~30s of inactivity, in which case the client reconnects on
    # the next call. Authentication is handled server-side via peer
    # credentials; the client simply connects. Binary object data is
    # base64-encoded on the wire.
    #
    # Streaming (put_stream/get_stream) is supported by buffering the IO in
    # memory prior to framing, which is appropriate for message-framed transports
    # that cannot interleave JSON frames mid-stream.
    #
    # @api private
    class UnixClient
      include JsonRpcHelpers

      attr_reader :host, :port, :use_ssl, :timeout

      # Initialize a new Unix socket client.
      #
      # @param socket_path [String] Path to the Unix domain socket file
      # @param timeout [Integer] I/O timeout in seconds
      def initialize(socket_path: "/tmp/objstore.sock", timeout: 30, **_ignored)
        @socket_path = socket_path
        @timeout = timeout
        # host/port/use_ssl are exposed for API parity with HTTP clients so that
        # switch_protocol can call @client.host etc. unconditionally.
        @host = "localhost"
        @port = nil
        @use_ssl = false
        @id_counter = 0
        @id_mutex = Mutex.new
        @sock = nil
        @conn_mutex = Mutex.new
      end

      # Upload an object to the store.
      #
      # @param key [String] Object key
      # @param data [String] Object data (may be binary)
      # @param metadata [Models::Metadata, Hash, nil] Optional metadata
      #
      # @return [Models::PutResponse] Response with success status
      #
      # @raise [ValidationError, ServerError, ConnectionError]
      def put(key, data, metadata = nil)
        metadata_obj = metadata.is_a?(Models::Metadata) ? metadata : Models::Metadata.new(metadata || {})
        params = { key: key, data: Base64.strict_encode64(data.to_s) }
        params[:metadata] = build_metadata_params(metadata_obj) unless metadata_obj_empty?(metadata_obj)

        rpc_call("put", params) do |result|
          Models::PutResponse.new(success: result.fetch("success", true), message: result["message"])
        end
      end

      # Upload an object from an IO stream.
      #
      # Reads the entire IO into memory before framing (buffered; acceptable for
      # message-framed transports).
      #
      # @param key [String] Object key
      # @param io [IO, #read] IO object to read from
      # @param metadata [Models::Metadata, Hash, nil] Optional metadata
      # @param chunk_size [Integer] Size of chunks to read
      #
      # @return [Models::PutResponse] Response with success status
      def put_stream(key, io, metadata: nil, chunk_size: 8192)
        data = String.new(encoding: "BINARY")
        while (chunk = io.read(chunk_size))
          data << chunk
        end
        put(key, data, metadata)
      end

      # Retrieve an object from the store.
      #
      # @param key [String] Object key
      #
      # @return [Models::GetResponse] Response with data and metadata
      #
      # @raise [NotFoundError, ServerError, ConnectionError]
      def get(key)
        rpc_call("get", { key: key }) do |result|
          data = Base64.strict_decode64(result.fetch("data", ""))
          metadata = parse_metadata_result(result["metadata"])
          Models::GetResponse.new(data, metadata)
        end
      end

      # Retrieve an object and yield it as a single chunk.
      #
      # The Unix protocol is message-framed; the full object is received in one
      # JSON response and yielded as a single chunk for streaming API parity.
      #
      # @param key [String] Object key
      #
      # @yieldparam chunk [String] The full object data as one chunk
      #
      # @return [Models::Metadata] Object metadata
      #
      # @raise [NotFoundError, ServerError, ConnectionError]
      def get_stream(key)
        rpc_call("get", { key: key }) do |result|
          data = Base64.strict_decode64(result.fetch("data", ""))
          yield data if block_given?
          parse_metadata_result(result["metadata"])
        end
      end

      # Delete an object from the store.
      #
      # @param key [String] Object key
      #
      # @return [Models::DeleteResponse] Response with success status
      #
      # @raise [NotFoundError, ServerError, ConnectionError]
      def delete(key)
        rpc_call("delete", { key: key }) do |result|
          Models::DeleteResponse.new(success: result.fetch("success", true), message: result["message"])
        end
      end

      # List objects in the store.
      #
      # @param prefix [String, nil] Filter by key prefix
      # @param delimiter [String, nil] Delimiter (passed through; not interpreted by Unix server)
      # @param max_results [Integer] Maximum results to return
      # @param continue_from [String, nil] Pagination cursor from previous response
      #
      # @return [Models::ListResponse] List response with objects
      #
      # @raise [ServerError, ConnectionError]
      def list(prefix: nil, delimiter: nil, max_results: 100, continue_from: nil)
        params = {}
        params[:prefix] = prefix if prefix
        params[:delimiter] = delimiter if delimiter
        params[:max_results] = max_results if max_results
        params[:continue_from] = continue_from if continue_from

        rpc_call("list", params) do |result|
          objects = (result["objects"] || []).map do |obj|
            Models::ObjectInfo.new(
              key: obj["key"],
              metadata: Models::Metadata.new(
                size: obj["size"],
                last_modified: obj["last_modified"],
                etag: obj["etag"]
              )
            )
          end

          Models::ListResponse.new(
            objects: objects,
            next_token: result["next_cursor"],
            truncated: result.fetch("is_truncated", false)
          )
        end
      end

      # Check if an object exists.
      #
      # @param key [String] Object key
      #
      # @return [Boolean] true if the object exists
      #
      # @raise [ServerError, ConnectionError]
      def exists?(key)
        rpc_call("exists", { key: key }) do |result|
          result.fetch("exists", false)
        end
      end

      # Get metadata for an object.
      #
      # @param key [String] Object key
      #
      # @return [Models::MetadataResponse] Response with metadata
      #
      # @raise [NotFoundError, ServerError, ConnectionError]
      def get_metadata(key)
        rpc_call("get_metadata", { key: key }) do |result|
          meta = result["metadata"] || {}
          Models::MetadataResponse.new(
            metadata: Models::Metadata.new(
              content_type: meta["content_type"],
              content_encoding: meta["content_encoding"],
              size: result["size"],
              etag: result["etag"],
              last_modified: result["last_modified"],
              custom: meta["custom"] || {}
            ),
            success: true
          )
        end
      end

      # Update metadata for an object.
      #
      # @param key [String] Object key
      # @param metadata [Models::Metadata, Hash] New metadata
      #
      # @return [Models::UpdateMetadataResponse] Response with success status
      #
      # @raise [NotFoundError, ServerError, ConnectionError]
      def update_metadata(key, metadata)
        metadata_obj = metadata.is_a?(Models::Metadata) ? metadata : Models::Metadata.new(metadata)
        params = { key: key, metadata: build_metadata_params(metadata_obj) }

        rpc_call("update_metadata", params) do |result|
          Models::UpdateMetadataResponse.new(success: result.fetch("success", true), message: result["message"])
        end
      end

      # Check server health status.
      #
      # @param service [String, nil] Ignored; included for API parity
      #
      # @return [Models::HealthResponse] Response with health status
      #
      # @raise [ServerError, ConnectionError]
      def health(service: nil)
        rpc_call("health", {}) do |result|
          Models::HealthResponse.new(
            status: result.fetch("status", "healthy"),
            message: result["version"]
          )
        end
      end

      # Archive an object to external storage.
      #
      # @param key [String] Object key
      # @param destination_type [String] Archive destination type (e.g. "glacier")
      # @param destination_settings [Hash] Settings for the destination
      #
      # @return [Models::ArchiveResponse] Response with success status
      #
      # @raise [NotFoundError, ServerError, ConnectionError]
      def archive(key, destination_type:, destination_settings: {})
        params = {
          key: key,
          destination_type: destination_type,
          destination_settings: destination_settings
        }

        rpc_call("archive", params) do |result|
          Models::ArchiveResponse.new(success: result.fetch("success", true), message: result["message"])
        end
      end

      # Add a lifecycle policy.
      #
      # @param policy [Models::LifecyclePolicy, Hash] Policy to add
      #
      # @return [Hash] Response with :success and :message keys
      #
      # @raise [ValidationError, ServerError, ConnectionError]
      def add_policy(policy)
        policy_obj = policy.is_a?(Models::LifecyclePolicy) ? policy : Models::LifecyclePolicy.new(policy)
        retention = policy_obj.retention_seconds || 0
        params = {
          id: policy_obj.id,
          prefix: policy_obj.prefix || "",
          action: policy_obj.action,
          # retention_seconds carries the exact retention and takes precedence
          # server-side; after_days is included (rounded down) for backward
          # compatibility with older servers.
          retention_seconds: retention,
          after_days: retention / 86_400
        }
        params[:destination_type] = policy_obj.destination_type if policy_obj.destination_type
        params[:destination_settings] = policy_obj.destination_settings if policy_obj.destination_settings&.any?

        rpc_call("add_policy", params) do |result|
          { success: result.fetch("success", true), message: result["message"] }
        end
      end

      # Remove a lifecycle policy.
      #
      # @param id [String] Policy ID
      #
      # @return [Hash] Response with :success and :message keys
      #
      # @raise [NotFoundError, ServerError, ConnectionError]
      def remove_policy(id)
        rpc_call("remove_policy", { id: id }) do |result|
          { success: result.fetch("success", true), message: result["message"] }
        end
      end

      # Get all lifecycle policies.
      #
      # @param prefix [String, nil] Filter by prefix
      #
      # @return [Hash] Response with :policies array and :success key
      #
      # @raise [ServerError, ConnectionError]
      def get_policies(prefix: nil)
        params = prefix ? { prefix: prefix } : {}

        rpc_call("get_policies", params) do |result|
          # The Unix server returns a bare JSON array (extract_policies also
          # accepts a {"policies" => [...]} hash defensively).
          policies = extract_policies(result).map { |p| parse_lifecycle_policy(p) }
          { policies: policies, success: true }
        end
      end

      # Apply all lifecycle policies.
      #
      # @return [Hash] Response with :success, :policies_count, :objects_processed, :message
      #
      # @raise [ServerError, ConnectionError]
      def apply_policies
        rpc_call("apply_policies", {}) do |result|
          {
            success: result.fetch("success", true),
            policies_count: result["policies_count"] || 0,
            objects_processed: result["objects_processed"] || 0,
            message: result["message"]
          }
        end
      end

      # Add a replication policy.
      #
      # @param policy [Models::ReplicationPolicy, Hash] Policy to add
      #
      # @return [Hash] Response with :success and :message keys
      #
      # @raise [ValidationError, ServerError, ConnectionError]
      def add_replication_policy(policy)
        policy_obj = policy.is_a?(Models::ReplicationPolicy) ? policy : Models::ReplicationPolicy.new(policy)
        params = {
          id: policy_obj.id,
          source_prefix: policy_obj.source_prefix || "",
          destination_type: policy_obj.destination_backend || "",
          destination: policy_obj.destination_settings || {},
          schedule: "",
          enabled: policy_obj.enabled || false
        }

        rpc_call("add_replication_policy", params) do |result|
          { success: result.fetch("success", true), message: result["message"] }
        end
      end

      # Remove a replication policy.
      #
      # @param id [String] Policy ID
      #
      # @return [Hash] Response with :success and :message keys
      #
      # @raise [NotFoundError, ServerError, ConnectionError]
      def remove_replication_policy(id)
        rpc_call("remove_replication_policy", { id: id }) do |result|
          { success: result.fetch("success", true), message: result["message"] }
        end
      end

      # Get all replication policies.
      #
      # @return [Hash] Response with :policies array
      #
      # @raise [ServerError, ConnectionError]
      def get_replication_policies
        rpc_call("get_replication_policies", {}) do |result|
          # The Unix server returns a bare JSON array (extract_policies also
          # accepts a {"policies" => [...]} hash defensively).
          policies = extract_policies(result).map { |p| parse_replication_policy(p) }
          { policies: policies }
        end
      end

      # Get a specific replication policy.
      #
      # @param id [String] Policy ID
      #
      # @return [Hash] Response with :policy key
      #
      # @raise [NotFoundError, ServerError, ConnectionError]
      def get_replication_policy(id)
        rpc_call("get_replication_policy", { id: id }) do |result|
          { policy: parse_replication_policy(result) }
        end
      end

      # Trigger replication synchronization.
      #
      # @param policy_id [String, nil] Specific policy to trigger (nil for all)
      # @param parallel [Boolean] Ignored; included for API parity
      # @param worker_count [Integer] Ignored; included for API parity
      #
      # @return [Hash] Response with :success, :result, :message keys
      #
      # @raise [ServerError, ConnectionError]
      def trigger_replication(policy_id: nil, parallel: false, worker_count: 4)
        params = policy_id ? { id: policy_id } : {}

        rpc_call("trigger_replication", params) do |result|
          {
            success: result.fetch("success", true),
            result: {
              objects_synced: result["objects_synced"] || 0,
              objects_failed: result["objects_failed"] || 0,
              bytes_transferred: result["bytes_transferred"] || 0,
              errors: result["errors"] || []
            },
            message: result["message"]
          }
        end
      end

      # Get replication status for a policy.
      #
      # @param id [String] Policy ID
      #
      # @return [Hash] Response with :success and :status keys
      #
      # @raise [NotFoundError, ServerError, ConnectionError]
      def get_replication_status(id)
        rpc_call("get_replication_status", { id: id }) do |result|
          {
            success: result.fetch("success", true),
            status: Models::ReplicationStatus.new(
              policy_id: result["policy_id"],
              enabled: result.fetch("enabled", false),
              total_objects_synced: result["objects_synced"] || 0,
              total_objects_deleted: 0,
              total_bytes_synced: 0,
              total_errors: result["objects_failed"] || 0,
              sync_count: 0
            )
          }
        end
      end

      # Close the persistent socket connection.
      #
      # The next request transparently reconnects. Safe to call multiple times
      # and before any connection has been established.
      #
      # @return [void]
      def close
        @conn_mutex.synchronize { drop_connection }
      end

      private

      # Send a JSON-RPC 2.0 request over the persistent connection and yield
      # the result hash to the block.
      #
      # The connection is established lazily and reused across requests
      # (serialized by a mutex). The server closes idle connections after
      # ~30s; on any IO error or response-id mismatch the connection is
      # dropped so the next call reconnects.
      #
      # @param method [String] JSON-RPC method name
      # @param params [Hash] Method parameters
      #
      # @yieldparam result [Hash] The result from a successful response
      #
      # @return [Object] Whatever the block returns
      #
      # @raise [NotFoundError, ValidationError, ServerError, ConnectionError, TimeoutError]
      def rpc_call(method, params)
        id = next_id
        request = JSON.generate(
          jsonrpc: "2.0",
          method: method,
          params: params,
          id: id
        )

        response = @conn_mutex.synchronize { exchange(request, id) }

        if (err = response["error"])
          raise_rpc_error(err)
        end

        yield response.fetch("result", {})
      rescue Errno::ENOENT, Errno::ECONNREFUSED => e
        raise ObjectStore::ConnectionError, "Cannot connect to Unix socket #{@socket_path}: #{e.message}"
      rescue Errno::ETIMEDOUT, IO::TimeoutError => e
        raise ObjectStore::TimeoutError, e.message
      rescue JSON::ParserError => e
        # Malformed JSON from the server is a protocol violation, not a
        # connection problem.
        raise ObjectStore::ProtocolError, "Invalid JSON from Unix server: #{e.message}"
      rescue ObjectStore::Error
        # Typed objstore errors (NotFound, Validation, Authorization, Server,
        # Connection, Timeout, Protocol) pass through unchanged.
        raise
      rescue StandardError => e
        raise ObjectStore::ConnectionError, e.message
      end

      # Write one request and read one response on the shared connection,
      # validating that the response id matches the request id. Drops the
      # connection on any failure so the next call reconnects.
      def exchange(request, id)
        sock = connection
        begin
          write_with_timeout(sock, request + "\n")
          raw = read_line_with_timeout(sock)
          response = JSON.parse(raw)
        rescue StandardError
          drop_connection
          raise
        end

        unless response["id"] == id
          drop_connection
          raise ObjectStore::ProtocolError,
                "response id #{response['id'].inspect} does not match request id #{id}"
        end

        response
      end

      # The lazily-established persistent connection. Must be called while
      # holding @conn_mutex.
      def connection
        @sock ||= UNIXSocket.new(@socket_path)
      end

      # Close and discard the held connection. Must be called while holding
      # @conn_mutex (or from #close, which acquires it).
      def drop_connection
        return unless @sock

        begin
          @sock.close
        rescue IOError
          # Already closed; nothing to release.
        end
        @sock = nil
      end

      # Write data to the socket, enforcing @timeout per write via IO.select.
      # SO_SNDTIMEO struct packing is platform-dependent; IO.select is portable.
      def write_with_timeout(sock, data)
        until data.empty?
          unless IO.select(nil, [sock], nil, @timeout)
            raise ObjectStore::TimeoutError, "write to #{@socket_path} timed out after #{@timeout}s"
          end

          begin
            written = sock.write_nonblock(data)
            data = data.byteslice(written..)
          rescue IO::WaitWritable
            next
          end
        end
      end

      # Read one newline-terminated line, enforcing @timeout per read via
      # IO.select. Raises TimeoutError when the server stops responding.
      def read_line_with_timeout(sock)
        buffer = +""
        loop do
          unless IO.select([sock], nil, nil, @timeout)
            raise ObjectStore::TimeoutError, "read from #{@socket_path} timed out after #{@timeout}s"
          end

          begin
            chunk = sock.read_nonblock(65_536)
          rescue IO::WaitReadable
            next
          rescue EOFError
            raise ObjectStore::ConnectionError, "connection closed before a full response was received"
          end

          buffer << chunk
          if (idx = buffer.index("\n"))
            return buffer[0...idx]
          end
        end
      end

      def parse_metadata_result(meta)
        return Models::Metadata.new unless meta

        Models::Metadata.new(
          content_type: meta["content_type"],
          content_encoding: meta["content_encoding"],
          custom: meta["custom"] || {}
        )
      end
    end
  end
end
