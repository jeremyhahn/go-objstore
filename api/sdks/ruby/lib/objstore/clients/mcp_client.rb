require "net/http"
require "json"
require "base64"
require "openssl"

module ObjectStore
  module Clients
    # MCP (Model Context Protocol) HTTP client for ObjectStore.
    #
    # Communicates via HTTP POST JSON-RPC 2.0 to the MCP server endpoint at "/".
    # Operations are invoked with method "tools/call" and tool names prefixed
    # with "objstore_". Text results are returned in result.content[0].text.
    #
    # Streaming (put_stream/get_stream) is supported by buffering the IO in
    # memory, which is appropriate for request/response HTTP transports.
    #
    # @api private
    class McpClient
      include JsonRpcHelpers

      attr_reader :host, :port, :use_ssl, :timeout

      # Initialize a new MCP client.
      #
      # @param host [String] Server hostname
      # @param port [Integer] Server port
      # @param use_ssl [Boolean] Whether to use HTTPS
      # @param timeout [Integer] Request timeout in seconds
      # @param token [String, nil] Bearer token for Authorization header
      # @param headers [Hash] Additional HTTP headers to send on every request
      # @param tenant_id [String, nil] Tenant identifier sent as X-Tenant-ID header
      def initialize(host: "localhost", port: 8081, use_ssl: false, timeout: 30,
                     token: nil, headers: {}, tenant_id: nil)
        @host = host
        @port = port
        @use_ssl = use_ssl
        @timeout = timeout
        @token = token
        @extra_headers = headers || {}
        @tenant_id = tenant_id
        @id_counter = 0
        @id_mutex = Mutex.new
        @http = build_http
        @http_mutex = Mutex.new
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
        # Object data travels base64-encoded so the transport is binary-safe.
        args = { "key" => key, "data" => Base64.strict_encode64(data.to_s) }
        args["metadata"] = build_metadata_args(metadata_obj) unless metadata_obj_empty?(metadata_obj)

        call_tool("objstore_put", args) do |result|
          Models::PutResponse.new(
            success: result.fetch("success", true),
            message: result["message"]
          )
        end
      end

      # Upload an object from an IO stream.
      #
      # Reads the entire IO into memory before sending (buffered).
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
        call_tool("objstore_get", { "key" => key }) do |result|
          Models::GetResponse.new(decode_object_data(result), Models::Metadata.new)
        end
      end

      # Retrieve an object and yield it as a single chunk.
      #
      # @param key [String] Object key
      #
      # @yieldparam chunk [String] The full object data as one chunk
      #
      # @return [Models::Metadata] Object metadata
      #
      # @raise [NotFoundError, ServerError, ConnectionError]
      def get_stream(key)
        call_tool("objstore_get", { "key" => key }) do |result|
          data = decode_object_data(result)
          yield data if block_given?
          Models::Metadata.new
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
        call_tool("objstore_delete", { "key" => key }) do |result|
          Models::DeleteResponse.new(success: result.fetch("success", true), message: result["message"])
        end
      end

      # List objects in the store.
      #
      # @param prefix [String, nil] Filter by key prefix
      # @param delimiter [String, nil] Ignored; MCP tool does not expose delimiter
      # @param max_results [Integer] Maximum results to return
      # @param continue_from [String, nil] Pagination token from previous response
      #
      # @return [Models::ListResponse] List response with objects
      #
      # @raise [ServerError, ConnectionError]
      def list(prefix: nil, delimiter: nil, max_results: 100, continue_from: nil)
        args = {}
        args["prefix"] = prefix if prefix
        args["max_results"] = max_results if max_results
        args["continue_from"] = continue_from if continue_from

        call_tool("objstore_list", args) do |result|
          keys = result["keys"] || []
          objects = keys.map { |k| Models::ObjectInfo.new(key: k) }

          Models::ListResponse.new(
            objects: objects,
            next_token: result["next_token"],
            truncated: result.fetch("truncated", false)
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
        call_tool("objstore_exists", { "key" => key }) do |result|
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
        call_tool("objstore_get_metadata", { "key" => key }) do |result|
          Models::MetadataResponse.new(
            metadata: Models::Metadata.new(
              content_type: result["content_type"],
              content_encoding: result["content_encoding"],
              size: result["size"],
              etag: result["etag"],
              last_modified: result["last_modified"],
              custom: result["custom"] || {}
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
        args = { "key" => key, "metadata" => build_metadata_args(metadata_obj) }

        call_tool("objstore_update_metadata", args) do |result|
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
        call_tool("objstore_health", {}) do |result|
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
        args = {
          "key" => key,
          "destination_type" => destination_type,
          "destination_settings" => destination_settings
        }

        call_tool("objstore_archive", args) do |result|
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
        args = {
          "id" => policy_obj.id,
          "prefix" => policy_obj.prefix || "",
          "retention_seconds" => policy_obj.retention_seconds || 0,
          "action" => policy_obj.action
        }
        args["destination_type"] = policy_obj.destination_type if policy_obj.destination_type
        args["destination_settings"] = policy_obj.destination_settings if policy_obj.destination_settings&.any?

        call_tool("objstore_add_policy", args) do |result|
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
        call_tool("objstore_remove_policy", { "id" => id }) do |result|
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
        args = prefix ? { "prefix" => prefix } : {}

        call_tool("objstore_get_policies", args) do |result|
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
        call_tool("objstore_apply_policies", {}) do |result|
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
        args = {
          "id" => policy_obj.id,
          "source_backend" => policy_obj.source_backend || "",
          "destination_backend" => policy_obj.destination_backend || "",
          "check_interval" => policy_obj.check_interval_seconds || 60,
          "enabled" => policy_obj.enabled || false
        }
        args["source_settings"] = policy_obj.source_settings if policy_obj.source_settings&.any?
        args["destination_settings"] = policy_obj.destination_settings if policy_obj.destination_settings&.any?
        args["source_prefix"] = policy_obj.source_prefix if policy_obj.source_prefix

        call_tool("objstore_add_replication_policy", args) do |result|
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
        call_tool("objstore_remove_replication_policy", { "id" => id }) do |result|
          { success: result.fetch("success", true), message: result["message"] }
        end
      end

      # Get all replication policies.
      #
      # @return [Hash] Response with :policies array
      #
      # @raise [ServerError, ConnectionError]
      def get_replication_policies
        call_tool("objstore_list_replication_policies", {}) do |result|
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
        call_tool("objstore_get_replication_policy", { "id" => id }) do |result|
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
        args = policy_id ? { "policy_id" => policy_id } : {}

        call_tool("objstore_trigger_replication", args) do |result|
          sync = result["result"] || {}
          {
            success: result.fetch("success", true),
            result: sync.transform_keys(&:to_sym),
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
        call_tool("objstore_get_replication_status", { "policy_id" => id }) do |result|
          {
            success: result.fetch("success", true),
            status: Models::ReplicationStatus.new(
              policy_id: result["policy_id"],
              source_backend: result["source_backend"],
              destination_backend: result["destination_backend"],
              enabled: result.fetch("enabled", false),
              total_objects_synced: result["total_objects_synced"] || 0,
              total_objects_deleted: result["total_objects_deleted"] || 0,
              total_bytes_synced: result["total_bytes_synced"] || 0,
              total_errors: result["total_errors"] || 0,
              average_sync_duration_ms: 0,
              sync_count: result["sync_count"] || 0
            )
          }
        end
      end

      # Close the persistent HTTP connection.
      #
      # The next request transparently reconnects. Safe to call multiple times
      # and before any connection has been established.
      #
      # @return [void]
      def close
        @http_mutex.synchronize { finish_http }
      end

      private

      # Build and send a JSON-RPC 2.0 "tools/call" request to the MCP endpoint,
      # parse the text result, and yield the parsed JSON hash to the block.
      #
      # @param tool_name [String] MCP tool name (e.g. "objstore_put")
      # @param arguments [Hash] Tool arguments
      #
      # @yieldparam result [Hash] Parsed JSON from result.content[0].text
      #
      # @return [Object] Whatever the block returns
      #
      # @raise [NotFoundError, ValidationError, ServerError, ConnectionError, TimeoutError]
      def call_tool(tool_name, arguments)
        payload = JSON.generate(
          jsonrpc: "2.0",
          method: "tools/call",
          params: { name: tool_name, arguments: arguments },
          id: next_id
        )

        response = http_post(payload)
        body = JSON.parse(response.body)

        if (err = body["error"])
          raise_rpc_error(err)
        end

        result_envelope = body.fetch("result", {})
        content = result_envelope.fetch("content", [])
        text = content.dig(0, "text") || "{}"

        parsed = begin
          JSON.parse(text)
        rescue JSON::ParserError
          { "data" => text }
        end

        yield parsed
      rescue ObjectStore::Error
        # Typed objstore errors (NotFound, Validation, Authentication,
        # Authorization, AlreadyExists, RateLimit, Server, Connection,
        # Timeout, Protocol) pass through unchanged.
        raise
      rescue Net::OpenTimeout, Net::ReadTimeout => e
        raise ObjectStore::TimeoutError, e.message
      rescue JSON::ParserError => e
        raise ObjectStore::ProtocolError, "Invalid JSON from MCP server: #{e.message}"
      rescue StandardError => e
        raise ObjectStore::ConnectionError, e.message
      end

      # Build the persistent Net::HTTP connection. It is started lazily on
      # first use and reused across requests, avoiding a fresh TCP (and TLS)
      # handshake per call.
      def build_http
        http = Net::HTTP.new(@host, @port)
        http.use_ssl = @use_ssl
        http.verify_mode = OpenSSL::SSL::VERIFY_PEER if @use_ssl
        http.open_timeout = @timeout
        http.read_timeout = @timeout
        http.write_timeout = @timeout
        http
      end

      # Execute an HTTP POST to the MCP endpoint ("/") over the persistent
      # connection. When the server has closed an idle connection, the
      # connection is re-established and the request retried once.
      def http_post(body)
        request = Net::HTTP::Post.new(build_uri("/"))
        request["Content-Type"] = "application/json"
        request["Accept"] = "application/json"
        apply_auth_headers(request)

        response = @http_mutex.synchronize do
          retried = false
          begin
            @http.start unless @http.started?
            @http.request(request, body)
          rescue EOFError, Errno::ECONNRESET, Errno::EPIPE, IOError
            finish_http
            raise if retried

            retried = true
            retry
          end
        end

        handle_http_response(response)
      end

      # Close the underlying connection if it is open. Must be called while
      # holding @http_mutex (or from #close, which acquires it).
      def finish_http
        @http.finish if @http.started?
      rescue IOError
        # Already closed; nothing to release.
      end

      # Attach Authorization, X-Tenant-ID, and any extra headers.
      def apply_auth_headers(request)
        request["Authorization"] = "Bearer #{@token}" if @token
        request["X-Tenant-ID"] = @tenant_id if @tenant_id
        @extra_headers.each { |k, v| request[k.to_s] = v.to_s }
      end

      def build_uri(path)
        scheme = @use_ssl ? "https" : "http"
        URI("#{scheme}://#{@host}:#{@port}#{path}")
      end

      def handle_http_response(response)
        case response.code
        when "200", "201"
          response
        when "401"
          raise ObjectStore::AuthenticationError, "Unauthenticated"
        when "403"
          raise ObjectStore::AuthorizationError, "Forbidden"
        when "404"
          raise ObjectStore::NotFoundError, "Resource not found"
        when "400"
          body = safe_parse_body(response)
          raise ObjectStore::ValidationError, body["message"] || "Bad request"
        when "409"
          body = safe_parse_body(response)
          raise ObjectStore::AlreadyExistsError, body["message"] || "Already exists"
        when "429"
          body = safe_parse_body(response)
          raise ObjectStore::RateLimitError, body["message"] || "Rate limited"
        when /^5/
          body = safe_parse_body(response)
          raise ObjectStore::ServerError, body["message"] || "Server error"
        else
          raise ObjectStore::Error, "Unexpected response: #{response.code}"
        end
      end

      def safe_parse_body(response)
        return {} if response.body.nil? || response.body.empty?

        JSON.parse(response.body)
      rescue JSON::ParserError
        {}
      end

      # Decode the base64 "data" field of a tool result. Object data is
      # base64-encoded on the MCP transport; anything else is a protocol
      # violation and raises rather than being passed through silently.
      def decode_object_data(result)
        raw = result.fetch("data", "")
        return "" if raw.empty?

        Base64.strict_decode64(raw)
      rescue ArgumentError
        raise ObjectStore::ProtocolError, "invalid base64 data in MCP response"
      end

    end
  end
end
