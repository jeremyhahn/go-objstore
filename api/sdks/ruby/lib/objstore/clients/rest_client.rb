require "faraday"
require "faraday/multipart"
require "json"

module ObjectStore
  module Clients
    # REST client implementation for ObjectStore
    # Uses Faraday for HTTP communication with chunked transfer encoding support
    #
    # @api private
    class RestClient
      attr_reader :host, :port, :use_ssl, :timeout

      # Initialize a new REST client
      #
      # @param host [String] Server hostname
      # @param port [Integer] Server port
      # @param use_ssl [Boolean] Whether to use HTTPS
      # @param timeout [Integer] Request timeout in seconds
      def initialize(host: "localhost", port: 8080, use_ssl: false, timeout: 30)
        @host = host
        @port = port
        @use_ssl = use_ssl
        @timeout = timeout
        @connection = build_connection
      end

      # Upload an object to the store
      #
      # @param key [String] Object key
      # @param data [String] Object data
      # @param metadata [Models::Metadata, Hash, nil] Optional metadata
      #
      # @return [Models::PutResponse] Response with success status and etag
      #
      # @raise [ValidationError, ServerError]
      def put(key, data, metadata = nil)
        metadata_obj = metadata.is_a?(Models::Metadata) ? metadata : Models::Metadata.new(metadata || {})

        response = @connection.put("/objects/#{encode_key(key)}") do |req|
          req.headers["Content-Type"] = metadata_obj.content_type || "application/octet-stream"

          if metadata_obj.content_encoding
            req.headers["Content-Encoding"] = metadata_obj.content_encoding
          end

          # Build metadata JSON including content_type and custom fields
          metadata_hash = {}
          metadata_hash["content_type"] = metadata_obj.content_type if metadata_obj.content_type
          metadata_hash["content_encoding"] = metadata_obj.content_encoding if metadata_obj.content_encoding
          metadata_hash.merge!(metadata_obj.custom) if metadata_obj.custom

          if metadata_hash.any?
            req.headers["X-Object-Metadata"] = metadata_hash.to_json
          end

          req.body = data
        end

        handle_response(response) do |body|
          Models::PutResponse.new(
            success: true,
            message: body["message"],
            etag: response.headers["etag"]
          )
        end
      end

      # Upload an object from an IO stream using chunked transfer encoding
      #
      # @param key [String] Object key
      # @param io [IO, #read] IO object to read from
      # @param metadata [Models::Metadata, Hash, nil] Optional metadata
      # @param chunk_size [Integer] Size of chunks to read
      #
      # @return [Models::PutResponse] Response with success status and etag
      #
      # @raise [ValidationError, ServerError]
      def put_stream(key, io, metadata: nil, chunk_size: 8192)
        metadata_obj = metadata.is_a?(Models::Metadata) ? metadata : Models::Metadata.new(metadata || {})

        # Read all data from IO for Faraday (Faraday doesn't support true streaming uploads easily)
        # For true streaming, consider using Net::HTTP directly or async-http gem
        data = String.new
        while (chunk = io.read(chunk_size))
          data << chunk
        end

        # Use the regular put method with the collected data
        put(key, data, metadata_obj)
      end

      # Retrieve an object from the store
      #
      # @param key [String] Object key
      #
      # @return [Models::GetResponse] Response with data and metadata
      #
      # @raise [NotFoundError, ServerError]
      def get(key)
        response = @connection.get("/objects/#{encode_key(key)}")

        handle_response(response) do |body|
          metadata = Models::Metadata.new(
            content_type: response.headers["content-type"],
            size: response.headers["content-length"]&.to_i,
            etag: response.headers["etag"],
            last_modified: response.headers["last-modified"]
          )

          Models::GetResponse.new(body, metadata)
        end
      end

      # Retrieve an object in chunks via streaming
      #
      # @param key [String] Object key
      #
      # @yieldparam chunk [String] A chunk of data
      #
      # @return [Models::Metadata] Object metadata
      #
      # @raise [NotFoundError, ServerError]
      def get_stream(key, chunk_size: 8192)
        response = @connection.get("/objects/#{encode_key(key)}") do |req|
          # Request streaming response if supported
          req.options.on_data = proc do |chunk, _overall_received_bytes|
            yield chunk if block_given?
          end
        end

        handle_response(response) do |_body|
          # Return metadata from headers
          Models::Metadata.new(
            content_type: response.headers["content-type"],
            size: response.headers["content-length"]&.to_i,
            etag: response.headers["etag"],
            last_modified: response.headers["last-modified"]
          )
        end
      rescue StandardError
        # Fallback for Faraday versions that don't support streaming
        # Download entire response and yield in chunks
        response = @connection.get("/objects/#{encode_key(key)}")

        handle_response(response) do |body|
          # Yield the body in chunks
          offset = 0
          while offset < body.bytesize
            chunk = body.byteslice(offset, chunk_size)
            yield chunk if block_given?
            offset += chunk_size
          end

          # Return metadata
          Models::Metadata.new(
            content_type: response.headers["content-type"],
            size: response.headers["content-length"]&.to_i,
            etag: response.headers["etag"],
            last_modified: response.headers["last-modified"]
          )
        end
      end

      # Delete an object from the store
      #
      # @param key [String] Object key
      #
      # @return [Models::DeleteResponse] Response with success status
      #
      # @raise [NotFoundError, ServerError]
      def delete(key)
        response = @connection.delete("/objects/#{encode_key(key)}")

        handle_response(response) do |body|
          Models::DeleteResponse.new(success: true, message: body["message"])
        end
      end

      # List objects in the store
      #
      # @param prefix [String, nil] Filter by key prefix
      # @param delimiter [String, nil] Delimiter for hierarchical listing
      # @param max_results [Integer] Maximum results to return
      # @param continue_from [String, nil] Pagination token
      #
      # @return [Models::ListResponse] List response with objects
      #
      # @raise [ServerError]
      def list(prefix: nil, delimiter: nil, max_results: 100, continue_from: nil)
        params = {}
        params[:prefix] = prefix if prefix
        params[:delimiter] = delimiter if delimiter
        params[:limit] = max_results if max_results
        params[:token] = continue_from if continue_from

        response = @connection.get("/objects", params)

        handle_response(response) do |body|
          Models::ListResponse.new(body)
        end
      end

      # Check if an object exists
      #
      # @param key [String] Object key
      #
      # @return [Boolean] true if exists, false otherwise
      def exists?(key)
        response = @connection.head("/objects/#{encode_key(key)}")
        response.status == 200
      rescue ObjectStore::NotFoundError
        false
      end

      def get_metadata(key)
        response = @connection.get("/metadata/#{encode_key(key)}")

        handle_response(response) do |body|
          # Server returns: { "key": "...", "size": 123, "content_type": "text/plain",
          #                   "etag": "...", "modified": "...", "metadata": {...} }
          # Standard fields are at the top level, custom fields are in the metadata object
          metadata_obj = body["metadata"] || {}

          # Extract standard fields from top level
          content_type = body["content_type"]
          content_encoding = body["content_encoding"]

          # Collect custom metadata fields (the metadata object contains only custom fields)
          custom = metadata_obj.empty? ? {} : metadata_obj

          metadata_data = {
            content_type: content_type,
            content_encoding: content_encoding,
            size: body["size"],
            etag: body["etag"],
            last_modified: body["modified"] || body["last_modified"],
            custom: custom
          }

          Models::MetadataResponse.new(
            metadata: metadata_data,
            success: true
          )
        end
      end

      def update_metadata(key, metadata)
        metadata_obj = metadata.is_a?(Models::Metadata) ? metadata : Models::Metadata.new(metadata)

        response = @connection.put("/metadata/#{encode_key(key)}") do |req|
          req.headers["Content-Type"] = "application/json"
          req.body = metadata_obj.to_json
        end

        handle_response(response) do |body|
          Models::UpdateMetadataResponse.new(success: true, message: body["message"])
        end
      end

      def health(service: nil)
        params = service ? { service: service } : {}
        response = @connection.get("/health", params)

        handle_response(response) do |body|
          Models::HealthResponse.new(body)
        end
      end

      def archive(key, destination_type:, destination_settings: {})
        payload = {
          key: key,
          destination_type: destination_type,
          destination_settings: destination_settings
        }

        response = @connection.post("/archive") do |req|
          req.headers["Content-Type"] = "application/json"
          req.body = payload.to_json
        end

        handle_response(response) do |body|
          Models::ArchiveResponse.new(success: true, message: body["message"])
        end
      end

      def add_policy(policy)
        policy_obj = policy.is_a?(Models::LifecyclePolicy) ? policy : Models::LifecyclePolicy.new(policy)

        response = @connection.post("/policies") do |req|
          req.headers["Content-Type"] = "application/json"
          req.body = policy_obj.to_json
        end

        handle_response(response) do |body|
          { success: true, message: body["message"] }
        end
      end

      def remove_policy(id)
        response = @connection.delete("/policies/#{id}")

        handle_response(response) do |body|
          { success: true, message: body["message"] }
        end
      end

      def get_policies(prefix: nil)
        params = prefix ? { prefix: prefix } : {}
        response = @connection.get("/policies", params)

        handle_response(response) do |body|
          policies = (body["policies"] || []).map { |p| Models::LifecyclePolicy.new(p) }
          { policies: policies, success: true }
        end
      end

      def apply_policies
        response = @connection.post("/policies/apply")

        handle_response(response) do |body|
          {
            success: true,
            policies_count: body["policies_count"] || 0,
            objects_processed: body["objects_processed"] || 0,
            message: body["message"]
          }
        end
      end

      def add_replication_policy(policy)
        policy_obj = policy.is_a?(Models::ReplicationPolicy) ? policy : Models::ReplicationPolicy.new(policy)

        response = @connection.post("/replication/policies") do |req|
          req.headers["Content-Type"] = "application/json"
          req.body = policy_obj.to_json
        end

        handle_response(response) do |body|
          { success: true, message: body["message"] }
        end
      end

      def remove_replication_policy(id)
        response = @connection.delete("/replication/policies/#{id}")

        handle_response(response) do |body|
          { success: true, message: body["message"] }
        end
      end

      def get_replication_policies
        response = @connection.get("/replication/policies")

        handle_response(response) do |body|
          policies = (body["policies"] || []).map { |p| Models::ReplicationPolicy.new(p) }
          { policies: policies }
        end
      end

      def get_replication_policy(id)
        response = @connection.get("/replication/policies/#{id}")

        handle_response(response) do |body|
          { policy: Models::ReplicationPolicy.new(body) }
        end
      end

      def trigger_replication(policy_id: nil, parallel: false, worker_count: 4)
        payload = {
          policy_id: policy_id,
          parallel: parallel,
          worker_count: worker_count
        }.compact

        response = @connection.post("/replication/trigger") do |req|
          req.headers["Content-Type"] = "application/json"
          req.body = payload.to_json
        end

        handle_response(response) do |body|
          {
            success: true,
            result: body["result"],
            message: body["message"]
          }
        end
      end

      def get_replication_status(id)
        response = @connection.get("/replication/policies/#{id}/status")

        handle_response(response) do |body|
          {
            success: true,
            status: Models::ReplicationStatus.new(body["status"])
          }
        end
      end

      private

      def build_connection
        scheme = @use_ssl ? "https" : "http"
        url = "#{scheme}://#{@host}:#{@port}"

        Faraday.new(url: url) do |conn|
          conn.request :multipart
          conn.request :url_encoded
          conn.adapter Faraday.default_adapter
          conn.options.timeout = @timeout
          # Note: open_timeout removed for compatibility with different Faraday versions
        end
      end

      def encode_key(key)
        URI.encode_www_form_component(key)
      end

      def handle_response(response)
        case response.status
        when 200, 201
          body = parse_body(response)
          yield(body)
        when 404
          raise ObjectStore::NotFoundError, "Resource not found"
        when 400
          body = parse_body(response)
          raise ObjectStore::ValidationError, body["message"] || "Bad request"
        when 413
          raise ObjectStore::ValidationError, "Request entity too large"
        when 500..599
          body = parse_body(response)
          raise ObjectStore::ServerError, body["message"] || "Server error"
        else
          raise ObjectStore::Error, "Unexpected response: #{response.status}"
        end
      end

      def parse_body(response)
        return response.body unless response.headers["content-type"]&.include?("application/json")

        JSON.parse(response.body)
      rescue JSON::ParserError
        response.body
      end
    end
  end
end
