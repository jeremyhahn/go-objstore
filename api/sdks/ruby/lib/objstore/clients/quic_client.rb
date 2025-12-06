require "net/http"
require "json"
require "openssl"

module ObjectStore
  module Clients
    # QUIC/HTTP3 client implementation for ObjectStore
    # Note: Ruby doesn't have native HTTP/3 support yet, so this uses HTTP/2 or HTTP/1.1
    # with Net::HTTP as a fallback implementation
    #
    # @api private
    class QuicClient
      attr_reader :host, :port, :use_ssl, :timeout

      # Initialize a new QUIC client
      #
      # @param host [String] Server hostname
      # @param port [Integer] Server port (default: 4433 for QUIC)
      # @param use_ssl [Boolean] Whether to use TLS
      # @param timeout [Integer] Request timeout in seconds
      def initialize(host: "localhost", port: 4433, use_ssl: true, timeout: 30)
        @host = host
        @port = port
        @use_ssl = use_ssl
        @timeout = timeout
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

        uri = build_uri("/objects/#{encode_key(key)}")
        request = Net::HTTP::Put.new(uri)
        request.body = data
        request["Content-Type"] = metadata_obj.content_type || "application/octet-stream"

        if metadata_obj.content_encoding
          request["Content-Encoding"] = metadata_obj.content_encoding
        end

        # Build metadata JSON including content_type and custom fields
        metadata_hash = {}
        metadata_hash["content_type"] = metadata_obj.content_type if metadata_obj.content_type
        metadata_hash["content_encoding"] = metadata_obj.content_encoding if metadata_obj.content_encoding
        metadata_hash.merge!(metadata_obj.custom) if metadata_obj.custom

        if metadata_hash.any?
          request["X-Object-Metadata"] = metadata_hash.to_json
        end

        response = execute_request(uri, request)

        Models::PutResponse.new(
          success: true,
          message: parse_body(response)["message"],
          etag: response["etag"]
        )
      end

      # Upload an object from an IO stream
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

        uri = build_uri("/objects/#{encode_key(key)}")
        request = Net::HTTP::Put.new(uri)
        request["Content-Type"] = metadata_obj.content_type || "application/octet-stream"
        request["Transfer-Encoding"] = "chunked"

        if metadata_obj.content_encoding
          request["Content-Encoding"] = metadata_obj.content_encoding
        end

        # Build metadata JSON
        metadata_hash = {}
        metadata_hash["content_type"] = metadata_obj.content_type if metadata_obj.content_type
        metadata_hash["content_encoding"] = metadata_obj.content_encoding if metadata_obj.content_encoding
        metadata_hash.merge!(metadata_obj.custom) if metadata_obj.custom

        if metadata_hash.any?
          request["X-Object-Metadata"] = metadata_hash.to_json
        end

        # Set request body to the IO stream
        request.body_stream = io

        response = execute_request(uri, request)

        Models::PutResponse.new(
          success: true,
          message: parse_body(response)["message"],
          etag: response["etag"]
        )
      end

      # Retrieve an object from the store
      #
      # @param key [String] Object key
      #
      # @return [Models::GetResponse] Response with data and metadata
      #
      # @raise [NotFoundError, ServerError]
      def get(key)
        uri = build_uri("/objects/#{encode_key(key)}")
        request = Net::HTTP::Get.new(uri)

        response = execute_request(uri, request)

        metadata = Models::Metadata.new(
          content_type: response["content-type"],
          size: response["content-length"]&.to_i,
          etag: response["etag"],
          last_modified: response["last-modified"]
        )

        Models::GetResponse.new(response.body, metadata)
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
        uri = build_uri("/objects/#{encode_key(key)}")
        request = Net::HTTP::Get.new(uri)

        metadata = nil

        Net::HTTP.start(uri.hostname, uri.port,
                        use_ssl: @use_ssl,
                        read_timeout: @timeout,
                        write_timeout: @timeout,
                        verify_mode: OpenSSL::SSL::VERIFY_NONE) do |http|
          http.request(request) do |response|
            handle_response(response)

            # Extract metadata from headers
            metadata = Models::Metadata.new(
              content_type: response["content-type"],
              size: response["content-length"]&.to_i,
              etag: response["etag"],
              last_modified: response["last-modified"]
            )

            # Stream the response body in chunks
            response.read_body do |chunk|
              yield chunk if block_given?
            end
          end
        end

        metadata
      rescue Net::OpenTimeout, Net::ReadTimeout => e
        raise ObjectStore::TimeoutError, e.message
      rescue ObjectStore::NotFoundError, ObjectStore::ValidationError, ObjectStore::ServerError
        raise
      rescue StandardError => e
        raise ObjectStore::ConnectionError, e.message
      end

      # Delete an object from the store
      #
      # @param key [String] Object key
      #
      # @return [Models::DeleteResponse] Response with success status
      #
      # @raise [NotFoundError, ServerError]
      def delete(key)
        uri = build_uri("/objects/#{encode_key(key)}")
        request = Net::HTTP::Delete.new(uri)

        response = execute_request(uri, request)

        Models::DeleteResponse.new(success: true, message: parse_body(response)["message"])
      end

      def list(prefix: nil, delimiter: nil, max_results: 100, continue_from: nil)
        params = {}
        params[:prefix] = prefix if prefix
        params[:delimiter] = delimiter if delimiter
        params[:limit] = max_results if max_results
        params[:token] = continue_from if continue_from

        uri = build_uri("/objects", params)
        request = Net::HTTP::Get.new(uri)

        response = execute_request(uri, request)
        body = parse_body(response)

        Models::ListResponse.new(body)
      end

      def exists?(key)
        uri = build_uri("/objects/#{encode_key(key)}")
        request = Net::HTTP::Head.new(uri)

        response = execute_request(uri, request)
        response.code == "200"
      rescue ObjectStore::NotFoundError
        # If we get a 404 for a HEAD request, object doesn't exist
        false
      end

      def get_metadata(key)
        uri = build_uri("/metadata/#{encode_key(key)}")
        request = Net::HTTP::Get.new(uri)

        response = execute_request(uri, request)
        body = parse_body(response)

        # Server returns: { "metadata": {...}, "size": 123, "etag": "...", "content_type": "..." }
        # Extract fields from both the nested metadata object and top level
        metadata_obj = body["metadata"] || {}

        # Try to get content_type from multiple places: top-level, metadata object
        # Don't use response headers as they reflect the API response, not the stored object
        content_type = body["content_type"] || metadata_obj["content_type"]
        content_encoding = body["content_encoding"] || metadata_obj["content_encoding"]

        # Collect custom metadata fields (excluding standard fields)
        standard_fields = ["content_type", "content_encoding", "size", "etag", "last_modified"]
        custom = metadata_obj.reject { |k, _| standard_fields.include?(k) }

        metadata_data = {
          content_type: content_type,
          content_encoding: content_encoding,
          size: body["size"],
          etag: body["etag"],
          last_modified: body["last_modified"],
          custom: custom
        }

        Models::MetadataResponse.new(
          metadata: metadata_data,
          success: true
        )
      end

      def update_metadata(key, metadata)
        metadata_obj = metadata.is_a?(Models::Metadata) ? metadata : Models::Metadata.new(metadata)

        uri = build_uri("/metadata/#{encode_key(key)}")
        request = Net::HTTP::Put.new(uri)
        request["Content-Type"] = "application/json"
        request.body = metadata_obj.to_json

        response = execute_request(uri, request)

        Models::UpdateMetadataResponse.new(success: true, message: parse_body(response)["message"])
      end

      def health(service: nil)
        params = service ? { service: service } : {}
        uri = build_uri("/health", params)
        request = Net::HTTP::Get.new(uri)

        response = execute_request(uri, request)
        body = parse_body(response)

        Models::HealthResponse.new(body)
      end

      def archive(key, destination_type:, destination_settings: {})
        payload = {
          key: key,
          destination_type: destination_type,
          destination_settings: destination_settings
        }

        uri = build_uri("/archive")
        request = Net::HTTP::Post.new(uri)
        request["Content-Type"] = "application/json"
        request.body = payload.to_json

        response = execute_request(uri, request)

        Models::ArchiveResponse.new(success: true, message: parse_body(response)["message"])
      end

      def add_policy(policy)
        policy_obj = policy.is_a?(Models::LifecyclePolicy) ? policy : Models::LifecyclePolicy.new(policy)

        uri = build_uri("/policies")
        request = Net::HTTP::Post.new(uri)
        request["Content-Type"] = "application/json"
        request.body = policy_obj.to_json

        response = execute_request(uri, request)

        { success: true, message: parse_body(response)["message"] }
      end

      def remove_policy(id)
        uri = build_uri("/policies/#{id}")
        request = Net::HTTP::Delete.new(uri)

        response = execute_request(uri, request)

        { success: true, message: parse_body(response)["message"] }
      end

      def get_policies(prefix: nil)
        params = prefix ? { prefix: prefix } : {}
        uri = build_uri("/policies", params)
        request = Net::HTTP::Get.new(uri)

        response = execute_request(uri, request)
        body = parse_body(response)

        policies = (body["policies"] || []).map { |p| Models::LifecyclePolicy.new(p) }
        { policies: policies, success: true }
      end

      def apply_policies
        uri = build_uri("/policies/apply")
        request = Net::HTTP::Post.new(uri)

        response = execute_request(uri, request)
        body = parse_body(response)

        {
          success: true,
          policies_count: body["policies_count"] || 0,
          objects_processed: body["objects_processed"] || 0,
          message: body["message"]
        }
      end

      def add_replication_policy(policy)
        policy_obj = policy.is_a?(Models::ReplicationPolicy) ? policy : Models::ReplicationPolicy.new(policy)

        uri = build_uri("/replication/policies")
        request = Net::HTTP::Post.new(uri)
        request["Content-Type"] = "application/json"
        request.body = policy_obj.to_json

        response = execute_request(uri, request)

        { success: true, message: parse_body(response)["message"] }
      end

      def remove_replication_policy(id)
        uri = build_uri("/replication/policies/#{id}")
        request = Net::HTTP::Delete.new(uri)

        response = execute_request(uri, request)

        { success: true, message: parse_body(response)["message"] }
      end

      def get_replication_policies
        uri = build_uri("/replication/policies")
        request = Net::HTTP::Get.new(uri)

        response = execute_request(uri, request)
        body = parse_body(response)

        policies = (body["policies"] || []).map { |p| Models::ReplicationPolicy.new(p) }
        { policies: policies }
      end

      def get_replication_policy(id)
        uri = build_uri("/replication/policies/#{id}")
        request = Net::HTTP::Get.new(uri)

        response = execute_request(uri, request)
        body = parse_body(response)

        { policy: Models::ReplicationPolicy.new(body) }
      end

      def trigger_replication(policy_id: nil, parallel: false, worker_count: 4)
        payload = {
          policy_id: policy_id,
          parallel: parallel,
          worker_count: worker_count
        }.compact

        uri = build_uri("/replication/trigger")
        request = Net::HTTP::Post.new(uri)
        request["Content-Type"] = "application/json"
        request.body = payload.to_json

        response = execute_request(uri, request)
        body = parse_body(response)

        {
          success: true,
          result: body["result"],
          message: body["message"]
        }
      end

      def get_replication_status(id)
        uri = build_uri("/replication/policies/#{id}/status")
        request = Net::HTTP::Get.new(uri)

        response = execute_request(uri, request)
        body = parse_body(response)

        {
          success: true,
          status: Models::ReplicationStatus.new(body["status"])
        }
      end

      private

      def build_uri(path, params = {})
        scheme = @use_ssl ? "https" : "http"
        uri = URI("#{scheme}://#{@host}:#{@port}#{path}")

        unless params.empty?
          uri.query = URI.encode_www_form(params)
        end

        uri
      end

      def encode_key(key)
        URI.encode_www_form_component(key)
      end

      def execute_request(uri, request)
        Net::HTTP.start(uri.hostname, uri.port,
                        use_ssl: @use_ssl,
                        read_timeout: @timeout,
                        write_timeout: @timeout,
                        verify_mode: OpenSSL::SSL::VERIFY_NONE) do |http|
          response = http.request(request)
          handle_response(response)
        end
      rescue Net::OpenTimeout, Net::ReadTimeout => e
        raise ObjectStore::TimeoutError, e.message
      rescue ObjectStore::NotFoundError, ObjectStore::ValidationError, ObjectStore::ServerError
        # Re-raise our custom errors as-is
        raise
      rescue StandardError => e
        raise ObjectStore::ConnectionError, e.message
      end

      def handle_response(response)
        case response.code
        when "200", "201"
          response
        when "404"
          raise ObjectStore::NotFoundError, "Resource not found"
        when "400"
          body = parse_body(response)
          raise ObjectStore::ValidationError, body["message"] || "Bad request"
        when "413"
          raise ObjectStore::ValidationError, "Request entity too large"
        when /^5/
          body = parse_body(response)
          raise ObjectStore::ServerError, body["message"] || "Server error"
        else
          raise ObjectStore::Error, "Unexpected response: #{response.code}"
        end
      end

      def parse_body(response)
        return {} if response.body.nil? || response.body.empty?
        return response.body unless response["content-type"]&.include?("application/json")

        JSON.parse(response.body)
      rescue JSON::ParserError
        {}
      end
    end
  end
end
