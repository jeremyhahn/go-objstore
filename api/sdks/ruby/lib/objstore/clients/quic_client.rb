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
      # @param token [String, nil] Bearer token for Authorization header
      # @param headers [Hash] Additional HTTP headers to send on every request
      # @param tenant_id [String, nil] Tenant identifier sent as X-Tenant-ID header
      def initialize(host: "localhost", port: 4433, use_ssl: true, timeout: 30,
                     token: nil, headers: {}, tenant_id: nil, verify_ssl: true)
        @host = host
        @port = port
        @use_ssl = use_ssl
        @timeout = timeout
        @token = token
        @extra_headers = headers || {}
        @tenant_id = tenant_id
        # TLS certificates are verified by default; disable only for testing
        # against self-signed certificates.
        @verify_mode = verify_ssl ? OpenSSL::SSL::VERIFY_PEER : OpenSSL::SSL::VERIFY_NONE
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

        # Custom metadata is sent as individual X-Meta-<key> headers (the QUIC
        # server reads Content-Type/Content-Encoding plus X-Meta-* headers).
        if metadata_obj.custom
          metadata_obj.custom.each do |k, v|
            request["X-Meta-#{k}"] = v.to_s
          end
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

        # Custom metadata is sent as individual X-Meta-<key> headers.
        if metadata_obj.custom
          metadata_obj.custom.each do |k, v|
            request["X-Meta-#{k}"] = v.to_s
          end
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

        metadata = metadata_from_headers(response, size: response.body&.bytesize)

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
        apply_auth_headers(request)

        Net::HTTP.start(uri.hostname, uri.port,
                        use_ssl: @use_ssl,
                        read_timeout: @timeout,
                        write_timeout: @timeout,
                        verify_mode: @verify_mode) do |http|
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
      rescue ObjectStore::Error
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

        # The server returns 204 No Content; parse_body yields {} for the
        # empty body, so default the message.
        body = parse_body(response)
        message = body.is_a?(Hash) ? body["message"] : nil
        Models::DeleteResponse.new(success: true, message: message || "Object deleted successfully")
      end

      def list(prefix: nil, delimiter: nil, max_results: 100, continue_from: nil)
        params = {}
        params[:prefix] = prefix if prefix
        params[:delimiter] = delimiter if delimiter
        params[:max] = max_results if max_results
        params[:continue] = continue_from if continue_from

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
        # The QUIC server has no /metadata route; metadata is read via a
        # HEAD on the object and parsed from response headers.
        uri = build_uri("/objects/#{encode_key(key)}")
        request = Net::HTTP::Head.new(uri)

        response = execute_request(uri, request)

        Models::MetadataResponse.new(
          metadata: metadata_from_headers(response),
          success: true
        )
      end

      def update_metadata(key, metadata)
        metadata_obj = metadata.is_a?(Models::Metadata) ? metadata : Models::Metadata.new(metadata)

        # The QUIC server updates metadata via PATCH on the object with a
        # JSON body of { content_type, content_encoding, custom }.
        payload = {
          content_type: metadata_obj.content_type,
          content_encoding: metadata_obj.content_encoding,
          custom: metadata_obj.custom
        }.compact

        uri = build_uri("/objects/#{encode_key(key)}")
        request = Net::HTTP::Patch.new(uri)
        request["Content-Type"] = "application/json"
        request.body = payload.to_json

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

        # The QUIC server expects `check_interval` (seconds), not
        # `check_interval_seconds` as the REST server uses.
        payload = policy_obj.to_h
        if payload.key?(:check_interval_seconds)
          payload[:check_interval] = payload.delete(:check_interval_seconds)
        end

        uri = build_uri("/replication/policies")
        request = Net::HTTP::Post.new(uri)
        request["Content-Type"] = "application/json"
        request.body = payload.to_json

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
        # The QUIC server takes policy_id as a query param (empty = sync all),
        # not a JSON body. parallel/worker_count are not supported over QUIC.
        params = {}
        params[:policy_id] = policy_id if policy_id

        uri = build_uri("/replication/trigger", params)
        request = Net::HTTP::Post.new(uri)

        response = execute_request(uri, request)
        body = parse_body(response)

        {
          success: true,
          result: body["result"]&.transform_keys(&:to_sym),
          message: body["message"]
        }
      end

      def get_replication_status(id)
        uri = build_uri("/replication/status/#{id}")
        request = Net::HTTP::Get.new(uri)

        response = execute_request(uri, request)
        body = parse_body(response)

        # The server returns status fields flat at the top level (not nested
        # under a "status" key).
        {
          success: true,
          status: Models::ReplicationStatus.new(body)
        }
      end

      # Close the underlying HTTP connection if one is held
      #
      # This client opens a short-lived connection per request via
      # Net::HTTP.start, so there is no persistent connection to release.
      # Provided for API parity; closes a persistent connection if one is
      # ever held. Safe to call multiple times.
      #
      # @return [void]
      def close
        return unless defined?(@http) && @http

        @http.finish if @http.respond_to?(:finish) && @http.respond_to?(:started?) && @http.started?
        @http = nil
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

      # Build a Models::Metadata from the standard and X-Meta-<key> response
      # headers. The QUIC server conveys content_type/content_encoding via
      # standard headers and custom metadata as individual X-Meta-* headers.
      #
      # @param response [Net::HTTPResponse] the HTTP response to read headers from
      # @param size [Integer, nil] explicit size override (e.g. body bytesize for
      #   GET, where Content-Length may be absent); falls back to Content-Length
      #
      # @return [Models::Metadata]
      def metadata_from_headers(response, size: nil)
        custom = {}
        response.each_header do |name, value|
          next unless name =~ /\Ax-meta-(.+)\z/i

          custom[Regexp.last_match(1)] = value
        end

        Models::Metadata.new(
          content_type: response["content-type"],
          content_encoding: response["content-encoding"],
          size: size || response["content-length"]&.to_i,
          etag: response["etag"],
          last_modified: response["last-modified"],
          custom: custom
        )
      end

      def apply_auth_headers(request)
        request["Authorization"] = "Bearer #{@token}" if @token
        request["X-Tenant-ID"] = @tenant_id if @tenant_id
        @extra_headers.each { |k, v| request[k.to_s] = v.to_s }
      end

      def execute_request(uri, request)
        apply_auth_headers(request)
        Net::HTTP.start(uri.hostname, uri.port,
                        use_ssl: @use_ssl,
                        read_timeout: @timeout,
                        write_timeout: @timeout,
                        verify_mode: @verify_mode) do |http|
          response = http.request(request)
          handle_response(response)
        end
      rescue Net::OpenTimeout, Net::ReadTimeout => e
        raise ObjectStore::TimeoutError, e.message
      rescue ObjectStore::Error
        # Re-raise our custom errors as-is
        raise
      rescue StandardError => e
        raise ObjectStore::ConnectionError, e.message
      end

      def handle_response(response)
        case response.code
        when "200", "201", "204"
          response
        when "404"
          raise ObjectStore::NotFoundError, "Resource not found"
        when "400"
          raise ObjectStore::ValidationError, error_message(response, "Bad request")
        when "401"
          raise ObjectStore::AuthenticationError, error_message(response, "Unauthenticated")
        when "403"
          raise ObjectStore::AuthorizationError, error_message(response, "Forbidden")
        when "409"
          raise ObjectStore::AlreadyExistsError, error_message(response, "Already exists")
        when "413"
          raise ObjectStore::ValidationError, "Request entity too large"
        when "429"
          raise ObjectStore::RateLimitError, error_message(response, "Rate limited")
        when /^5/
          raise ObjectStore::ServerError, error_message(response, "Server error")
        else
          raise ObjectStore::Error, "Unexpected response: #{response.code}"
        end
      end

      # Extract the server-provided error message from a response body,
      # falling back to a generic default when the body is absent or not JSON.
      def error_message(response, fallback)
        body = parse_body(response)
        (body.is_a?(Hash) && body["message"]) || fallback
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
