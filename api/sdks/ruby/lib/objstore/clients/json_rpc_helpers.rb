# frozen_string_literal: true

module ObjectStore
  module Clients
    # Shared helpers for the JSON-RPC 2.0 based protocol clients (MCP, Unix).
    #
    # Provides request id generation, error-code-to-exception mapping, policy
    # result parsing, and metadata serialization so the transports cannot
    # drift apart.
    #
    # Including classes must initialize +@id_counter+ (Integer) and
    # +@id_mutex+ (Mutex) in their constructors.
    #
    # @api private
    module JsonRpcHelpers
      private

      def next_id
        @id_mutex.synchronize { @id_counter += 1 }
      end

      # Map a JSON-RPC error object to a typed ObjectStore exception.
      #
      # Classification is by error code, never by message substring. The
      # server's application-defined codes (see pkg/server/jsonrpc):
      #   -32001  Forbidden
      #   -32002  Unauthenticated
      #   -32004  NotFound
      #   -32005  AlreadyExists
      #   -32029  RateLimited
      #   -32602  InvalidParams
      #
      # HTTP-style codes (404, 401, 403, 409, 429, 400) are accepted for
      # backward compatibility with older servers. Codes without a dedicated
      # exception class (internal errors) raise ServerError. The
      # server-provided message is preserved in the raised error.
      def raise_rpc_error(err)
        code = err["code"].to_i
        message = err["message"] || "RPC error"

        case code
        when -32_004, 404
          raise ObjectStore::NotFoundError, message
        when -32_002, 401
          raise ObjectStore::AuthenticationError, message
        when -32_001, 403
          raise ObjectStore::AuthorizationError, message
        when -32_005, 409
          raise ObjectStore::AlreadyExistsError, message
        when -32_029, 429
          raise ObjectStore::RateLimitError, message
        when -32_602, 400
          raise ObjectStore::ValidationError, message
        when -32_700, -32_600
          raise ObjectStore::ProtocolError, message
        else
          raise ObjectStore::ServerError, message
        end
      end

      # Extract the policy array from a get_policies/get_replication_policies
      # result. The Unix server returns a bare JSON array; other transports
      # wrap it in a {"policies" => [...]} hash. Accept both.
      def extract_policies(result)
        return result["policies"] || [] if result.is_a?(Hash)

        Array(result)
      end

      # Parse a lifecycle policy hash from the wire into a model. Prefers the
      # exact retention_seconds field, falling back to after_days for older
      # servers that only report whole days.
      def parse_lifecycle_policy(data)
        retention = data["retention_seconds"] || (data["after_days"] || 0) * 86_400
        Models::LifecyclePolicy.new(
          id: data["id"],
          prefix: data["prefix"],
          retention_seconds: retention,
          action: data["action"]
        )
      end

      # Parse a replication policy hash from the wire into a model. Accepts
      # both the HTTP field names (destination_backend/destination_settings)
      # and the Unix wire names (destination_type/destination).
      def parse_replication_policy(data)
        Models::ReplicationPolicy.new(
          id: data["id"],
          source_backend: data["source_backend"],
          source_settings: data["source_settings"] || {},
          source_prefix: data["source_prefix"],
          destination_backend: data["destination_backend"] || data["destination_type"],
          destination_settings: data["destination_settings"] || data["destination"] || {},
          check_interval_seconds: data["check_interval"],
          enabled: data.fetch("enabled", false),
          replication_mode: data["replication_mode"]
        )
      end

      # Build a metadata hash with symbol keys for JSON-RPC params.
      def build_metadata_params(metadata_obj)
        params = {}
        params[:content_type] = metadata_obj.content_type if metadata_obj.content_type
        params[:content_encoding] = metadata_obj.content_encoding if metadata_obj.content_encoding
        params[:custom] = metadata_obj.custom if metadata_obj.custom&.any?
        params
      end

      # Build a metadata hash with string keys for MCP tool arguments.
      def build_metadata_args(metadata_obj)
        build_metadata_params(metadata_obj).transform_keys(&:to_s)
      end

      def metadata_obj_empty?(metadata_obj)
        metadata_obj.content_type.nil? &&
          metadata_obj.content_encoding.nil? &&
          (metadata_obj.custom.nil? || metadata_obj.custom.empty?)
      end
    end
  end
end
