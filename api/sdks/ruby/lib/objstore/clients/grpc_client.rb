require "grpc"

module ObjectStore
  module Clients
    # gRPC client implementation for ObjectStore
    # Supports streaming for efficient large file transfers
    #
    # @api private
    class GrpcClient
      attr_reader :host, :port, :use_ssl, :timeout

      # Initialize a new gRPC client
      #
      # @param host [String] Server hostname
      # @param port [Integer] Server port
      # @param use_ssl [Boolean] Whether to use TLS
      # @param timeout [Integer] Request timeout in seconds
      def initialize(host: "localhost", port: 50051, use_ssl: false, timeout: 30)
        @host = host
        @port = port
        @use_ssl = use_ssl
        @timeout = timeout
        @stub = nil
      end

      # Upload an object to the store
      #
      # @param key [String] Object key
      # @param data [String] Object data
      # @param metadata [Models::Metadata, Hash, nil] Optional metadata
      #
      # @return [Models::PutResponse] Response with success status and etag
      #
      # @raise [ValidationError, ServerError, TimeoutError]
      def put(key, data, metadata = nil)
        ensure_stub

        metadata_obj = metadata.is_a?(Models::Metadata) ? metadata : Models::Metadata.new(metadata || {})

        request = Objstore::V1::PutRequest.new(
          key: key,
          data: data,
          metadata: build_metadata_proto(metadata_obj)
        )
        response = @stub.put(request, deadline: grpc_deadline)

        Models::PutResponse.new(
          success: response.success,
          message: response.message,
          etag: response.etag
        )
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
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
      # @raise [ValidationError, ServerError, TimeoutError]
      def put_stream(key, io, metadata: nil, chunk_size: 8192)
        metadata_obj = metadata.is_a?(Models::Metadata) ? metadata : Models::Metadata.new(metadata || {})

        # Read all data from IO (gRPC streaming would require server-side streaming support)
        data = String.new
        while (chunk = io.read(chunk_size))
          data << chunk
        end

        # Use regular put method
        put(key, data, metadata_obj)
      end

      # Retrieve an object from the store
      #
      # @param key [String] Object key
      #
      # @return [Models::GetResponse] Response with data and metadata
      #
      # @raise [NotFoundError, ServerError, TimeoutError]
      def get(key)
        ensure_stub

        request = Objstore::V1::GetRequest.new(key: key)
        chunks = []
        metadata = nil

        @stub.get(request, deadline: grpc_deadline).each do |response|
          # Only append data if it's not empty
          chunks << response.data if response.data && !response.data.empty?
          # Parse metadata from the first response that has it
          metadata ||= parse_metadata(response.metadata) if response.metadata
        end

        # Join chunks to get the full data
        data = chunks.join
        Models::GetResponse.new(data, metadata || Models::Metadata.new)
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Retrieve an object in chunks via streaming
      #
      # @param key [String] Object key
      #
      # @yieldparam chunk [String] A chunk of data
      #
      # @return [Models::Metadata] Object metadata
      #
      # @raise [NotFoundError, ServerError, TimeoutError]
      def get_stream(key)
        ensure_stub

        request = Objstore::V1::GetRequest.new(key: key)
        metadata = nil

        # gRPC naturally supports streaming
        @stub.get(request, deadline: grpc_deadline).each do |response|
          # Yield data chunks as they arrive
          if response.data && !response.data.empty?
            yield response.data if block_given?
          end
          # Parse metadata from the first response that has it
          metadata ||= parse_metadata(response.metadata) if response.metadata
        end

        metadata || Models::Metadata.new
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Delete an object from the store
      #
      # @param key [String] Object key
      #
      # @return [Models::DeleteResponse] Response with success status
      #
      # @raise [NotFoundError, ServerError]
      def delete(key)
        ensure_stub

        request = Objstore::V1::DeleteRequest.new(key: key)
        response = @stub.delete(request, deadline: grpc_deadline)

        Models::DeleteResponse.new(
          success: response.success,
          message: response.message
        )
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # List objects in the store
      #
      # @param prefix [String, nil] Filter by key prefix
      # @param delimiter [String, nil] Delimiter for grouping
      # @param max_results [Integer] Maximum results to return
      # @param continue_from [String, nil] Continuation token
      #
      # @return [Models::ListResponse] Response with objects list
      def list(prefix: nil, delimiter: nil, max_results: 100, continue_from: nil)
        ensure_stub

        request = Objstore::V1::ListRequest.new(
          prefix: prefix || "",
          delimiter: delimiter || "",
          max_results: max_results || 100,
          continue_from: continue_from || ""
        )
        response = @stub.list(request, deadline: grpc_deadline)

        # Handle nil or empty objects array
        objects = (response.objects || []).map do |obj|
          Models::ObjectInfo.new(
            key: obj.key,
            metadata: parse_metadata(obj.metadata)
          )
        end

        Models::ListResponse.new(
          objects: objects,
          common_prefixes: (response.common_prefixes || []).to_a,
          next_token: response.next_token,
          truncated: response.truncated || false
        )
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Check if an object exists
      #
      # @param key [String] Object key
      #
      # @return [Boolean] True if object exists
      def exists?(key)
        ensure_stub

        request = Objstore::V1::ExistsRequest.new(key: key)
        response = @stub.exists(request, deadline: grpc_deadline)

        response.exists
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Get metadata for an object
      #
      # @param key [String] Object key
      #
      # @return [Models::MetadataResponse] Response with metadata
      def get_metadata(key)
        ensure_stub

        request = Objstore::V1::GetMetadataRequest.new(key: key)
        response = @stub.get_metadata(request, deadline: grpc_deadline)

        Models::MetadataResponse.new(
          metadata: parse_metadata(response.metadata),
          success: response.success,
          message: response.message
        )
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Update metadata for an object
      #
      # @param key [String] Object key
      # @param metadata [Models::Metadata, Hash] New metadata
      #
      # @return [Models::UpdateMetadataResponse] Response with success status
      def update_metadata(key, metadata)
        ensure_stub

        metadata_obj = metadata.is_a?(Models::Metadata) ? metadata : Models::Metadata.new(metadata)
        request = Objstore::V1::UpdateMetadataRequest.new(
          key: key,
          metadata: build_metadata_proto(metadata_obj)
        )
        response = @stub.update_metadata(request, deadline: grpc_deadline)

        Models::UpdateMetadataResponse.new(
          success: response.success,
          message: response.message
        )
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Check server health
      #
      # @param service [String, nil] Service name to check
      #
      # @return [Models::HealthResponse] Response with health status
      def health(service: nil)
        ensure_stub

        request = Objstore::V1::HealthRequest.new(service: service || "")
        response = @stub.health(request, deadline: grpc_deadline)

        # Handle protobuf enum status
        status_name = case response.status
                      when :SERVING, 1
                        "SERVING"
                      when :NOT_SERVING, 2
                        "NOT_SERVING"
                      else
                        "UNKNOWN"
                      end

        Models::HealthResponse.new(
          status: status_name,
          message: response.message
        )
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Archive an object to external storage
      #
      # @param key [String] Object key
      # @param destination_type [String] Archive destination type
      # @param destination_settings [Hash] Settings for the destination
      #
      # @return [Models::ArchiveResponse] Response with success status
      def archive(key, destination_type:, destination_settings: {})
        ensure_stub

        request = Objstore::V1::ArchiveRequest.new(
          key: key,
          destination_type: destination_type
        )
        destination_settings.each { |k, v| request.destination_settings[k.to_s] = v.to_s } if destination_settings
        response = @stub.archive(request, deadline: grpc_deadline)

        Models::ArchiveResponse.new(
          success: response.success,
          message: response.message
        )
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Add a lifecycle policy
      #
      # @param policy [Models::LifecyclePolicy, Hash] Policy to add
      #
      # @return [Hash] Response with success status
      def add_policy(policy)
        ensure_stub

        policy_obj = policy.is_a?(Models::LifecyclePolicy) ? policy : Models::LifecyclePolicy.new(policy)
        request = Objstore::V1::AddPolicyRequest.new(
          policy: build_lifecycle_policy_proto(policy_obj)
        )
        response = @stub.add_policy(request, deadline: grpc_deadline)

        { success: response.success, message: response.message }
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Remove a lifecycle policy
      #
      # @param id [String] Policy ID
      #
      # @return [Hash] Response with success status
      def remove_policy(id)
        ensure_stub

        request = Objstore::V1::RemovePolicyRequest.new(id: id)
        response = @stub.remove_policy(request, deadline: grpc_deadline)

        { success: response.success, message: response.message }
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Get all lifecycle policies
      #
      # @param prefix [String, nil] Filter by prefix
      #
      # @return [Hash] Response with policies list
      def get_policies(prefix: nil)
        ensure_stub

        request = Objstore::V1::GetPoliciesRequest.new(prefix: prefix || "")
        response = @stub.get_policies(request, deadline: grpc_deadline)

        policies = (response.policies || []).map { |p| parse_lifecycle_policy(p) }

        { policies: policies, success: response.success }
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Apply all lifecycle policies
      #
      # @return [Hash] Response with execution results
      def apply_policies
        ensure_stub

        request = Objstore::V1::ApplyPoliciesRequest.new
        response = @stub.apply_policies(request, deadline: grpc_deadline)

        {
          success: response.success,
          policies_count: response.policies_count,
          objects_processed: response.objects_processed,
          message: response.message
        }
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Add a replication policy
      #
      # @param policy [Models::ReplicationPolicy, Hash] Policy to add
      #
      # @return [Hash] Response with success status
      def add_replication_policy(policy)
        ensure_stub

        policy_obj = policy.is_a?(Models::ReplicationPolicy) ? policy : Models::ReplicationPolicy.new(policy)
        request = Objstore::V1::AddReplicationPolicyRequest.new(
          policy: build_replication_policy_proto(policy_obj)
        )
        response = @stub.add_replication_policy(request, deadline: grpc_deadline)

        { success: response.success, message: response.message }
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Remove a replication policy
      #
      # @param id [String] Policy ID
      #
      # @return [Hash] Response with success status
      def remove_replication_policy(id)
        ensure_stub

        request = Objstore::V1::RemoveReplicationPolicyRequest.new(id: id)
        response = @stub.remove_replication_policy(request, deadline: grpc_deadline)

        { success: response.success, message: response.message }
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Get all replication policies
      #
      # @return [Hash] Response with policies list
      def get_replication_policies
        ensure_stub

        request = Objstore::V1::GetReplicationPoliciesRequest.new
        response = @stub.get_replication_policies(request, deadline: grpc_deadline)

        policies = (response.policies || []).map { |p| parse_replication_policy(p) }

        { policies: policies }
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Get a specific replication policy
      #
      # @param id [String] Policy ID
      #
      # @return [Hash] Response with policy
      def get_replication_policy(id)
        ensure_stub

        request = Objstore::V1::GetReplicationPolicyRequest.new(id: id)
        response = @stub.get_replication_policy(request, deadline: grpc_deadline)

        { policy: parse_replication_policy(response.policy) }
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Trigger replication
      #
      # @param policy_id [String, nil] Specific policy to trigger
      # @param parallel [Boolean] Run in parallel
      # @param worker_count [Integer] Number of workers
      #
      # @return [Hash] Response with sync results
      def trigger_replication(policy_id: nil, parallel: false, worker_count: 4)
        ensure_stub

        request = Objstore::V1::TriggerReplicationRequest.new(
          policy_id: policy_id || "",
          parallel: parallel || false,
          worker_count: worker_count || 4
        )
        response = @stub.trigger_replication(request, deadline: grpc_deadline)

        {
          success: response.success,
          result: parse_sync_result(response.result),
          message: response.message
        }
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      # Get replication status
      #
      # @param id [String] Policy ID
      #
      # @return [Hash] Response with status
      def get_replication_status(id)
        ensure_stub

        request = Objstore::V1::GetReplicationStatusRequest.new(id: id)
        response = @stub.get_replication_status(request, deadline: grpc_deadline)

        {
          success: response.success,
          status: parse_replication_status(response.status),
          message: response.message
        }
      rescue GRPC::BadStatus => e
        handle_grpc_error(e)
      end

      private

      def ensure_stub
        return if @stub

        require_relative "../proto/objstore_services_pb"

        address = "#{@host}:#{@port}"
        credentials = @use_ssl ? GRPC::Core::ChannelCredentials.new : :this_channel_is_insecure
        @stub = Objstore::V1::ObjectStore::Stub.new(address, credentials)
      end

      def grpc_deadline
        Time.now + @timeout
      end

      def handle_grpc_error(error)
        case error.code
        when GRPC::Core::StatusCodes::NOT_FOUND
          raise ObjectStore::NotFoundError, error.details
        when GRPC::Core::StatusCodes::INVALID_ARGUMENT
          raise ObjectStore::ValidationError, error.details
        when GRPC::Core::StatusCodes::DEADLINE_EXCEEDED
          raise ObjectStore::TimeoutError, error.details
        when GRPC::Core::StatusCodes::UNIMPLEMENTED
          raise ObjectStore::ServerError, error.details
        else
          raise ObjectStore::Error, "gRPC error: #{error.details}"
        end
      end

      def build_metadata_proto(metadata)
        proto = Objstore::V1::Metadata.new
        proto.content_type = metadata.content_type if metadata.content_type
        proto.content_encoding = metadata.content_encoding if metadata.content_encoding
        proto.size = metadata.size.to_i if metadata.size
        proto.etag = metadata.etag if metadata.etag
        if metadata.custom && !metadata.custom.empty?
          metadata.custom.each do |k, v|
            proto.custom[k.to_s] = v.to_s
          end
        end
        proto
      end

      def build_lifecycle_policy_proto(policy)
        proto = Objstore::V1::LifecyclePolicy.new(
          id: policy.id || "",
          prefix: policy.prefix || "",
          retention_seconds: policy.retention_seconds || 0,
          action: policy.action || "",
          destination_type: policy.destination_type || ""
        )
        if policy.destination_settings && !policy.destination_settings.empty?
          policy.destination_settings.each { |k, v| proto.destination_settings[k.to_s] = v.to_s }
        end
        proto
      end

      def build_replication_policy_proto(policy)
        proto = Objstore::V1::ReplicationPolicy.new(
          id: policy.id || "",
          source_backend: policy.source_backend || "",
          source_prefix: policy.source_prefix || "",
          destination_backend: policy.destination_backend || "",
          check_interval_seconds: policy.check_interval_seconds || 0,
          enabled: policy.enabled || false
        )
        if policy.source_settings && !policy.source_settings.empty?
          policy.source_settings.each { |k, v| proto.source_settings[k.to_s] = v.to_s }
        end
        if policy.destination_settings && !policy.destination_settings.empty?
          policy.destination_settings.each { |k, v| proto.destination_settings[k.to_s] = v.to_s }
        end
        if policy.replication_mode
          mode = policy.replication_mode.to_s.upcase
          proto.replication_mode = mode == "OPAQUE" ? :OPAQUE : :TRANSPARENT
        end
        proto
      end

      def parse_metadata(proto)
        return Models::Metadata.new unless proto

        custom_fields = {}
        if proto.respond_to?(:custom) && proto.custom
          custom_fields = proto.custom.respond_to?(:to_h) ? proto.custom.to_h : {}
        end

        Models::Metadata.new(
          content_type: proto.respond_to?(:content_type) ? proto.content_type : nil,
          content_encoding: proto.respond_to?(:content_encoding) ? proto.content_encoding : nil,
          size: proto.respond_to?(:size) ? proto.size : nil,
          etag: proto.respond_to?(:etag) ? proto.etag : nil,
          custom: custom_fields
        )
      end

      def parse_lifecycle_policy(proto)
        Models::LifecyclePolicy.new(
          id: proto.id,
          prefix: proto.prefix,
          retention_seconds: proto.retention_seconds,
          action: proto.action,
          destination_type: proto.destination_type,
          destination_settings: proto.destination_settings&.to_h || {}
        )
      end

      def parse_replication_policy(proto)
        Models::ReplicationPolicy.new(
          id: proto.id,
          source_backend: proto.source_backend,
          source_settings: proto.source_settings&.to_h || {},
          source_prefix: proto.source_prefix,
          destination_backend: proto.destination_backend,
          destination_settings: proto.destination_settings&.to_h || {},
          check_interval_seconds: proto.check_interval_seconds,
          enabled: proto.enabled,
          replication_mode: proto.replication_mode
        )
      end

      def parse_sync_result(proto)
        return {} unless proto

        {
          policy_id: proto.policy_id,
          synced: proto.synced,
          deleted: proto.deleted,
          failed: proto.failed,
          bytes_total: proto.bytes_total,
          duration_ms: proto.duration_ms,
          errors: proto.errors&.to_a || []
        }
      end

      def parse_replication_status(proto)
        return Models::ReplicationStatus.new({}) unless proto

        Models::ReplicationStatus.new(
          policy_id: proto.policy_id,
          source_backend: proto.source_backend,
          destination_backend: proto.destination_backend,
          enabled: proto.enabled,
          total_objects_synced: proto.total_objects_synced,
          total_objects_deleted: proto.total_objects_deleted,
          total_bytes_synced: proto.total_bytes_synced,
          total_errors: proto.total_errors,
          average_sync_duration_ms: proto.average_sync_duration_ms,
          sync_count: proto.sync_count
        )
      end
    end
  end
end
