module ObjectStore
  module Models
    class Metadata
      attr_accessor :content_type, :content_encoding, :size, :last_modified, :etag, :custom

      def initialize(attrs = {})
        @content_type = attrs[:content_type] || attrs["content_type"]
        @content_encoding = attrs[:content_encoding] || attrs["content_encoding"]
        @size = attrs[:size] || attrs["size"]
        @last_modified = parse_time(attrs[:last_modified] || attrs["last_modified"])
        @etag = attrs[:etag] || attrs["etag"]
        @custom = attrs[:custom] || attrs["custom"] || {}
      end

      def to_h
        {
          content_type: @content_type,
          content_encoding: @content_encoding,
          size: @size,
          last_modified: @last_modified&.iso8601,
          etag: @etag,
          custom: @custom
        }.compact
      end

      def to_json(*args)
        to_h.to_json(*args)
      end

      private

      def parse_time(value)
        return nil if value.nil?
        return value if value.is_a?(Time)

        Time.parse(value.to_s)
      rescue ArgumentError
        nil
      end
    end

    class ObjectInfo
      attr_accessor :key, :metadata

      def initialize(attrs = {})
        @key = attrs[:key] || attrs["key"]
        metadata_attrs = attrs[:metadata] || attrs["metadata"] || {}
        @metadata = metadata_attrs.is_a?(Metadata) ? metadata_attrs : Metadata.new(metadata_attrs)
      end

      def to_h
        {
          key: @key,
          metadata: @metadata.to_h
        }
      end

      def to_json(*args)
        to_h.to_json(*args)
      end
    end

    class PutRequest
      attr_accessor :key, :data, :metadata

      def initialize(key, data, metadata = nil)
        @key = key
        @data = data
        @metadata = metadata.is_a?(Metadata) ? metadata : Metadata.new(metadata || {})
      end

      def to_h
        {
          key: @key,
          data: @data,
          metadata: @metadata.to_h
        }
      end
    end

    class PutResponse
      attr_accessor :success, :message, :etag

      def initialize(attrs = {})
        @success = attrs[:success] || attrs["success"]
        @message = attrs[:message] || attrs["message"]
        @etag = attrs[:etag] || attrs["etag"]
      end

      def success?
        @success == true
      end
    end

    class GetResponse
      attr_accessor :data, :metadata

      def initialize(data, metadata = nil)
        @data = data
        @metadata = metadata.is_a?(Metadata) ? metadata : Metadata.new(metadata || {})
      end
    end

    class DeleteResponse
      attr_accessor :success, :message

      def initialize(attrs = {})
        @success = attrs[:success] || attrs["success"]
        @message = attrs[:message] || attrs["message"]
      end

      def success?
        @success == true
      end
    end

    class ListResponse
      attr_accessor :objects, :common_prefixes, :next_token, :truncated

      def initialize(attrs = {})
        objects = attrs[:objects] || attrs["objects"] || []
        @objects = objects.map { |obj| obj.is_a?(ObjectInfo) ? obj : ObjectInfo.new(obj) }
        @common_prefixes = attrs[:common_prefixes] || attrs["common_prefixes"] || []
        @next_token = attrs[:next_token] || attrs["next_token"]
        @truncated = attrs[:truncated] || attrs["truncated"] || false
      end
    end

    class ExistsResponse
      attr_accessor :exists

      def initialize(attrs = {})
        @exists = attrs[:exists] || attrs["exists"] || false
      end

      def exists?
        @exists == true
      end
    end

    class MetadataResponse
      attr_accessor :metadata, :success, :message

      def initialize(attrs = {})
        metadata_attrs = attrs[:metadata] || attrs["metadata"] || {}
        @metadata = metadata_attrs.is_a?(Metadata) ? metadata_attrs : Metadata.new(metadata_attrs)
        @success = attrs[:success] || attrs["success"]
        @message = attrs[:message] || attrs["message"]
      end

      def success?
        @success == true
      end
    end

    class UpdateMetadataResponse
      attr_accessor :success, :message

      def initialize(attrs = {})
        @success = attrs[:success] || attrs["success"]
        @message = attrs[:message] || attrs["message"]
      end

      def success?
        @success == true
      end
    end

    class HealthResponse
      attr_accessor :status, :message

      def initialize(attrs = {})
        @status = attrs[:status] || attrs["status"]
        @message = attrs[:message] || attrs["message"]
      end

      def healthy?
        @status == "SERVING" || @status == "healthy"
      end
    end

    class ArchiveResponse
      attr_accessor :success, :message

      def initialize(attrs = {})
        @success = attrs[:success] || attrs["success"]
        @message = attrs[:message] || attrs["message"]
      end

      def success?
        @success == true
      end
    end

    class LifecyclePolicy
      attr_accessor :id, :prefix, :retention_seconds, :action, :destination_type, :destination_settings

      def initialize(attrs = {})
        @id = attrs[:id] || attrs["id"]
        @prefix = attrs[:prefix] || attrs["prefix"]
        @retention_seconds = attrs[:retention_seconds] || attrs["retention_seconds"]
        @action = attrs[:action] || attrs["action"]
        @destination_type = attrs[:destination_type] || attrs["destination_type"]
        @destination_settings = attrs[:destination_settings] || attrs["destination_settings"] || {}
      end

      def to_h
        {
          id: @id,
          prefix: @prefix,
          retention_seconds: @retention_seconds,
          action: @action,
          destination_type: @destination_type,
          destination_settings: @destination_settings
        }.compact
      end

      def to_json(*args)
        to_h.to_json(*args)
      end
    end

    class ReplicationPolicy
      attr_accessor :id, :source_backend, :source_settings, :source_prefix,
                    :destination_backend, :destination_settings, :check_interval_seconds,
                    :last_sync_time, :enabled, :encryption, :replication_mode

      def initialize(attrs = {})
        @id = attrs[:id] || attrs["id"]
        @source_backend = attrs[:source_backend] || attrs["source_backend"]
        @source_settings = attrs[:source_settings] || attrs["source_settings"] || {}
        @source_prefix = attrs[:source_prefix] || attrs["source_prefix"]
        @destination_backend = attrs[:destination_backend] || attrs["destination_backend"]
        @destination_settings = attrs[:destination_settings] || attrs["destination_settings"] || {}
        @check_interval_seconds = attrs[:check_interval_seconds] || attrs["check_interval_seconds"] || 3600
        @last_sync_time = parse_time(attrs[:last_sync_time] || attrs["last_sync_time"])
        @enabled = attrs[:enabled] || attrs["enabled"] || false
        @encryption = attrs[:encryption] || attrs["encryption"]
        @replication_mode = attrs[:replication_mode] || attrs["replication_mode"] || "transparent"
      end

      def to_h
        {
          id: @id,
          source_backend: @source_backend,
          source_settings: @source_settings,
          source_prefix: @source_prefix,
          destination_backend: @destination_backend,
          destination_settings: @destination_settings,
          check_interval_seconds: @check_interval_seconds,
          last_sync_time: @last_sync_time&.iso8601,
          enabled: @enabled,
          encryption: @encryption,
          replication_mode: @replication_mode
        }.compact
      end

      def to_json(*args)
        to_h.to_json(*args)
      end

      private

      def parse_time(value)
        return nil if value.nil?
        return value if value.is_a?(Time)

        Time.parse(value.to_s)
      rescue ArgumentError
        nil
      end
    end

    class ReplicationStatus
      attr_accessor :policy_id, :source_backend, :destination_backend, :enabled,
                    :total_objects_synced, :total_objects_deleted, :total_bytes_synced,
                    :total_errors, :last_sync_time, :average_sync_duration_ms, :sync_count

      def initialize(attrs = {})
        @policy_id = attrs[:policy_id] || attrs["policy_id"]
        @source_backend = attrs[:source_backend] || attrs["source_backend"]
        @destination_backend = attrs[:destination_backend] || attrs["destination_backend"]
        @enabled = attrs[:enabled] || attrs["enabled"] || false
        @total_objects_synced = attrs[:total_objects_synced] || attrs["total_objects_synced"] || 0
        @total_objects_deleted = attrs[:total_objects_deleted] || attrs["total_objects_deleted"] || 0
        @total_bytes_synced = attrs[:total_bytes_synced] || attrs["total_bytes_synced"] || 0
        @total_errors = attrs[:total_errors] || attrs["total_errors"] || 0
        @last_sync_time = parse_time(attrs[:last_sync_time] || attrs["last_sync_time"])
        @average_sync_duration_ms = attrs[:average_sync_duration_ms] || attrs["average_sync_duration_ms"] || 0
        @sync_count = attrs[:sync_count] || attrs["sync_count"] || 0
      end

      private

      def parse_time(value)
        return nil if value.nil?
        return value if value.is_a?(Time)

        Time.parse(value.to_s)
      rescue ArgumentError
        nil
      end
    end
  end
end
