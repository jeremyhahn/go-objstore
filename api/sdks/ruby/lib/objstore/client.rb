# ObjectStore client that supports multiple protocols (REST, gRPC, QUIC/HTTP3)
#
# @example Basic usage with REST
#   client = ObjectStore::Client.new(protocol: :rest, host: "localhost", port: 8080)
#   client.put("myfile.txt", "Hello World")
#   data = client.get("myfile.txt")
#
# @example Using gRPC protocol
#   client = ObjectStore::Client.new(protocol: :grpc, host: "localhost", port: 50051)
#   client.put("myfile.txt", "Hello World")
#
# @example Streaming large files
#   File.open("large_file.bin", "rb") do |file|
#     client.put_stream("large_file.bin", file)
#   end
#
#   File.open("output.bin", "wb") do |file|
#     client.get_stream("large_file.bin") { |chunk| file.write(chunk) }
#   end
module ObjectStore
  class Client
    attr_reader :protocol, :client

    VALID_PROTOCOLS = %i[rest grpc quic].freeze

    # Initialize a new ObjectStore client
    #
    # @param protocol [Symbol] The protocol to use (:rest, :grpc, or :quic)
    # @param host [String] The server hostname
    # @param port [Integer, nil] The server port (defaults based on protocol if nil)
    # @param use_ssl [Boolean] Whether to use SSL/TLS
    # @param timeout [Integer] Request timeout in seconds
    #
    # @raise [ArgumentError] if protocol is not one of VALID_PROTOCOLS
    #
    # @example
    #   client = ObjectStore::Client.new(protocol: :rest, host: "localhost", port: 8080)
    def initialize(protocol: :rest, host: "localhost", port: nil, use_ssl: false, timeout: 30)
      @protocol = protocol.to_sym

      unless VALID_PROTOCOLS.include?(@protocol)
        raise ArgumentError, "Invalid protocol: #{protocol}. Must be one of: #{VALID_PROTOCOLS.join(', ')}"
      end

      @port = port || default_port_for_protocol(@protocol)
      @client = create_client(host, @port, use_ssl, timeout)
    end

    # Object operations

    # Upload an object to the store
    #
    # @param key [String] The object key (must be non-empty)
    # @param data [String] The object data
    # @param metadata [Models::Metadata, Hash, nil] Optional metadata
    #
    # @return [Models::PutResponse] The response containing success status and etag
    #
    # @raise [ArgumentError] if key is empty or data is nil
    # @raise [ValidationError] if server rejects the request
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   response = client.put("file.txt", "Hello World")
    #   puts response.etag
    #
    # @example With metadata
    #   metadata = ObjectStore::Models::Metadata.new(content_type: "text/plain")
    #   client.put("file.txt", "Hello World", metadata)
    def put(key, data, metadata = nil)
      validate_key!(key)
      validate_data!(data)
      @client.put(key, data, metadata)
    end

    # Upload an object from an IO stream
    #
    # @param key [String] The object key (must be non-empty)
    # @param io [IO, #read] An IO object or any object responding to :read
    # @param metadata [Models::Metadata, Hash, nil] Optional metadata
    # @param chunk_size [Integer] Size of chunks to read (default: 8KB)
    #
    # @return [Models::PutResponse] The response containing success status and etag
    #
    # @raise [ArgumentError] if key is empty or io doesn't respond to :read
    # @raise [ValidationError] if server rejects the request
    # @raise [ServerError] if server encounters an error
    #
    # @example Upload from file
    #   File.open("large_file.bin", "rb") do |file|
    #     client.put_stream("large_file.bin", file)
    #   end
    #
    # @example Upload from StringIO
    #   require 'stringio'
    #   io = StringIO.new("Hello World")
    #   client.put_stream("file.txt", io)
    def put_stream(key, io, metadata: nil, chunk_size: 8192)
      validate_key!(key)
      validate_io!(io)
      @client.put_stream(key, io, metadata: metadata, chunk_size: chunk_size)
    end

    # Retrieve an object from the store
    #
    # @param key [String] The object key (must be non-empty)
    #
    # @return [Models::GetResponse] The response containing data and metadata
    #
    # @raise [ArgumentError] if key is empty
    # @raise [NotFoundError] if object doesn't exist
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   response = client.get("file.txt")
    #   puts response.data
    #   puts response.metadata.content_type
    def get(key)
      validate_key!(key)
      @client.get(key)
    end

    # Retrieve an object in chunks via streaming
    #
    # @param key [String] The object key (must be non-empty)
    #
    # @yieldparam chunk [String] A chunk of the object data
    #
    # @return [Models::Metadata] The object metadata
    #
    # @raise [ArgumentError] if key is empty or no block given
    # @raise [NotFoundError] if object doesn't exist
    # @raise [ServerError] if server encounters an error
    #
    # @example Download to file
    #   File.open("output.bin", "wb") do |file|
    #     client.get_stream("large_file.bin") { |chunk| file.write(chunk) }
    #   end
    #
    # @example Process chunks
    #   total_size = 0
    #   client.get_stream("file.txt") do |chunk|
    #     total_size += chunk.bytesize
    #     puts "Received #{chunk.bytesize} bytes"
    #   end
    def get_stream(key, &block)
      validate_key!(key)
      raise ArgumentError, "Block required for streaming" unless block_given?
      @client.get_stream(key, &block)
    end

    # Delete an object from the store
    #
    # @param key [String] The object key (must be non-empty)
    #
    # @return [Models::DeleteResponse] The response containing success status
    #
    # @raise [ArgumentError] if key is empty
    # @raise [NotFoundError] if object doesn't exist
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   client.delete("file.txt")
    def delete(key)
      validate_key!(key)
      @client.delete(key)
    end

    # List objects in the store
    #
    # @param prefix [String, nil] Filter objects by key prefix
    # @param delimiter [String, nil] Delimiter for hierarchical listing
    # @param max_results [Integer] Maximum number of results to return
    # @param continue_from [String, nil] Pagination token from previous response
    #
    # @return [Models::ListResponse] The response containing objects and pagination info
    #
    # @raise [ServerError] if server encounters an error
    #
    # @example List all objects
    #   response = client.list
    #   response.objects.each { |obj| puts obj.key }
    #
    # @example List with prefix
    #   response = client.list(prefix: "docs/")
    #
    # @example Paginated listing
    #   response = client.list(max_results: 100)
    #   next_response = client.list(continue_from: response.next_token) if response.truncated
    def list(prefix: nil, delimiter: nil, max_results: 100, continue_from: nil)
      @client.list(
        prefix: prefix,
        delimiter: delimiter,
        max_results: max_results,
        continue_from: continue_from
      )
    end

    # Check if an object exists
    #
    # @param key [String] The object key (must be non-empty)
    #
    # @return [Boolean] true if object exists, false otherwise
    #
    # @raise [ArgumentError] if key is empty
    #
    # @example
    #   if client.exists?("file.txt")
    #     puts "File exists"
    #   end
    def exists?(key)
      validate_key!(key)
      @client.exists?(key)
    end

    # Metadata operations

    # Get metadata for an object
    #
    # @param key [String] The object key (must be non-empty)
    #
    # @return [Models::MetadataResponse] The response containing metadata
    #
    # @raise [ArgumentError] if key is empty
    # @raise [NotFoundError] if object doesn't exist
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   response = client.get_metadata("file.txt")
    #   puts response.metadata.content_type
    #   puts response.metadata.size
    def get_metadata(key)
      validate_key!(key)
      @client.get_metadata(key)
    end

    # Update metadata for an object
    #
    # @param key [String] The object key (must be non-empty)
    # @param metadata [Models::Metadata, Hash] The new metadata
    #
    # @return [Models::UpdateMetadataResponse] The response containing success status
    #
    # @raise [ArgumentError] if key is empty
    # @raise [NotFoundError] if object doesn't exist
    # @raise [ValidationError] if metadata is invalid
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   metadata = ObjectStore::Models::Metadata.new(content_type: "application/json")
    #   client.update_metadata("file.txt", metadata)
    def update_metadata(key, metadata)
      validate_key!(key)
      @client.update_metadata(key, metadata)
    end

    # Health check

    # Check server health status
    #
    # @param service [String, nil] Optional specific service to check
    #
    # @return [Models::HealthResponse] The response containing health status
    #
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   response = client.health
    #   puts "Server is healthy" if response.healthy?
    #
    # @example Check specific service
    #   response = client.health(service: "storage")
    def health(service: nil)
      @client.health(service: service)
    end

    # Archive operations

    # Archive an object to long-term storage
    #
    # @param key [String] The object key (must be non-empty)
    # @param destination_type [String] The archive destination type (e.g., "glacier")
    # @param destination_settings [Hash] Settings for the archive destination
    #
    # @return [Models::ArchiveResponse] The response containing success status
    #
    # @raise [ArgumentError] if key is empty
    # @raise [NotFoundError] if object doesn't exist
    # @raise [ValidationError] if destination settings are invalid
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   client.archive("old_file.txt", destination_type: "glacier")
    def archive(key, destination_type:, destination_settings: {})
      validate_key!(key)
      @client.archive(
        key,
        destination_type: destination_type,
        destination_settings: destination_settings
      )
    end

    # Lifecycle policy operations

    # Add a lifecycle policy
    #
    # @param policy [Models::LifecyclePolicy, Hash] The policy to add
    #
    # @return [Hash] Response with :success and :message keys
    #
    # @raise [ValidationError] if policy is invalid
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   policy = ObjectStore::Models::LifecyclePolicy.new(
    #     id: "delete-old-logs",
    #     prefix: "logs/",
    #     retention_seconds: 2592000,  # 30 days
    #     action: "delete"
    #   )
    #   client.add_policy(policy)
    def add_policy(policy)
      @client.add_policy(policy)
    end

    # Remove a lifecycle policy
    #
    # @param id [String] The policy ID (must be non-empty)
    #
    # @return [Hash] Response with :success and :message keys
    #
    # @raise [ArgumentError] if id is empty
    # @raise [NotFoundError] if policy doesn't exist
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   client.remove_policy("delete-old-logs")
    def remove_policy(id)
      validate_policy_id!(id)
      @client.remove_policy(id)
    end

    # Get all lifecycle policies
    #
    # @param prefix [String, nil] Filter policies by prefix
    #
    # @return [Hash] Response with :policies array and :success keys
    #
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   response = client.get_policies
    #   response[:policies].each { |policy| puts policy.id }
    #
    # @example Filter by prefix
    #   response = client.get_policies(prefix: "logs/")
    def get_policies(prefix: nil)
      @client.get_policies(prefix: prefix)
    end

    # Apply all lifecycle policies
    #
    # @return [Hash] Response with :success, :policies_count, :objects_processed, and :message keys
    #
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   result = client.apply_policies
    #   puts "Processed #{result[:objects_processed]} objects"
    def apply_policies
      @client.apply_policies
    end

    # Replication policy operations

    # Add a replication policy
    #
    # @param policy [Models::ReplicationPolicy, Hash] The replication policy to add
    #
    # @return [Hash] Response with :success and :message keys
    #
    # @raise [ValidationError] if policy is invalid
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   policy = ObjectStore::Models::ReplicationPolicy.new(
    #     id: "backup-to-s3",
    #     source_backend: "local",
    #     destination_backend: "s3",
    #     destination_settings: { bucket: "my-backup-bucket" },
    #     enabled: true
    #   )
    #   client.add_replication_policy(policy)
    def add_replication_policy(policy)
      @client.add_replication_policy(policy)
    end

    # Remove a replication policy
    #
    # @param id [String] The policy ID (must be non-empty)
    #
    # @return [Hash] Response with :success and :message keys
    #
    # @raise [ArgumentError] if id is empty
    # @raise [NotFoundError] if policy doesn't exist
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   client.remove_replication_policy("backup-to-s3")
    def remove_replication_policy(id)
      validate_policy_id!(id)
      @client.remove_replication_policy(id)
    end

    # Get all replication policies
    #
    # @return [Hash] Response with :policies array
    #
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   response = client.get_replication_policies
    #   response[:policies].each { |policy| puts policy.id }
    def get_replication_policies
      @client.get_replication_policies
    end

    # Get a specific replication policy
    #
    # @param id [String] The policy ID (must be non-empty)
    #
    # @return [Hash] Response with :policy key
    #
    # @raise [ArgumentError] if id is empty
    # @raise [NotFoundError] if policy doesn't exist
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   response = client.get_replication_policy("backup-to-s3")
    #   puts response[:policy].enabled
    def get_replication_policy(id)
      validate_policy_id!(id)
      @client.get_replication_policy(id)
    end

    # Trigger replication synchronization
    #
    # @param policy_id [String, nil] Specific policy ID to trigger (nil for all policies)
    # @param parallel [Boolean] Whether to run replication in parallel
    # @param worker_count [Integer] Number of parallel workers to use
    #
    # @return [Hash] Response with :success, :result, and :message keys
    #
    # @raise [ServerError] if server encounters an error
    #
    # @example Trigger all policies
    #   result = client.trigger_replication
    #
    # @example Trigger specific policy in parallel
    #   result = client.trigger_replication(
    #     policy_id: "backup-to-s3",
    #     parallel: true,
    #     worker_count: 8
    #   )
    def trigger_replication(policy_id: nil, parallel: false, worker_count: 4)
      @client.trigger_replication(
        policy_id: policy_id,
        parallel: parallel,
        worker_count: worker_count
      )
    end

    # Get replication status for a policy
    #
    # @param id [String] The policy ID (must be non-empty)
    #
    # @return [Hash] Response with :success and :status keys
    #
    # @raise [ArgumentError] if id is empty
    # @raise [NotFoundError] if policy doesn't exist
    # @raise [ServerError] if server encounters an error
    #
    # @example
    #   response = client.get_replication_status("backup-to-s3")
    #   status = response[:status]
    #   puts "Synced #{status.total_objects_synced} objects"
    def get_replication_status(id)
      validate_policy_id!(id)
      @client.get_replication_status(id)
    end

    # Switch protocol at runtime
    #
    # @param protocol [Symbol] The new protocol to use (:rest, :grpc, or :quic)
    # @param port [Integer, nil] Optional port override
    #
    # @return [Symbol] The new protocol
    #
    # @raise [ArgumentError] if protocol is not one of VALID_PROTOCOLS
    #
    # @example
    #   client.switch_protocol(:grpc)
    #   client.switch_protocol(:rest, port: 8081)
    def switch_protocol(protocol, port: nil)
      @protocol = protocol.to_sym

      unless VALID_PROTOCOLS.include?(@protocol)
        raise ArgumentError, "Invalid protocol: #{protocol}. Must be one of: #{VALID_PROTOCOLS.join(', ')}"
      end

      new_port = port || default_port_for_protocol(@protocol)
      @client = create_client(@client.host, new_port, @client.use_ssl, @client.timeout)
      @protocol
    end

    private

    def create_client(host, port, use_ssl, timeout)
      case @protocol
      when :rest
        Clients::RestClient.new(host: host, port: port, use_ssl: use_ssl, timeout: timeout)
      when :grpc
        Clients::GrpcClient.new(host: host, port: port, use_ssl: use_ssl, timeout: timeout)
      when :quic
        Clients::QuicClient.new(host: host, port: port, use_ssl: use_ssl, timeout: timeout)
      end
    end

    def default_port_for_protocol(protocol)
      case protocol
      when :rest
        8080
      when :grpc
        50051
      when :quic
        4433
      end
    end

    # Validation helper methods

    def validate_key!(key)
      raise ArgumentError, "Key must be a non-empty string" if key.nil? || key.to_s.strip.empty?
    end

    def validate_data!(data)
      raise ArgumentError, "Data cannot be nil" if data.nil?
    end

    def validate_io!(io)
      raise ArgumentError, "IO object must respond to :read" unless io.respond_to?(:read)
    end

    def validate_policy_id!(id)
      raise ArgumentError, "Policy ID must be a non-empty string" if id.nil? || id.to_s.strip.empty?
    end
  end
end
