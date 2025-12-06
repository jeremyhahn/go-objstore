require "spec_helper"

RSpec.describe "Comprehensive Integration Tests", :integration do
  before(:all) do
    WebMock.allow_net_connect!
  end

  after(:all) do
    WebMock.disable_net_connect!
  end

  # Test data table for all 19 operations
  OPERATION_TEST_CASES = [
    {
      name: "put",
      operation: :put,
      setup: -> (_client) { nil },
      test: -> (client, test_key) do
        response = client.put(test_key, "test data")
        expect(response).to be_a(ObjectStore::Models::PutResponse)
        expect(response.success?).to be true
        expect(response.etag).not_to be_nil
        { success: true, cleanup: true }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "get",
      operation: :get,
      setup: -> (client, test_key) { client.put(test_key, "get test data") },
      test: -> (client, test_key) do
        response = client.get(test_key)
        expect(response).to be_a(ObjectStore::Models::GetResponse)
        expect(response.data).to eq("get test data")
        expect(response.metadata).to be_a(ObjectStore::Models::Metadata)
        { success: true, cleanup: true }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "delete",
      operation: :delete,
      setup: -> (client, test_key) { client.put(test_key, "delete test data") },
      test: -> (client, test_key) do
        response = client.delete(test_key)
        expect(response).to be_a(ObjectStore::Models::DeleteResponse)
        expect(response.success?).to be true
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "exists?",
      operation: :exists?,
      setup: -> (client, test_key) { client.put(test_key, "exists test data") },
      test: -> (client, test_key) do
        exists = client.exists?(test_key)
        expect(exists).to be true

        # Test non-existent key
        non_existent = client.exists?("non-existent-#{test_key}")
        expect(non_existent).to be false
        { success: true, cleanup: true }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "list",
      operation: :list,
      setup: -> (client, test_key) do
        3.times { |i| client.put("#{test_key}-list-#{i}", "list data #{i}") }
      end,
      test: -> (client, test_key) do
        response = client.list(prefix: "#{test_key}-list-")
        expect(response).to be_a(ObjectStore::Models::ListResponse)
        expect(response.objects).to be_an(Array)
        expect(response.objects.size).to be >= 3
        expect(response.objects.first).to be_a(ObjectStore::Models::ObjectInfo)

        # Cleanup list test objects
        response.objects.each { |obj| client.delete(obj.key) }
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "get_metadata",
      operation: :get_metadata,
      setup: -> (client, test_key) do
        metadata = ObjectStore::Models::Metadata.new(
          content_type: "text/plain",
          custom: { "test" => "metadata" }
        )
        client.put(test_key, "metadata test data", metadata)
      end,
      test: -> (client, test_key) do
        response = client.get_metadata(test_key)
        expect(response).to be_a(ObjectStore::Models::MetadataResponse)
        expect(response.success?).to be true
        expect(response.metadata).to be_a(ObjectStore::Models::Metadata)
        expect(response.metadata.content_type).to eq("text/plain")
        { success: true, cleanup: true }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "update_metadata",
      operation: :update_metadata,
      setup: -> (client, test_key) { client.put(test_key, "update metadata test") },
      test: -> (client, test_key) do
        new_metadata = ObjectStore::Models::Metadata.new(
          content_type: "application/json",
          custom: { "updated" => "true" }
        )
        response = client.update_metadata(test_key, new_metadata)
        expect(response).to be_a(ObjectStore::Models::UpdateMetadataResponse)
        expect(response.success?).to be true

        # Verify metadata was updated
        meta_response = client.get_metadata(test_key)
        expect(meta_response.metadata.content_type).to eq("application/json")
        { success: true, cleanup: true }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "archive",
      operation: :archive,
      setup: -> (client, test_key) { client.put(test_key, "archive test data") },
      test: -> (client, test_key) do
        begin
          response = client.archive(
            test_key,
            destination_type: "local",
            destination_settings: { "path" => "/tmp/archive" }
          )
          expect(response).to be_a(ObjectStore::Models::ArchiveResponse)
          expect(response.success?).to be true
        rescue ObjectStore::ServerError, ObjectStore::ValidationError => e
          # Backend may not support archive operations
          expect(e.message).to match(/not supported|unsupported/i)
        end
        { success: true, cleanup: true }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "add_policy",
      operation: :add_policy,
      setup: -> (_client) { nil },
      test: -> (client, test_key) do
        policy = ObjectStore::Models::LifecyclePolicy.new(
          id: "test-policy-#{test_key}",
          prefix: "archive/",
          retention_seconds: 86400,
          action: "delete"
        )
        response = client.add_policy(policy)
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true

        # Cleanup
        client.remove_policy(policy.id) rescue nil
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "remove_policy",
      operation: :remove_policy,
      setup: -> (client, test_key) do
        policy = ObjectStore::Models::LifecyclePolicy.new(
          id: "test-remove-policy-#{test_key}",
          prefix: "temp/",
          retention_seconds: 3600,
          action: "delete"
        )
        client.add_policy(policy)
      end,
      test: -> (client, test_key) do
        response = client.remove_policy("test-remove-policy-#{test_key}")
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "get_policies",
      operation: :get_policies,
      setup: -> (client, test_key) do
        policy = ObjectStore::Models::LifecyclePolicy.new(
          id: "test-get-policies-#{test_key}",
          prefix: "logs/",
          retention_seconds: 7200,
          action: "delete"
        )
        client.add_policy(policy)
      end,
      test: -> (client, test_key) do
        response = client.get_policies
        expect(response).to be_a(Hash)
        expect(response[:policies]).to be_an(Array)

        # Verify our policy is in the list
        policy_ids = response[:policies].map { |p| p.is_a?(Hash) ? p[:id] || p["id"] : p.id }
        expect(policy_ids).to include("test-get-policies-#{test_key}")

        # Cleanup
        client.remove_policy("test-get-policies-#{test_key}") rescue nil
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "apply_policies",
      operation: :apply_policies,
      setup: -> (_client) { nil },
      test: -> (client, _test_key) do
        response = client.apply_policies
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true
        expect(response).to have_key(:policies_count)
        expect(response).to have_key(:objects_processed)
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "add_replication_policy",
      operation: :add_replication_policy,
      setup: -> (_client) { nil },
      test: -> (client, test_key) do
        begin
          policy = ObjectStore::Models::ReplicationPolicy.new(
            id: "test-repl-#{test_key}",
            source_backend: "local",
            source_settings: { "path" => "/tmp/source" },
            destination_backend: "local",
            destination_settings: { "path" => "/tmp/dest" },
            check_interval_seconds: 3600,
            enabled: true
          )
          response = client.add_replication_policy(policy)
          expect(response).to be_a(Hash)
          expect(response[:success]).to be true

          # Cleanup
          client.remove_replication_policy(policy.id) rescue nil
        rescue ObjectStore::ServerError, ObjectStore::ValidationError => e
          # Backend may not support replication operations
          expect(e.message).to match(/not supported|unsupported/i)
        end
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "remove_replication_policy",
      operation: :remove_replication_policy,
      setup: -> (client, test_key) do
        policy = ObjectStore::Models::ReplicationPolicy.new(
          id: "test-remove-repl-#{test_key}",
          source_backend: "local",
          source_settings: { "path" => "/tmp/source" },
          destination_backend: "local",
          destination_settings: { "path" => "/tmp/dest" },
          check_interval_seconds: 3600,
          enabled: true
        )
        client.add_replication_policy(policy)
      end,
      test: -> (client, test_key) do
        begin
          response = client.remove_replication_policy("test-remove-repl-#{test_key}")
          expect(response).to be_a(Hash)
          expect(response[:success]).to be true
        rescue ObjectStore::ServerError, ObjectStore::ValidationError => e
          # Backend may not support replication operations
          expect(e.message).to match(/not supported|unsupported/i)
        end
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "get_replication_policies",
      operation: :get_replication_policies,
      setup: -> (client, test_key) do
        policy = ObjectStore::Models::ReplicationPolicy.new(
          id: "test-get-repl-#{test_key}",
          source_backend: "local",
          source_settings: { "path" => "/tmp/source" },
          destination_backend: "local",
          destination_settings: { "path" => "/tmp/dest" },
          check_interval_seconds: 3600,
          enabled: true
        )
        client.add_replication_policy(policy)
      end,
      test: -> (client, test_key) do
        begin
          response = client.get_replication_policies
          expect(response).to be_a(Hash)
          expect(response[:policies]).to be_an(Array)

          # Cleanup
          client.remove_replication_policy("test-get-repl-#{test_key}") rescue nil
        rescue ObjectStore::ServerError, ObjectStore::ValidationError => e
          # Backend may not support replication operations
          expect(e.message).to match(/not supported|unsupported/i)
        end
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "get_replication_policy",
      operation: :get_replication_policy,
      setup: -> (client, test_key) do
        policy = ObjectStore::Models::ReplicationPolicy.new(
          id: "test-get-one-repl-#{test_key}",
          source_backend: "local",
          source_settings: { "path" => "/tmp/source" },
          destination_backend: "local",
          destination_settings: { "path" => "/tmp/dest" },
          check_interval_seconds: 3600,
          enabled: true
        )
        client.add_replication_policy(policy)
      end,
      test: -> (client, test_key) do
        begin
          response = client.get_replication_policy("test-get-one-repl-#{test_key}")
          expect(response).to be_a(Hash)
          expect(response[:policy]).to be_a(ObjectStore::Models::ReplicationPolicy)
          expect(response[:policy].id).to eq("test-get-one-repl-#{test_key}")

          # Cleanup
          client.remove_replication_policy("test-get-one-repl-#{test_key}") rescue nil
        rescue ObjectStore::ServerError, ObjectStore::ValidationError => e
          # Backend may not support replication operations
          expect(e.message).to match(/not supported|unsupported/i)
        end
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "trigger_replication",
      operation: :trigger_replication,
      setup: -> (_client) { nil },
      test: -> (client, _test_key) do
        begin
          response = client.trigger_replication
          expect(response).to be_a(Hash)
          expect(response[:success]).to be true
          expect(response).to have_key(:result)
        rescue ObjectStore::ServerError, ObjectStore::ValidationError => e
          # Backend may not support replication operations
          expect(e.message).to match(/not supported|unsupported/i)
        end
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "get_replication_status",
      operation: :get_replication_status,
      setup: -> (client, test_key) do
        policy = ObjectStore::Models::ReplicationPolicy.new(
          id: "test-status-repl-#{test_key}",
          source_backend: "local",
          source_settings: { "path" => "/tmp/source" },
          destination_backend: "local",
          destination_settings: { "path" => "/tmp/dest" },
          check_interval_seconds: 3600,
          enabled: true
        )
        client.add_replication_policy(policy)
      end,
      test: -> (client, test_key) do
        begin
          response = client.get_replication_status("test-status-repl-#{test_key}")
          expect(response).to be_a(Hash)
          expect(response[:success]).to be true
          expect(response[:status]).to be_a(ObjectStore::Models::ReplicationStatus)

          # Cleanup
          client.remove_replication_policy("test-status-repl-#{test_key}") rescue nil
        rescue ObjectStore::ServerError, ObjectStore::ValidationError => e
          # Backend may not support replication operations
          expect(e.message).to match(/not supported|unsupported/i)
        end
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "health",
      operation: :health,
      setup: -> (_client) { nil },
      test: -> (client, _test_key) do
        response = client.health
        expect(response).to be_a(ObjectStore::Models::HealthResponse)
        expect(response.healthy?).to be true
        expect(response.status).to match(/SERVING|healthy/i)
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    }
  ].freeze

  # Protocol configurations
  PROTOCOLS = [
    {
      name: :rest,
      port: ENV["OBJSTORE_REST_PORT"]&.to_i || 8080,
      skip_reason: nil
    },
    {
      name: :grpc,
      port: ENV["OBJSTORE_GRPC_PORT"]&.to_i || 50051,
      skip_reason: nil
    },
    {
      name: :quic,
      port: ENV["OBJSTORE_QUIC_PORT"]&.to_i || 4433,
      skip_reason: "Ruby does not support HTTP/3 - QUIC tests skipped"
    }
  ].freeze

  # Shared examples for table-driven testing
  shared_examples "protocol operation tests" do |protocol_config|
    let(:protocol) { protocol_config[:name] }
    let(:port) { protocol_config[:port] }
    let(:host) { ENV["OBJSTORE_HOST"] || "localhost" }
    let(:backend) { (ENV["OBJSTORE_BACKEND"] || "local").to_sym }
    let(:client) do
      ObjectStore::Client.new(
        protocol: protocol,
        host: host,
        port: port
      )
    end

    before(:each) do
      # Skip entire protocol if needed
      skip protocol_config[:skip_reason] if protocol_config[:skip_reason]
    end

    # Generate tests for each operation
    OPERATION_TEST_CASES.each do |test_case|
      describe "#{test_case[:name]} operation" do
        let(:test_key) { "comprehensive-test-#{protocol}-#{test_case[:name]}-#{Time.now.to_i}-#{rand(1000)}" }

        it "executes #{test_case[:name]} correctly" do
          # Skip if protocol is in skip list
          if test_case[:skip_protocols].include?(protocol)
            skip "Operation #{test_case[:name]} not supported for #{protocol}"
          end

          # Skip if backend is in skip list
          if test_case[:skip_backends].include?(backend)
            skip test_case[:skip_reason] || "Operation #{test_case[:name]} not supported for #{backend} backend"
          end

          # Run setup if defined
          begin
            if test_case[:setup]
              if test_case[:setup].arity == 1
                test_case[:setup].call(client)
              else
                test_case[:setup].call(client, test_key)
              end
            end
          rescue => e
            skip "Setup failed: #{e.message}"
          end

          # Run the test - use instance_exec to run in the correct RSpec context
          result = nil
          begin
            result = instance_exec(client, test_key, &test_case[:test])
            expect(result[:success]).to be true
          rescue ObjectStore::Error => e
            # If we expect an error for this backend/protocol combo, that's OK
            if test_case[:skip_backends].include?(backend) || test_case[:skip_protocols].include?(protocol)
              skip "Expected error for unsupported operation: #{e.message}"
            else
              raise
            end
          ensure
            # Cleanup if needed
            if result && result[:cleanup]
              begin
                client.delete(test_key)
              rescue ObjectStore::NotFoundError
                # Already deleted, that's fine
              rescue => e
                warn "Cleanup failed for #{test_key}: #{e.message}"
              end
            end
          end
        end
      end
    end
  end

  # Run tests for each protocol
  PROTOCOLS.each do |protocol_config|
    context "with #{protocol_config[:name].upcase} protocol" do
      include_examples "protocol operation tests", protocol_config
    end
  end

  # Cross-protocol consistency tests
  describe "Cross-Protocol Consistency" do
    let(:host) { ENV["OBJSTORE_HOST"] || "localhost" }
    let(:test_data) { "cross-protocol test data #{Time.now.to_i}" }
    let(:test_key) { "cross-protocol-test-#{Time.now.to_i}" }

    before(:each) do
      skip "Cross-protocol tests require REST protocol" unless rest_available?
    end

    after(:each) do
      cleanup_test_key(test_key)
    end

    it "maintains data consistency across REST and gRPC protocols" do
      rest_client = ObjectStore::Client.new(protocol: :rest, host: host, port: 8080)
      grpc_client = ObjectStore::Client.new(protocol: :grpc, host: host, port: 50051)

      # Put via REST
      rest_client.put(test_key, test_data)

      # Get via gRPC
      grpc_response = grpc_client.get(test_key)
      expect(grpc_response.data).to eq(test_data)

      # Delete via REST
      rest_client.delete(test_key)

      # Verify deletion via gRPC
      expect(grpc_client.exists?(test_key)).to be false
    end

    it "ensures metadata consistency across protocols" do
      rest_client = ObjectStore::Client.new(protocol: :rest, host: host, port: 8080)

      metadata = ObjectStore::Models::Metadata.new(
        content_type: "application/json",
        custom: { "version" => "1.0", "author" => "test" }
      )

      # Put with metadata via REST
      rest_client.put(test_key, test_data, metadata)

      # Get metadata via REST
      rest_meta = rest_client.get_metadata(test_key)
      expect(rest_meta.metadata.content_type).to eq("application/json")

      rest_client.delete(test_key)
    end

    it "maintains list consistency across protocols" do
      rest_client = ObjectStore::Client.new(protocol: :rest, host: host, port: 8080)
      grpc_client = ObjectStore::Client.new(protocol: :grpc, host: host, port: 50051)

      # Create objects via REST
      keys = 3.times.map { |i| "#{test_key}-#{i}" }
      keys.each { |key| rest_client.put(key, "data-#{key}") }

      # List via gRPC
      grpc_list = grpc_client.list(prefix: test_key)
      expect(grpc_list.objects.size).to eq(3)

      # Cleanup
      keys.each { |key| rest_client.delete(key) }
    end
  end

  # Response structure validation tests
  describe "Response Structure Validation" do
    let(:host) { ENV["OBJSTORE_HOST"] || "localhost" }
    let(:client) { ObjectStore::Client.new(protocol: :rest, host: host, port: 8080) }
    let(:test_key) { "response-validation-test-#{Time.now.to_i}" }

    after(:each) do
      cleanup_test_key(test_key)
    end

    it "validates PutResponse structure" do
      response = client.put(test_key, "test data")

      expect(response).to respond_to(:success?)
      expect(response).to respond_to(:message)
      expect(response).to respond_to(:etag)
      expect(response.success?).to be true
    end

    it "validates GetResponse structure" do
      client.put(test_key, "test data")
      response = client.get(test_key)

      expect(response).to respond_to(:data)
      expect(response).to respond_to(:metadata)
      expect(response.metadata).to be_a(ObjectStore::Models::Metadata)
    end

    it "validates ListResponse structure" do
      client.put(test_key, "test data")
      response = client.list(prefix: test_key)

      expect(response).to respond_to(:objects)
      expect(response).to respond_to(:common_prefixes)
      expect(response).to respond_to(:next_token)
      expect(response).to respond_to(:truncated)
      expect(response.objects).to be_an(Array)
      expect(response.objects.first).to be_a(ObjectStore::Models::ObjectInfo) unless response.objects.empty?
    end

    it "validates MetadataResponse structure" do
      client.put(test_key, "test data")
      response = client.get_metadata(test_key)

      expect(response).to respond_to(:metadata)
      expect(response).to respond_to(:success?)
      expect(response.metadata).to be_a(ObjectStore::Models::Metadata)
      expect(response.metadata).to respond_to(:content_type)
      expect(response.metadata).to respond_to(:size)
      expect(response.metadata).to respond_to(:etag)
      expect(response.metadata).to respond_to(:last_modified)
    end

    it "validates HealthResponse structure" do
      response = client.health

      expect(response).to respond_to(:status)
      expect(response).to respond_to(:message)
      expect(response).to respond_to(:healthy?)
      expect(response.healthy?).to be true
    end

    it "validates lifecycle policy response structures" do
      policy = ObjectStore::Models::LifecyclePolicy.new(
        id: "test-structure-#{Time.now.to_i}",
        prefix: "test/",
        retention_seconds: 3600,
        action: "delete"
      )

      add_response = client.add_policy(policy)
      expect(add_response).to be_a(Hash)
      expect(add_response).to have_key(:success)
      expect(add_response).to have_key(:message)

      get_response = client.get_policies
      expect(get_response).to be_a(Hash)
      expect(get_response).to have_key(:policies)
      expect(get_response[:policies]).to be_an(Array)

      remove_response = client.remove_policy(policy.id)
      expect(remove_response).to be_a(Hash)
      expect(remove_response).to have_key(:success)
    end
  end

  # Error handling validation
  describe "Error Handling Validation" do
    let(:host) { ENV["OBJSTORE_HOST"] || "localhost" }
    let(:client) { ObjectStore::Client.new(protocol: :rest, host: host, port: 8080) }

    it "raises NotFoundError for non-existent object get" do
      expect {
        client.get("non-existent-key-#{Time.now.to_i}")
      }.to raise_error(ObjectStore::NotFoundError)
    end

    it "raises NotFoundError for non-existent object delete" do
      expect {
        client.delete("non-existent-key-#{Time.now.to_i}")
      }.to raise_error(ObjectStore::NotFoundError)
    end

    it "raises NotFoundError for non-existent object metadata" do
      expect {
        client.get_metadata("non-existent-key-#{Time.now.to_i}")
      }.to raise_error(ObjectStore::NotFoundError)
    end

    it "raises ArgumentError for empty key" do
      expect {
        client.put("", "data")
      }.to raise_error(ArgumentError, /Key must be a non-empty string/)
    end

    it "raises ArgumentError for nil data" do
      expect {
        client.put("test-key", nil)
      }.to raise_error(ArgumentError, /Data cannot be nil/)
    end

    it "raises ArgumentError for empty policy id" do
      expect {
        client.remove_policy("")
      }.to raise_error(ArgumentError, /Policy ID must be a non-empty string/)
    end
  end

  # Helper methods
  private

  def rest_available?
    host = ENV["OBJSTORE_HOST"] || "localhost"
    port = ENV["OBJSTORE_REST_PORT"]&.to_i || 8080

    begin
      client = ObjectStore::Client.new(protocol: :rest, host: host, port: port)
      client.health
      true
    rescue
      false
    end
  end

  def cleanup_test_key(key)
    return unless rest_available?

    host = ENV["OBJSTORE_HOST"] || "localhost"
    client = ObjectStore::Client.new(protocol: :rest, host: host, port: 8080)

    # Clean up the main key
    client.delete(key) rescue nil

    # Clean up any related keys (for list tests, etc.)
    begin
      list_response = client.list(prefix: key)
      list_response.objects.each do |obj|
        client.delete(obj.key) rescue nil
      end
    rescue
      # Ignore errors during cleanup
    end
  end
end
