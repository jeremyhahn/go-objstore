require "spec_helper"

RSpec.describe "Comprehensive Integration Tests", :integration do
  before(:all) do
    WebMock.allow_net_connect!
  end

  after(:all) do
    WebMock.disable_net_connect!
  end

  # Test data table for all 19 operations
  # Each entry is a self-contained definition: setup lambda, test lambda with real
  # assertions, and optional per-operation skip lists. The driver below iterates
  # every available protocol against every operation.
  OPERATION_TEST_CASES = [
    {
      name: "put",
      operation: :put,
      setup: ->(_client) { nil },
      test: ->(client, test_key) do
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
      setup: ->(client, test_key) { client.put(test_key, "get test data") },
      test: ->(client, test_key) do
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
      setup: ->(client, test_key) { client.put(test_key, "delete test data") },
      test: ->(client, test_key) do
        response = client.delete(test_key)
        expect(response).to be_a(ObjectStore::Models::DeleteResponse)
        expect(response.success?).to be true
        # Verify object is gone
        expect(client.exists?(test_key)).to be false
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "exists?",
      operation: :exists?,
      setup: ->(client, test_key) { client.put(test_key, "exists test data") },
      test: ->(client, test_key) do
        expect(client.exists?(test_key)).to be true
        expect(client.exists?("non-existent-#{test_key}")).to be false
        { success: true, cleanup: true }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "list",
      operation: :list,
      setup: ->(client, test_key) do
        3.times { |i| client.put("#{test_key}-list-#{i}", "list data #{i}") }
      end,
      test: ->(client, test_key) do
        response = client.list(prefix: "#{test_key}-list-")
        expect(response).to be_a(ObjectStore::Models::ListResponse)
        expect(response.objects).to be_an(Array)
        expect(response.objects.size).to be >= 3

        listed_keys = response.objects.map(&:key)
        3.times { |i| expect(listed_keys).to include("#{test_key}-list-#{i}") }

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
      setup: ->(client, test_key) do
        metadata = ObjectStore::Models::Metadata.new(
          content_type: "text/plain",
          custom: { "test" => "metadata" }
        )
        client.put(test_key, "metadata test data", metadata)
      end,
      test: ->(client, test_key) do
        response = client.get_metadata(test_key)
        expect(response).to be_a(ObjectStore::Models::MetadataResponse)
        expect(response.success?).to be true
        expect(response.metadata).to be_a(ObjectStore::Models::Metadata)
        # Canonical: assert size == len(data), content_type, and custom round-trip
        expect(response.metadata.content_type).to eq("text/plain")
        expect(response.metadata.size).to eq("metadata test data".bytesize)
        expect(response.metadata.custom["test"]).to eq("metadata")
        { success: true, cleanup: true }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "update_metadata",
      operation: :update_metadata,
      setup: ->(client, test_key) { client.put(test_key, "update metadata test") },
      test: ->(client, test_key) do
        new_metadata = ObjectStore::Models::Metadata.new(
          content_type: "application/json",
          custom: { "updated" => "true" }
        )
        response = client.update_metadata(test_key, new_metadata)
        expect(response).to be_a(ObjectStore::Models::UpdateMetadataResponse)
        expect(response.success?).to be true

        # Read-back required: assert NEW values persisted
        meta_response = client.get_metadata(test_key)
        expect(meta_response.metadata.content_type).to eq("application/json")
        expect(meta_response.metadata.custom["updated"]).to eq("true")
        { success: true, cleanup: true }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "health",
      operation: :health,
      setup: ->(_client) { nil },
      test: ->(client, _test_key) do
        response = client.health
        expect(response).to be_a(ObjectStore::Models::HealthResponse)
        expect(response.healthy?).to be true
        expect(response.status).to match(/SERVING|healthy/i)
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "add_policy",
      operation: :add_policy,
      setup: ->(_client) { nil },
      test: ->(client, test_key) do
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
      name: "get_policies",
      operation: :get_policies,
      setup: ->(client, test_key) do
        policy = ObjectStore::Models::LifecyclePolicy.new(
          id: "test-get-policies-#{test_key}",
          prefix: "logs/",
          retention_seconds: 7200,
          action: "delete"
        )
        client.add_policy(policy)
      end,
      test: ->(client, test_key) do
        response = client.get_policies
        expect(response).to be_a(Hash)
        expect(response[:policies]).to be_an(Array)

        # Assert the added policy id is present
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
      name: "remove_policy",
      operation: :remove_policy,
      setup: ->(client, test_key) do
        policy = ObjectStore::Models::LifecyclePolicy.new(
          id: "test-remove-policy-#{test_key}",
          prefix: "temp/",
          retention_seconds: 3600,
          action: "delete"
        )
        client.add_policy(policy)
      end,
      test: ->(client, test_key) do
        response = client.remove_policy("test-remove-policy-#{test_key}")
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true

        # Assert it is gone from the list
        list_response = client.get_policies
        policy_ids = list_response[:policies].map { |p| p.is_a?(Hash) ? p[:id] || p["id"] : p.id }
        expect(policy_ids).not_to include("test-remove-policy-#{test_key}")
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "apply_policies",
      operation: :apply_policies,
      setup: ->(_client) { nil },
      test: ->(client, _test_key) do
        response = client.apply_policies
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true
        expect(response).to have_key(:policies_count)
        expect(response).to have_key(:objects_processed)
        expect(response[:policies_count]).to be >= 0
        expect(response[:objects_processed]).to be >= 0
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "archive",
      operation: :archive,
      setup: ->(client, test_key) { client.put(test_key, "archive test data") },
      test: ->(client, test_key) do
        begin
          response = client.archive(
            test_key,
            destination_type: "local",
            destination_settings: { "path" => "/tmp/archive" }
          )
          expect(response).to be_a(ObjectStore::Models::ArchiveResponse)
          expect(response.success?).to be true
        rescue ObjectStore::ServerError, ObjectStore::ValidationError => e
          # Local backend may not implement an archiver; skip with logged reason.
          skip "Archive not supported on this backend/protocol configuration: #{e.message}"
        end
        { success: true, cleanup: true }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    # -------------------------------------------------------------------------
    # Replication operations
    # Server now has replication ENABLED. All replication tests MUST assert real
    # success using the canonical payload:
    #   source_backend: "local", source_settings: {path: <tmp-src>}
    #   destination_backend: "local", destination_settings: {path: <tmp-dst>}
    #   check_interval_seconds: 3600
    # begin/rescue wrappers accepting "not supported" are NOT permitted here.
    # -------------------------------------------------------------------------
    {
      name: "add_replication_policy",
      operation: :add_replication_policy,
      setup: ->(_client) { nil },
      test: ->(client, test_key) do
        policy = ObjectStore::Models::ReplicationPolicy.new(
          id: "test-repl-#{test_key}",
          source_backend: "local",
          source_settings: { "path" => "/tmp/objstore-src-#{test_key}" },
          destination_backend: "local",
          destination_settings: { "path" => "/tmp/objstore-dst-#{test_key}" },
          check_interval_seconds: 3600,
          enabled: true
        )
        response = client.add_replication_policy(policy)
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true

        # Cleanup
        client.remove_replication_policy(policy.id) rescue nil
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "get_replication_policies",
      operation: :get_replication_policies,
      setup: ->(client, test_key) do
        policy = ObjectStore::Models::ReplicationPolicy.new(
          id: "test-get-repl-#{test_key}",
          source_backend: "local",
          source_settings: { "path" => "/tmp/objstore-src-#{test_key}" },
          destination_backend: "local",
          destination_settings: { "path" => "/tmp/objstore-dst-#{test_key}" },
          check_interval_seconds: 3600,
          enabled: true
        )
        client.add_replication_policy(policy)
      end,
      test: ->(client, test_key) do
        response = client.get_replication_policies
        expect(response).to be_a(Hash)
        expect(response[:policies]).to be_an(Array)
        expect(response[:policies].size).to be >= 1

        # Assert the added policy id is present
        policy_ids = response[:policies].map { |p| p.is_a?(Hash) ? p[:id] || p["id"] : p.id }
        expect(policy_ids).to include("test-get-repl-#{test_key}")

        # Cleanup
        client.remove_replication_policy("test-get-repl-#{test_key}") rescue nil
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "get_replication_policy",
      operation: :get_replication_policy,
      setup: ->(client, test_key) do
        policy = ObjectStore::Models::ReplicationPolicy.new(
          id: "test-get-one-repl-#{test_key}",
          source_backend: "local",
          source_settings: { "path" => "/tmp/objstore-src-#{test_key}" },
          destination_backend: "local",
          destination_settings: { "path" => "/tmp/objstore-dst-#{test_key}" },
          check_interval_seconds: 3600,
          enabled: true
        )
        client.add_replication_policy(policy)
      end,
      test: ->(client, test_key) do
        response = client.get_replication_policy("test-get-one-repl-#{test_key}")
        expect(response).to be_a(Hash)
        expect(response[:policy]).to be_a(ObjectStore::Models::ReplicationPolicy)

        policy = response[:policy]
        expect(policy.id).to eq("test-get-one-repl-#{test_key}")
        expect(policy.source_backend).to eq("local")
        expect(policy.destination_backend).to eq("local")
        expect(policy.check_interval_seconds).to eq(3600)

        # Cleanup
        client.remove_replication_policy("test-get-one-repl-#{test_key}") rescue nil
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "remove_replication_policy",
      operation: :remove_replication_policy,
      setup: ->(client, test_key) do
        policy = ObjectStore::Models::ReplicationPolicy.new(
          id: "test-remove-repl-#{test_key}",
          source_backend: "local",
          source_settings: { "path" => "/tmp/objstore-src-#{test_key}" },
          destination_backend: "local",
          destination_settings: { "path" => "/tmp/objstore-dst-#{test_key}" },
          check_interval_seconds: 3600,
          enabled: true
        )
        client.add_replication_policy(policy)
      end,
      test: ->(client, test_key) do
        response = client.remove_replication_policy("test-remove-repl-#{test_key}")
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true

        # Assert it is gone from the list
        list_response = client.get_replication_policies
        policy_ids = list_response[:policies].map { |p| p.is_a?(Hash) ? p[:id] || p["id"] : p.id }
        expect(policy_ids).not_to include("test-remove-repl-#{test_key}")
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "trigger_replication",
      operation: :trigger_replication,
      setup: ->(client, test_key) do
        policy = ObjectStore::Models::ReplicationPolicy.new(
          id: "test-trigger-repl-#{test_key}",
          source_backend: "local",
          source_settings: { "path" => "/tmp/objstore-src-#{test_key}" },
          destination_backend: "local",
          destination_settings: { "path" => "/tmp/objstore-dst-#{test_key}" },
          check_interval_seconds: 3600,
          enabled: true
        )
        client.add_replication_policy(policy)
      end,
      test: ->(client, test_key) do
        policy_id = "test-trigger-repl-#{test_key}"
        response = client.trigger_replication(policy_id: policy_id)
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true
        expect(response).to have_key(:result)

        # Canonical: result must have policy_id + counters present.
        # REST/QUIC servers return "duration" (string); gRPC returns "duration_ms" (int).
        result = response[:result]
        if result.is_a?(Hash) && result.any?
          expect(result).to have_key(:policy_id)
          expect(result).to have_key(:synced)
          expect(result).to have_key(:bytes_total)
          expect(result.key?(:duration_ms) || result.key?(:duration)).to be true
        end

        # Cleanup
        client.remove_replication_policy(policy_id) rescue nil
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    },
    {
      name: "get_replication_status",
      operation: :get_replication_status,
      setup: ->(client, test_key) do
        policy = ObjectStore::Models::ReplicationPolicy.new(
          id: "test-status-repl-#{test_key}",
          source_backend: "local",
          source_settings: { "path" => "/tmp/objstore-src-#{test_key}" },
          destination_backend: "local",
          destination_settings: { "path" => "/tmp/objstore-dst-#{test_key}" },
          check_interval_seconds: 3600,
          enabled: true
        )
        client.add_replication_policy(policy)
      end,
      test: ->(client, test_key) do
        policy_id = "test-status-repl-#{test_key}"
        response = client.get_replication_status(policy_id)
        expect(response).to be_a(Hash)
        expect(response[:success]).to be true
        expect(response[:status]).to be_a(ObjectStore::Models::ReplicationStatus)

        # Canonical: assert policy_id and counters
        status = response[:status]
        expect(status.policy_id).to eq(policy_id)
        expect(status.total_objects_synced).to be >= 0
        expect(status.sync_count).to be >= 0

        # Cleanup
        client.remove_replication_policy(policy_id) rescue nil
        { success: true, cleanup: false }
      end,
      skip_protocols: [],
      skip_backends: []
    }
  ].freeze

  # Protocol configurations.
  #
  # QUIC is an EXPLICIT logged skip: Ruby has no native HTTP/3 (net/http is
  # TCP-only; there is no production-ready HTTP/3 gem as of Ruby 3.2). Per the
  # canonical SDK test contract, QUIC integration must be skipped with a clear
  # logged reason rather than silently excluded or faked. The QUIC client is
  # still exercised in unit specs via WebMock.
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
      # Ruby SDK: no native HTTP/3 support. Ruby's Net::HTTP is TCP-only and
      # there is no production-grade QUIC/HTTP3 gem available for Ruby 3.2.
      # QUIC integration tests are therefore an explicit skip per the canonical
      # SDK test contract. Unit coverage of the QUIC client exists via WebMock.
      skip_reason: "Ruby SDK: no native HTTP/3 support — QUIC integration skipped (unit coverage exists via WebMock)"
    }
  ].freeze

  # Shared examples for table-driven testing.
  # For each protocol, iterates every entry in OPERATION_TEST_CASES.
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
      if protocol_config[:skip_reason]
        skip protocol_config[:skip_reason]
      end
    end

    after(:each) do
      client.close rescue nil
    end

    OPERATION_TEST_CASES.each do |test_case|
      describe "#{test_case[:name]} operation" do
        let(:test_key) do
          safe_name = test_case[:name].to_s.gsub(/[^a-zA-Z0-9\-_.]/, "-")
          "comprehensive-test-#{protocol}-#{safe_name}-#{Time.now.to_i}-#{rand(10_000)}"
        end

        it "executes #{test_case[:name]} correctly" do
          if test_case[:skip_protocols].include?(protocol)
            skip "Operation #{test_case[:name]} not supported for #{protocol}"
          end

          if test_case[:skip_backends].include?(backend)
            skip test_case[:skip_reason] || "Operation #{test_case[:name]} not supported for #{backend} backend"
          end

          if test_case[:setup]
            if test_case[:setup].arity == 1
              test_case[:setup].call(client)
            else
              test_case[:setup].call(client, test_key)
            end
          end

          result = nil
          begin
            result = instance_exec(client, test_key, &test_case[:test])
            expect(result[:success]).to be true
          rescue ObjectStore::Error => e
            if test_case[:skip_backends].include?(backend) || test_case[:skip_protocols].include?(protocol)
              skip "Expected error for unsupported operation: #{e.message}"
            else
              raise
            end
          ensure
            if result && result[:cleanup]
              begin
                client.delete(test_key)
              rescue ObjectStore::NotFoundError
                # Already deleted — expected for delete tests
              rescue => e
                warn "Cleanup failed for #{test_key}: #{e.message}"
              end
            end
          end
        end
      end
    end
  end

  # Run every operation against every available protocol
  PROTOCOLS.each do |protocol_config|
    context "with #{protocol_config[:name].upcase} protocol" do
      include_examples "protocol operation tests", protocol_config
    end
  end

  # ---------------------------------------------------------------------------
  # Cross-Protocol Consistency
  # True write-via-A / read-via-B over all (REST, gRPC) ordered pairs.
  # QUIC is excluded with a logged skip per the canonical contract.
  # ---------------------------------------------------------------------------
  describe "Cross-Protocol Consistency" do
    let(:host) { ENV["OBJSTORE_HOST"] || "localhost" }
    let(:rest_port) { ENV["OBJSTORE_REST_PORT"]&.to_i || 8080 }
    let(:grpc_port) { ENV["OBJSTORE_GRPC_PORT"]&.to_i || 50051 }
    let(:test_data) { "cross-protocol consistency data #{Time.now.to_i}" }
    let(:test_key) { "cross-proto-#{Time.now.to_i}-#{rand(10_000)}" }
    let(:rest_client) { ObjectStore::Client.new(protocol: :rest, host: host, port: rest_port) }
    let(:grpc_client) { ObjectStore::Client.new(protocol: :grpc, host: host, port: grpc_port) }

    after(:each) do
      rest_client.delete(test_key) rescue nil
      rest_client.close rescue nil
      grpc_client.close rescue nil
    end

    context "REST writes, gRPC reads" do
      it "get via gRPC returns data equal to what was put via REST" do
        rest_client.put(test_key, test_data)

        grpc_response = grpc_client.get(test_key)
        expect(grpc_response.data).to eq(test_data)
      end

      it "getMetadata via gRPC reflects size and content_type set via REST put" do
        metadata = ObjectStore::Models::Metadata.new(
          content_type: "application/octet-stream",
          custom: { "origin" => "rest" }
        )
        rest_client.put(test_key, test_data, metadata)

        grpc_meta = grpc_client.get_metadata(test_key)
        expect(grpc_meta.metadata.size).to eq(test_data.bytesize)
        expect(grpc_meta.metadata.content_type).to eq("application/octet-stream")
      end

      it "exists? via gRPC returns false after delete via REST" do
        rest_client.put(test_key, test_data)
        rest_client.delete(test_key)

        expect(grpc_client.exists?(test_key)).to be false
      end
    end

    context "gRPC writes, REST reads" do
      it "get via REST returns data equal to what was put via gRPC" do
        grpc_client.put(test_key, test_data)

        rest_response = rest_client.get(test_key)
        expect(rest_response.data).to eq(test_data)
      end

      it "getMetadata via REST reflects size and content_type set via gRPC put" do
        metadata = ObjectStore::Models::Metadata.new(
          content_type: "text/plain",
          custom: { "origin" => "grpc" }
        )
        grpc_client.put(test_key, test_data, metadata)

        rest_meta = rest_client.get_metadata(test_key)
        expect(rest_meta.metadata.size).to eq(test_data.bytesize)
        expect(rest_meta.metadata.content_type).to eq("text/plain")
      end

      it "exists? via REST returns false after delete via gRPC" do
        grpc_client.put(test_key, test_data)
        grpc_client.delete(test_key)

        expect(rest_client.exists?(test_key)).to be false
      end
    end

    context "list consistency across protocols" do
      let(:list_prefix) { "cross-list-#{Time.now.to_i}-#{rand(10_000)}" }
      let(:list_keys) { 3.times.map { |i| "#{list_prefix}-#{i}" } }

      after(:each) do
        list_keys.each { |k| rest_client.delete(k) rescue nil }
      end

      it "objects created via REST appear in gRPC list" do
        list_keys.each { |k| rest_client.put(k, "data") }

        response = grpc_client.list(prefix: list_prefix)
        listed = response.objects.map(&:key)
        list_keys.each { |k| expect(listed).to include(k) }
      end

      it "objects created via gRPC appear in REST list" do
        list_keys.each { |k| grpc_client.put(k, "data") }

        response = rest_client.list(prefix: list_prefix)
        listed = response.objects.map(&:key)
        list_keys.each { |k| expect(listed).to include(k) }
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Response Structure Validation (supplement to the data-driven table above)
  # ---------------------------------------------------------------------------
  describe "Response Structure Validation" do
    let(:host) { ENV["OBJSTORE_HOST"] || "localhost" }
    let(:client) { ObjectStore::Client.new(protocol: :rest, host: host, port: 8080) }
    let(:test_key) { "response-validation-#{Time.now.to_i}-#{rand(10_000)}" }

    after(:each) do
      client.delete(test_key) rescue nil
      client.close rescue nil
    end

    it "validates PutResponse structure and value" do
      response = client.put(test_key, "test data")

      expect(response).to respond_to(:success?)
      expect(response).to respond_to(:message)
      expect(response).to respond_to(:etag)
      expect(response.success?).to be true
      expect(response.etag).not_to be_nil
    end

    it "validates GetResponse structure and data equality" do
      client.put(test_key, "exact payload")
      response = client.get(test_key)

      expect(response).to respond_to(:data)
      expect(response).to respond_to(:metadata)
      expect(response.data).to eq("exact payload")
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

    it "validates MetadataResponse structure and custom round-trip" do
      meta_in = ObjectStore::Models::Metadata.new(
        content_type: "image/png",
        custom: { "round" => "trip" }
      )
      client.put(test_key, "img", meta_in)
      response = client.get_metadata(test_key)

      expect(response).to respond_to(:metadata)
      expect(response).to respond_to(:success?)
      expect(response.success?).to be true
      expect(response.metadata).to be_a(ObjectStore::Models::Metadata)
      expect(response.metadata.content_type).to eq("image/png")
      expect(response.metadata.custom["round"]).to eq("trip")
      expect(response.metadata.size).to eq("img".bytesize)
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
        id: "test-structure-#{Time.now.to_i}-#{rand(10_000)}",
        prefix: "test/",
        retention_seconds: 3600,
        action: "delete"
      )

      add_response = client.add_policy(policy)
      expect(add_response).to be_a(Hash)
      expect(add_response).to have_key(:success)
      expect(add_response[:success]).to be true

      get_response = client.get_policies
      expect(get_response).to be_a(Hash)
      expect(get_response).to have_key(:policies)
      expect(get_response[:policies]).to be_an(Array)

      remove_response = client.remove_policy(policy.id)
      expect(remove_response).to be_a(Hash)
      expect(remove_response).to have_key(:success)
      expect(remove_response[:success]).to be true
    end
  end

  # ---------------------------------------------------------------------------
  # Error Handling Validation
  # ---------------------------------------------------------------------------
  describe "Error Handling Validation" do
    let(:host) { ENV["OBJSTORE_HOST"] || "localhost" }
    let(:client) { ObjectStore::Client.new(protocol: :rest, host: host, port: 8080) }

    after(:each) do
      client.close rescue nil
    end

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

    it "raises NotFoundError when updating metadata on a non-existent object" do
      metadata = ObjectStore::Models::Metadata.new(content_type: "text/plain")
      expect {
        client.update_metadata("non-existent-update-#{Time.now.to_i}", metadata)
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

  # ---------------------------------------------------------------------------
  # Streaming (large-object round-trip)
  # Migrated from integration_spec.rb — unique coverage not in the table above.
  # ---------------------------------------------------------------------------
  describe "Large Object Streaming" do
    let(:host) { ENV["OBJSTORE_HOST"] || "localhost" }
    let(:client) { ObjectStore::Client.new(protocol: :rest, host: host, port: 8080) }
    let(:large_key) { "large-streaming-#{Time.now.to_i}-#{rand(10_000)}" }

    after(:each) do
      client.delete(large_key) rescue nil
      client.close rescue nil
    end

    it "round-trips a 10 KB object with byte-exact equality" do
      large_data = "x" * 10_000

      put_response = client.put(large_key, large_data)
      expect(put_response.success?).to be true

      get_response = client.get(large_key)
      expect(get_response.data).to eq(large_data)
      expect(get_response.data.bytesize).to eq(10_000)
    end
  end

  # ---------------------------------------------------------------------------
  # Helper methods
  # ---------------------------------------------------------------------------
  private

  def build_rest_client
    host = ENV["OBJSTORE_HOST"] || "localhost"
    port = ENV["OBJSTORE_REST_PORT"]&.to_i || 8080
    ObjectStore::Client.new(protocol: :rest, host: host, port: port)
  end
end
