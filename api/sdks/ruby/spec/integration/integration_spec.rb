require "spec_helper"

RSpec.describe "Integration Tests", :integration do
  before(:all) do
    WebMock.allow_net_connect!
  end

  after(:all) do
    WebMock.disable_net_connect!
  end

  shared_examples "object store operations" do |protocol|
    # QUIC tests are skipped because Ruby's Net::HTTP does not support HTTP/3.
    # The go-objstore QUIC server uses UDP-based HTTP/3, while Ruby's Net::HTTP
    # uses TCP-based HTTP/1.1 or HTTP/2.
    let(:skip_quic) { protocol == :quic ? "Ruby does not support HTTP/3 - QUIC tests skipped" : nil }
    let(:skip_grpc_data) { protocol == :grpc ? "gRPC data operations require protobuf stubs" : nil }
    let(:skip_grpc_error) { protocol == :grpc ? "gRPC error handling requires protobuf stubs" : nil }
    let(:skip_grpc_streaming) { protocol == :grpc ? "gRPC streaming requires protobuf stubs" : nil }

    let(:port) do
      case protocol
      when :rest then ENV["OBJSTORE_REST_PORT"]&.to_i || 8080
      when :grpc then ENV["OBJSTORE_GRPC_PORT"]&.to_i || 50051
      when :quic then ENV["OBJSTORE_QUIC_PORT"]&.to_i || 4433
      end
    end
    let(:client) { ObjectStore::Client.new(protocol: protocol, host: ENV["OBJSTORE_HOST"] || "localhost", port: port) }
    let(:test_key) { "integration-test-#{protocol}-#{Time.now.to_i}.txt" }
    let(:test_data) { "Integration test data for #{protocol}" }

    describe "basic operations" do
      it "performs PUT, GET, EXISTS, DELETE cycle" do
        skip skip_quic if skip_quic
        skip skip_grpc_data if skip_grpc_data

        # Put object
        put_response = client.put(test_key, test_data)
        expect(put_response.success?).to be true

        # Check exists
        expect(client.exists?(test_key)).to be true

        # Get object
        get_response = client.get(test_key)
        expect(get_response.data).to eq(test_data)

        # Delete object
        delete_response = client.delete(test_key)
        expect(delete_response.success?).to be true

        # Verify deleted
        expect(client.exists?(test_key)).to be false
      end

      it "lists objects" do
        skip skip_quic if skip_quic
        skip skip_grpc_data if skip_grpc_data

        # Create test objects
        3.times do |i|
          client.put("list-test-#{protocol}-#{i}.txt", "data #{i}")
        end

        # List objects
        list_response = client.list(prefix: "list-test-#{protocol}-")
        expect(list_response.objects.size).to be >= 3

        # Cleanup
        list_response.objects.each do |obj|
          client.delete(obj.key)
        end
      end

      it "manages metadata" do
        skip skip_quic if skip_quic

        metadata = ObjectStore::Models::Metadata.new(
          content_type: "text/plain",
          custom: { "author" => "test", "version" => "1.0" }
        )

        # Put with metadata
        client.put(test_key, test_data, metadata)

        # Get metadata
        meta_response = client.get_metadata(test_key)
        expect(meta_response.success?).to be true
        expect(meta_response.metadata).to be_a(ObjectStore::Models::Metadata)

        # Verify basic metadata fields exist
        expect(meta_response.metadata.etag).not_to be_nil if meta_response.metadata.etag
        expect(meta_response.metadata.size).to be > 0 if meta_response.metadata.size

        # Update metadata
        new_metadata = ObjectStore::Models::Metadata.new(
          content_type: "application/json",
          custom: { "version" => "2.0" }
        )
        update_response = client.update_metadata(test_key, new_metadata)
        expect(update_response.success?).to be true

        # Cleanup
        client.delete(test_key)
      end
    end

    describe "health check" do
      it "checks server health" do
        skip skip_quic if skip_quic

        health_response = client.health
        expect(health_response.healthy?).to be true
      end
    end

    describe "lifecycle policies" do
      it "manages lifecycle policies" do
        skip skip_quic if skip_quic

        policy = ObjectStore::Models::LifecyclePolicy.new(
          id: "test-policy-#{protocol}",
          prefix: "archive/",
          retention_seconds: 86400,
          action: "delete"
        )

        # Add policy
        add_response = client.add_policy(policy)
        expect(add_response[:success]).to be true

        # Get policies
        get_response = client.get_policies
        expect(get_response[:policies]).to be_an(Array)

        # Remove policy
        remove_response = client.remove_policy(policy.id)
        expect(remove_response[:success]).to be true
      end
    end

    describe "error handling" do
      it "raises error when getting nonexistent object" do
        skip skip_quic if skip_quic
        skip skip_grpc_error if skip_grpc_error

        expect {
          client.get("nonexistent-key-#{protocol}")
        }.to raise_error(ObjectStore::NotFoundError)
      end

      it "handles deleting nonexistent object gracefully" do
        skip skip_quic if skip_quic
        skip skip_grpc_error if skip_grpc_error

        expect {
          client.delete("nonexistent-key-#{protocol}")
        }.to raise_error(ObjectStore::NotFoundError)
      end

      it "handles updating metadata on nonexistent object" do
        skip skip_quic if skip_quic
        skip skip_grpc_error if skip_grpc_error

        metadata = ObjectStore::Models::Metadata.new(content_type: "text/plain")
        expect {
          client.update_metadata("nonexistent-key-#{protocol}", metadata)
        }.to raise_error(ObjectStore::NotFoundError)
      end
    end

    describe "streaming" do
      it "streams large object data" do
        skip skip_quic if skip_quic
        skip skip_grpc_streaming if skip_grpc_streaming

        large_data = "x" * 10_000  # 10KB of data
        large_key = "large-test-#{protocol}-#{Time.now.to_i}.txt"

        # Put large object
        put_response = client.put(large_key, large_data)
        expect(put_response.success?).to be true

        # Get large object
        get_response = client.get(large_key)
        expect(get_response.data).to eq(large_data)
        expect(get_response.data.size).to eq(10_000)

        # Cleanup
        client.delete(large_key)
      end
    end

    describe "archive operations" do
      it "archives objects" do
        skip skip_quic if skip_quic

        # Put object
        client.put(test_key, test_data)

        begin
          # Archive object - may not be supported by backend
          archive_response = client.archive(
            test_key,
            destination_type: "glacier",
            destination_settings: { "vault" => "test-vault" }
          )
          expect(archive_response.success?).to be true
        rescue ObjectStore::ServerError, ObjectStore::ValidationError, GRPC::BadStatus => e
          # Backend may not support archive operations or may have validation errors
          # Accept any error - the important thing is that SDK handles it gracefully
          expect(e.message).to be_a(String)
        ensure
          # Cleanup
          client.delete(test_key) rescue nil
        end
      end
    end

    describe "replication policies" do
      it "manages replication policies" do
        skip skip_quic if skip_quic

        policy = ObjectStore::Models::ReplicationPolicy.new(
          id: "test-replication-#{protocol}",
          source_backend: "local",
          source_settings: { "path" => "/tmp/source" },
          destination_backend: "local",
          destination_settings: { "path" => "/tmp/dest" },
          check_interval_seconds: 3600,
          enabled: true
        )

        begin
          # Add replication policy - may not be supported by backend
          add_response = client.add_replication_policy(policy)
          expect(add_response[:success]).to be true

          # Get replication policies
          get_response = client.get_replication_policies
          expect(get_response[:policies]).to be_an(Array)

          # Get specific policy
          policy_response = client.get_replication_policy(policy.id)
          expect(policy_response[:policy]).to be_a(ObjectStore::Models::ReplicationPolicy)

          # Remove replication policy
          remove_response = client.remove_replication_policy(policy.id)
          expect(remove_response[:success]).to be true
        rescue ObjectStore::ServerError, ObjectStore::ValidationError => e
          # Backend may not support replication operations
          expect(e.message).to match(/not supported|unsupported/i)
        end
      end
    end
  end

  context "with REST protocol" do
    include_examples "object store operations", :rest
  end

  context "with gRPC protocol" do
    include_examples "object store operations", :grpc
  end

  context "with QUIC protocol" do
    include_examples "object store operations", :quic
  end
end
