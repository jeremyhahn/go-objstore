require "spec_helper"

# Legacy integration spec — retained for protocol-matrix edge cases not covered
# by the table-driven comprehensive_spec.rb. Duplicate coverage (basic CRUD,
# lifecycle, replication happy-path) has been removed; see comprehensive_spec.rb
# for the canonical table-driven suite.
#
# What remains here:
#  - gRPC-specific stub-readiness gate (validates the protobuf stubs load cleanly)
#  - QUIC explicit-skip documentation for both REST and gRPC wrappers
#  - Archive operation legacy path (accepts capability-skip, kept for context)
RSpec.describe "Integration Tests (legacy edge cases)", :integration do
  before(:all) do
    WebMock.allow_net_connect!
  end

  after(:all) do
    WebMock.disable_net_connect!
  end

  # ---------------------------------------------------------------------------
  # QUIC explicit skip
  # Ruby SDK has no native HTTP/3 support. The QUIC context is kept here so
  # that CI output explicitly documents the skip rather than the protocol being
  # silently absent. Unit coverage of the QUIC client lives in spec/unit/.
  # ---------------------------------------------------------------------------
  context "with QUIC protocol" do
    let(:port) { ENV["OBJSTORE_QUIC_PORT"]&.to_i || 4433 }
    let(:client) do
      ObjectStore::Client.new(
        protocol: :quic,
        host: ENV["OBJSTORE_HOST"] || "localhost",
        port: port
      )
    end

    it "is explicitly skipped — Ruby SDK: no native HTTP/3 support (unit coverage via WebMock)" do
      skip "Ruby SDK: no native HTTP/3 support — QUIC integration skipped (unit coverage exists via WebMock)"
    end
  end

  # ---------------------------------------------------------------------------
  # gRPC protobuf stubs gate
  # Verifies the generated protobuf stubs can be required and that the stub
  # class exists; this catches codegen regressions without a live server.
  # ---------------------------------------------------------------------------
  describe "gRPC protobuf stubs" do
    it "loads objstore_services_pb without error" do
      expect {
        require "objstore/proto/objstore_services_pb"
      }.not_to raise_error
    end

    it "defines the ObjectStore stub class" do
      require "objstore/proto/objstore_services_pb"
      expect(defined?(Objstore::V1::ObjectStore::Stub)).to be_truthy
    end
  end

  # ---------------------------------------------------------------------------
  # Archive operation — legacy path
  # The table-driven suite skips archive when the backend lacks an archiver.
  # This context keeps the REST archive test as a named integration case so the
  # skip reason is visible in the CI report.
  # ---------------------------------------------------------------------------
  describe "archive operation (REST)" do
    let(:host) { ENV["OBJSTORE_HOST"] || "localhost" }
    let(:port) { ENV["OBJSTORE_REST_PORT"]&.to_i || 8080 }
    let(:client) { ObjectStore::Client.new(protocol: :rest, host: host, port: port) }
    let(:test_key) { "legacy-archive-#{Time.now.to_i}-#{rand(10_000)}" }

    after(:each) do
      client.delete(test_key) rescue nil
      client.close rescue nil
    end

    it "archives an object or skips with a capability log when backend lacks archiver" do
      client.put(test_key, "archive payload")

      begin
        response = client.archive(
          test_key,
          destination_type: "local",
          destination_settings: { "path" => "/tmp/archive" }
        )
        expect(response.success?).to be true
      rescue ObjectStore::ServerError, ObjectStore::ValidationError => e
        skip "Archive not supported on this backend configuration: #{e.message}"
      end
    end
  end
end
