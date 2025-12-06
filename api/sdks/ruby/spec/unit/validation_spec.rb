require "spec_helper"

RSpec.describe "Input Validation" do
  let(:client) { ObjectStore::Client.new(protocol: :rest, host: "localhost", port: 8080) }
  let(:base_url) { "http://localhost:8080" }

  describe "key validation" do
    it "rejects nil keys" do
      expect { client.get(nil) }.to raise_error(ArgumentError, /Key must be a non-empty string/)
      expect { client.put(nil, "data") }.to raise_error(ArgumentError, /Key must be a non-empty string/)
      expect { client.delete(nil) }.to raise_error(ArgumentError, /Key must be a non-empty string/)
      expect { client.exists?(nil) }.to raise_error(ArgumentError, /Key must be a non-empty string/)
    end

    it "rejects empty keys" do
      expect { client.get("") }.to raise_error(ArgumentError, /Key must be a non-empty string/)
      expect { client.put("", "data") }.to raise_error(ArgumentError, /Key must be a non-empty string/)
      expect { client.delete("") }.to raise_error(ArgumentError, /Key must be a non-empty string/)
      expect { client.exists?("") }.to raise_error(ArgumentError, /Key must be a non-empty string/)
    end

    it "rejects whitespace-only keys" do
      expect { client.get("   ") }.to raise_error(ArgumentError, /Key must be a non-empty string/)
      expect { client.put("  ", "data") }.to raise_error(ArgumentError, /Key must be a non-empty string/)
      expect { client.delete("\t\n") }.to raise_error(ArgumentError, /Key must be a non-empty string/)
    end

    it "accepts valid keys" do
      stub_request(:get, "#{base_url}/objects/valid-key.txt")
        .to_return(status: 200, body: "data", headers: { "content-type" => "text/plain" })

      expect { client.get("valid-key.txt") }.not_to raise_error
    end

    it "accepts keys with special characters" do
      stub_request(:get, "#{base_url}/objects/path%2Fto%2Ffile.txt")
        .to_return(status: 200, body: "data", headers: { "content-type" => "text/plain" })

      expect { client.get("path/to/file.txt") }.not_to raise_error
    end
  end

  describe "data validation" do
    it "rejects nil data" do
      expect { client.put("key.txt", nil) }.to raise_error(ArgumentError, /Data cannot be nil/)
    end

    it "accepts empty string data" do
      stub_request(:put, "#{base_url}/objects/empty.txt")
        .to_return(status: 201, body: { message: "success" }.to_json)

      expect { client.put("empty.txt", "") }.not_to raise_error
    end

    it "accepts string data" do
      stub_request(:put, "#{base_url}/objects/test.txt")
        .to_return(status: 201, body: { message: "success" }.to_json)

      expect { client.put("test.txt", "Hello World") }.not_to raise_error
    end

    it "accepts binary data" do
      stub_request(:put, "#{base_url}/objects/binary.bin")
        .to_return(status: 201, body: { message: "success" }.to_json)

      binary_data = [0xFF, 0xFE, 0xFD].pack("C*")
      expect { client.put("binary.bin", binary_data) }.not_to raise_error
    end
  end

  describe "IO validation" do
    it "rejects non-IO objects" do
      expect { client.put_stream("key.txt", "not an IO") }.to raise_error(ArgumentError, /IO object must respond to :read/)
      expect { client.put_stream("key.txt", 123) }.to raise_error(ArgumentError, /IO object must respond to :read/)
      expect { client.put_stream("key.txt", nil) }.to raise_error(ArgumentError, /IO object must respond to :read/)
    end

    it "accepts IO objects" do
      stub_request(:put, "#{base_url}/objects/io.txt")
        .to_return(status: 201, body: { message: "success" }.to_json)

      io = StringIO.new("IO data")
      expect { client.put_stream("io.txt", io) }.not_to raise_error
    end

    it "accepts File objects" do
      stub_request(:put, "#{base_url}/objects/file.txt")
        .to_return(status: 201, body: { message: "success" }.to_json)

      file_path = "/tmp/test_validation.txt"
      File.write(file_path, "File data")

      begin
        File.open(file_path, "rb") do |file|
          expect { client.put_stream("file.txt", file) }.not_to raise_error
        end
      ensure
        File.delete(file_path) if File.exist?(file_path)
      end
    end

    it "accepts objects that respond to :read" do
      stub_request(:put, "#{base_url}/objects/custom.txt")
        .to_return(status: 201, body: { message: "success" }.to_json)

      # Use StringIO which properly returns nil after EOF (unlike a simple object
      # whose read method always returns data, causing an infinite loop)
      custom_io = StringIO.new("custom data")

      expect { client.put_stream("custom.txt", custom_io) }.not_to raise_error
    end
  end

  describe "policy ID validation" do
    it "rejects nil policy IDs" do
      expect { client.remove_policy(nil) }.to raise_error(ArgumentError, /Policy ID must be a non-empty string/)
      expect { client.get_replication_policy(nil) }.to raise_error(ArgumentError, /Policy ID must be a non-empty string/)
      expect { client.remove_replication_policy(nil) }.to raise_error(ArgumentError, /Policy ID must be a non-empty string/)
      expect { client.get_replication_status(nil) }.to raise_error(ArgumentError, /Policy ID must be a non-empty string/)
    end

    it "rejects empty policy IDs" do
      expect { client.remove_policy("") }.to raise_error(ArgumentError, /Policy ID must be a non-empty string/)
      expect { client.get_replication_policy("") }.to raise_error(ArgumentError, /Policy ID must be a non-empty string/)
      expect { client.remove_replication_policy("") }.to raise_error(ArgumentError, /Policy ID must be a non-empty string/)
      expect { client.get_replication_status("") }.to raise_error(ArgumentError, /Policy ID must be a non-empty string/)
    end

    it "rejects whitespace-only policy IDs" do
      expect { client.remove_policy("   ") }.to raise_error(ArgumentError, /Policy ID must be a non-empty string/)
      expect { client.get_replication_policy("\t") }.to raise_error(ArgumentError, /Policy ID must be a non-empty string/)
    end

    it "accepts valid policy IDs" do
      stub_request(:delete, "#{base_url}/policies/valid-policy-123")
        .to_return(status: 200, body: { message: "deleted" }.to_json)

      expect { client.remove_policy("valid-policy-123") }.not_to raise_error
    end
  end

  describe "metadata operations validation" do
    it "validates key for get_metadata" do
      expect { client.get_metadata(nil) }.to raise_error(ArgumentError, /Key must be a non-empty string/)
      expect { client.get_metadata("") }.to raise_error(ArgumentError, /Key must be a non-empty string/)
    end

    it "validates key for update_metadata" do
      metadata = ObjectStore::Models::Metadata.new(content_type: "text/plain")
      expect { client.update_metadata(nil, metadata) }.to raise_error(ArgumentError, /Key must be a non-empty string/)
      expect { client.update_metadata("", metadata) }.to raise_error(ArgumentError, /Key must be a non-empty string/)
    end

    it "accepts valid metadata operations" do
      stub_request(:get, "#{base_url}/metadata/test.txt")
        .to_return(
          status: 200,
          body: {
            content_type: "text/plain",
            size: 100
          }.to_json
        )

      expect { client.get_metadata("test.txt") }.not_to raise_error
    end
  end

  describe "archive operations validation" do
    it "validates key for archive" do
      expect { client.archive(nil, destination_type: "glacier") }.to raise_error(ArgumentError, /Key must be a non-empty string/)
      expect { client.archive("", destination_type: "glacier") }.to raise_error(ArgumentError, /Key must be a non-empty string/)
    end

    it "accepts valid archive operations" do
      stub_request(:post, "#{base_url}/archive")
        .to_return(status: 200, body: { message: "archived" }.to_json)

      expect { client.archive("old-file.txt", destination_type: "glacier") }.not_to raise_error
    end
  end

  describe "block requirement for streaming" do
    it "requires block for get_stream" do
      expect { client.get_stream("test.txt") }.to raise_error(ArgumentError, /Block required for streaming/)
    end

    it "accepts get_stream with block" do
      stub_request(:get, "#{base_url}/objects/test.txt")
        .to_return(status: 200, body: "data", headers: { "content-type" => "text/plain" })

      expect { client.get_stream("test.txt") { |chunk| } }.not_to raise_error
    end
  end

  describe "protocol validation" do
    it "rejects invalid protocols" do
      expect { ObjectStore::Client.new(protocol: :invalid) }.to raise_error(ArgumentError, /Invalid protocol/)
      expect { ObjectStore::Client.new(protocol: "ftp") }.to raise_error(ArgumentError, /Invalid protocol/)
    end

    it "accepts valid protocols" do
      expect { ObjectStore::Client.new(protocol: :rest) }.not_to raise_error
      expect { ObjectStore::Client.new(protocol: :grpc) }.not_to raise_error
      expect { ObjectStore::Client.new(protocol: :quic) }.not_to raise_error
    end

    it "validates protocol in switch_protocol" do
      expect { client.switch_protocol(:invalid) }.to raise_error(ArgumentError, /Invalid protocol/)
    end
  end
end
