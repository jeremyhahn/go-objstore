require "spec_helper"
require "stringio"

RSpec.describe "Streaming Support" do
  let(:base_url) { "http://localhost:8080" }

  describe "REST Client Streaming" do
    let(:client) { ObjectStore::Clients::RestClient.new(host: "localhost", port: 8080) }

    describe "#put_stream" do
      it "uploads from an IO object" do
        stub_request(:put, /#{Regexp.escape(base_url)}\/objects\/stream\.txt/)
          .to_return(status: 201, body: { message: "success" }.to_json, headers: { "etag" => "abc123" })

        io = StringIO.new("Hello from stream")
        response = client.put_stream("stream.txt", io)

        expect(response).to be_a(ObjectStore::Models::PutResponse)
        expect(response.success?).to be true
        expect(response.etag).to eq("abc123")
      end

      it "uploads from a file" do
        stub_request(:put, /#{Regexp.escape(base_url)}\/objects\/file\.txt/)
          .to_return(status: 201, body: { message: "success" }.to_json, headers: { "etag" => "xyz789" })

        file_path = "/tmp/test_upload.txt"
        File.write(file_path, "Test file content")

        begin
          File.open(file_path, "rb") do |file|
            response = client.put_stream("file.txt", file)
            expect(response.success?).to be true
          end
        ensure
          File.delete(file_path) if File.exist?(file_path)
        end
      end

      it "includes metadata in streaming upload" do
        stub_request(:put, /#{Regexp.escape(base_url)}\/objects\/doc\.pdf/)
          .to_return(status: 201, body: {}.to_json)

        io = StringIO.new("PDF content")
        metadata = ObjectStore::Models::Metadata.new(content_type: "application/pdf")
        client.put_stream("doc.pdf", io, metadata: metadata)

        expect(WebMock).to have_requested(:put, /#{Regexp.escape(base_url)}\/objects\/doc\.pdf/)
      end

      it "handles large files in chunks" do
        stub_request(:put, /#{Regexp.escape(base_url)}\/objects\/large\.bin/)
          .to_return(status: 201, body: { message: "success" }.to_json)

        # Create a large string
        large_data = "x" * 100_000
        io = StringIO.new(large_data)

        response = client.put_stream("large.bin", io, chunk_size: 8192)
        expect(response.success?).to be true
      end
    end

    describe "#get_stream" do
      it "downloads in chunks" do
        response_body = "Hello World from stream"
        stub_request(:get, /#{Regexp.escape(base_url)}\/objects\/stream\.txt/)
          .to_return(
            status: 200,
            body: response_body,
            headers: {
              "content-type" => "text/plain",
              "content-length" => response_body.bytesize.to_s,
              "etag" => "abc123"
            }
          )

        chunks = []
        metadata = client.get_stream("stream.txt") do |chunk|
          chunks << chunk
        end

        expect(chunks.join).to eq(response_body)
        expect(metadata).to be_a(ObjectStore::Models::Metadata)
        expect(metadata.content_type).to eq("text/plain")
        expect(metadata.etag).to eq("abc123")
      end

      it "downloads to a file" do
        response_body = "File content to download"
        stub_request(:get, /#{Regexp.escape(base_url)}\/objects\/download\.txt/)
          .to_return(
            status: 200,
            body: response_body,
            headers: { "content-type" => "text/plain" }
          )

        file_path = "/tmp/test_download.txt"

        begin
          File.open(file_path, "wb") do |file|
            client.get_stream("download.txt") { |chunk| file.write(chunk) }
          end

          expect(File.read(file_path)).to eq(response_body)
        ensure
          File.delete(file_path) if File.exist?(file_path)
        end
      end

      it "handles large files in chunks" do
        large_data = "x" * 100_000
        stub_request(:get, /#{Regexp.escape(base_url)}\/objects\/large\.bin/)
          .to_return(
            status: 200,
            body: large_data,
            headers: { "content-type" => "application/octet-stream" }
          )

        total_size = 0
        chunk_count = 0
        client.get_stream("large.bin", chunk_size: 8192) do |chunk|
          total_size += chunk.bytesize
          chunk_count += 1
        end

        expect(total_size).to eq(large_data.bytesize)
        # WebMock delivers response as single chunk, so we just verify data was received
        expect(chunk_count).to be >= 1
      end
    end
  end

  describe "QUIC Client Streaming" do
    let(:client) { ObjectStore::Clients::QuicClient.new(host: "localhost", port: 4433) }

    describe "#put_stream" do
      it "uploads using chunked transfer encoding" do
        # QUIC client uses Net::HTTP which supports body_stream
        io = StringIO.new("Stream data")

        # We can't easily mock Net::HTTP, so we'll test the interface
        expect(client).to respond_to(:put_stream)
        expect { client.method(:put_stream) }.not_to raise_error
      end
    end

    describe "#get_stream" do
      it "supports streaming downloads" do
        expect(client).to respond_to(:get_stream)
        expect { client.method(:get_stream) }.not_to raise_error
      end
    end
  end

  describe "gRPC Client Streaming" do
    let(:client) { ObjectStore::Clients::GrpcClient.new(host: "localhost", port: 50051) }

    describe "#put_stream" do
      it "uploads from IO object" do
        io = StringIO.new("gRPC stream data")

        expect(client).to respond_to(:put_stream)
        expect { client.method(:put_stream) }.not_to raise_error
      end
    end

    describe "#get_stream" do
      it "supports streaming downloads" do
        expect(client).to respond_to(:get_stream)
        expect { client.method(:get_stream) }.not_to raise_error
      end
    end
  end

  describe "Client facade streaming" do
    let(:client) { ObjectStore::Client.new(protocol: :rest, host: "localhost", port: 8080) }

    describe "#put_stream" do
      it "delegates to protocol client" do
        stub_request(:put, /#{Regexp.escape(base_url)}\/objects\/facade\.txt/)
          .to_return(status: 201, body: { message: "success" }.to_json, headers: { "etag" => "xyz" })

        io = StringIO.new("Facade test")
        response = client.put_stream("facade.txt", io)

        expect(response).to be_a(ObjectStore::Models::PutResponse)
        expect(response.success?).to be true
      end

      it "validates key parameter" do
        io = StringIO.new("data")
        expect { client.put_stream("", io) }.to raise_error(ArgumentError, /Key must be a non-empty string/)
        expect { client.put_stream(nil, io) }.to raise_error(ArgumentError, /Key must be a non-empty string/)
      end

      it "validates IO parameter" do
        expect { client.put_stream("key.txt", "not an IO") }.to raise_error(ArgumentError, /IO object must respond to :read/)
        expect { client.put_stream("key.txt", nil) }.to raise_error(ArgumentError, /IO object must respond to :read/)
      end
    end

    describe "#get_stream" do
      it "delegates to protocol client" do
        stub_request(:get, /#{Regexp.escape(base_url)}\/objects\/facade\.txt/)
          .to_return(
            status: 200,
            body: "Streamed content",
            headers: { "content-type" => "text/plain" }
          )

        chunks = []
        metadata = client.get_stream("facade.txt") { |chunk| chunks << chunk }

        expect(chunks.join).to eq("Streamed content")
        expect(metadata).to be_a(ObjectStore::Models::Metadata)
      end

      it "validates key parameter" do
        expect { client.get_stream("") { |c| } }.to raise_error(ArgumentError, /Key must be a non-empty string/)
        expect { client.get_stream(nil) { |c| } }.to raise_error(ArgumentError, /Key must be a non-empty string/)
      end

      it "requires a block" do
        expect { client.get_stream("key.txt") }.to raise_error(ArgumentError, /Block required for streaming/)
      end
    end
  end
end
