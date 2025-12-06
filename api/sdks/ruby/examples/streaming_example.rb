#!/usr/bin/env ruby

require_relative "../lib/objstore"
require "stringio"

# Streaming Example - Demonstrating large file upload and download

# Initialize client
client = ObjectStore::Client.new(
  protocol: :rest,
  host: "localhost",
  port: 8080
)

puts "ObjectStore Streaming Example"
puts "=" * 50

# Example 1: Upload a file using streaming
puts "\n1. Uploading file using streaming..."

File.open(__FILE__, "rb") do |file|
  begin
    response = client.put_stream("streaming_example.rb", file,
      metadata: ObjectStore::Models::Metadata.new(
        content_type: "text/x-ruby"
      )
    )

    if response.success?
      puts "   ✓ File uploaded successfully"
      puts "   ETag: #{response.etag}"
    end
  rescue => e
    puts "   ✗ Upload failed: #{e.message}"
  end
end

# Example 2: Upload from StringIO
puts "\n2. Uploading from StringIO..."

large_content = "Line #{i}\n" * 10000
io = StringIO.new(large_content)

begin
  response = client.put_stream("large_text.txt", io,
    metadata: ObjectStore::Models::Metadata.new(
      content_type: "text/plain"
    )
  )

  if response.success?
    puts "   ✓ Large text uploaded successfully"
    puts "   Size: #{large_content.bytesize} bytes"
  end
rescue => e
  puts "   ✗ Upload failed: #{e.message}"
end

# Example 3: Download a file using streaming
puts "\n3. Downloading file using streaming..."

begin
  total_size = 0
  chunk_count = 0

  metadata = client.get_stream("large_text.txt") do |chunk|
    total_size += chunk.bytesize
    chunk_count += 1
    print "." if chunk_count % 10 == 0  # Progress indicator
  end

  puts "\n   ✓ File downloaded successfully"
  puts "   Total size: #{total_size} bytes"
  puts "   Chunks received: #{chunk_count}"
  puts "   Content type: #{metadata.content_type}"
rescue ObjectStore::NotFoundError
  puts "   ✗ File not found"
rescue => e
  puts "   ✗ Download failed: #{e.message}"
end

# Example 4: Download to file
puts "\n4. Downloading to file..."

output_path = "/tmp/downloaded_example.txt"

begin
  File.open(output_path, "wb") do |file|
    client.get_stream("large_text.txt") do |chunk|
      file.write(chunk)
    end
  end

  file_size = File.size(output_path)
  puts "   ✓ File saved to #{output_path}"
  puts "   Size: #{file_size} bytes"

  # Clean up
  File.delete(output_path)
rescue => e
  puts "   ✗ Download failed: #{e.message}"
ensure
  File.delete(output_path) if File.exist?(output_path)
end

# Example 5: Upload large binary file
puts "\n5. Uploading large binary data..."

# Generate random binary data
binary_data = Random.new.bytes(1_000_000)  # 1 MB
io = StringIO.new(binary_data)

begin
  response = client.put_stream("random.bin", io,
    metadata: ObjectStore::Models::Metadata.new(
      content_type: "application/octet-stream"
    ),
    chunk_size: 65536  # 64 KB chunks
  )

  if response.success?
    puts "   ✓ Binary data uploaded successfully"
    puts "   Size: #{binary_data.bytesize} bytes"
    puts "   ETag: #{response.etag}"
  end
rescue => e
  puts "   ✗ Upload failed: #{e.message}"
end

# Example 6: Stream processing with progress
puts "\n6. Streaming with progress tracking..."

begin
  total_size = 0
  last_progress = 0

  metadata = client.get_stream("random.bin", chunk_size: 65536) do |chunk|
    total_size += chunk.bytesize
    progress = (total_size / 1_000_000.0 * 100).to_i

    if progress != last_progress && progress % 10 == 0
      puts "   Progress: #{progress}%"
      last_progress = progress
    end
  end

  puts "   ✓ Download complete"
  puts "   Total: #{total_size} bytes"
rescue => e
  puts "   ✗ Download failed: #{e.message}"
end

# Example 7: Using different protocols
puts "\n7. Streaming with different protocols..."

protocols = [:rest, :grpc, :quic]

protocols.each do |protocol|
  puts "\n   Testing #{protocol.to_s.upcase} protocol:"

  begin
    # Create client for this protocol
    proto_client = ObjectStore::Client.new(
      protocol: protocol,
      host: "localhost"
    )

    # Small upload test
    io = StringIO.new("Test data for #{protocol}")
    response = proto_client.put_stream("test_#{protocol}.txt", io)

    if response.success?
      puts "   ✓ Upload succeeded"

      # Download test
      chunks = []
      proto_client.get_stream("test_#{protocol}.txt") { |chunk| chunks << chunk }
      puts "   ✓ Download succeeded (#{chunks.join.bytesize} bytes)"
    end
  rescue => e
    puts "   ✗ #{protocol} test failed: #{e.message}"
  end
end

puts "\n" + "=" * 50
puts "Streaming examples completed!"
puts "\nKey Benefits:"
puts "  • Memory efficient for large files"
puts "  • Progress tracking during transfer"
puts "  • Works with any IO object (File, StringIO, etc.)"
puts "  • Supports all protocols (REST, gRPC, QUIC)"
