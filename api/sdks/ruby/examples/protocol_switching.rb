#!/usr/bin/env ruby

require_relative '../lib/objstore'

puts "Protocol Switching Example"
puts "=" * 50

# Start with REST
puts "\nUsing REST protocol..."
client = ObjectStore::Client.new(protocol: :rest)
puts "Current protocol: #{client.protocol}"

# Upload via REST
client.put("test-rest.txt", "Uploaded via REST")
puts "Uploaded via REST"

# Switch to gRPC
puts "\nSwitching to gRPC..."
client.switch_protocol(:grpc)
puts "Current protocol: #{client.protocol}"

# Upload via gRPC
client.put("test-grpc.txt", "Uploaded via gRPC")
puts "Uploaded via gRPC"

# Switch to QUIC
puts "\nSwitching to QUIC..."
client.switch_protocol(:quic)
puts "Current protocol: #{client.protocol}"

# Upload via QUIC
client.put("test-quic.txt", "Uploaded via QUIC")
puts "Uploaded via QUIC"

# List all test files
puts "\nListing all test files..."
client.switch_protocol(:rest) # Switch back to REST for listing
list = client.list(prefix: "test-")
list.objects.each do |obj|
  puts "  - #{obj.key}"
end

# Cleanup
puts "\nCleaning up..."
list.objects.each do |obj|
  client.delete(obj.key)
end

puts "\nProtocol switching example completed!"
