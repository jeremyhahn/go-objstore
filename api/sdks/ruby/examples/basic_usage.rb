#!/usr/bin/env ruby

require_relative '../lib/objstore'

# Example 1: Basic PUT, GET, DELETE operations
puts "Example 1: Basic Operations"
puts "=" * 50

client = ObjectStore::Client.new(protocol: :rest)

# Upload an object
puts "\nUploading object..."
response = client.put("example.txt", "Hello from Ruby SDK!")
puts "Success: #{response.success?}"
puts "ETag: #{response.etag}"

# Check if exists
puts "\nChecking existence..."
exists = client.exists?("example.txt")
puts "Exists: #{exists}"

# Download the object
puts "\nDownloading object..."
response = client.get("example.txt")
puts "Content: #{response.data}"
puts "Size: #{response.metadata.size} bytes"

# Delete the object
puts "\nDeleting object..."
response = client.delete("example.txt")
puts "Deleted: #{response.success?}"

# Example 2: Working with metadata
puts "\n\nExample 2: Metadata Operations"
puts "=" * 50

metadata = ObjectStore::Models::Metadata.new(
  content_type: "application/json",
  custom: {
    "author" => "Ruby SDK Example",
    "version" => "1.0",
    "environment" => "development"
  }
)

puts "\nUploading with metadata..."
client.put("data.json", '{"key": "value"}', metadata)

puts "\nRetrieving metadata..."
meta_response = client.get_metadata("data.json")
puts "Content-Type: #{meta_response.metadata.content_type}"
puts "Custom metadata:"
meta_response.metadata.custom.each do |key, value|
  puts "  #{key}: #{value}"
end

puts "\nUpdating metadata..."
new_metadata = ObjectStore::Models::Metadata.new(
  content_type: "application/json",
  custom: { "version" => "2.0" }
)
client.update_metadata("data.json", new_metadata)

# Cleanup
client.delete("data.json")

# Example 3: Listing objects
puts "\n\nExample 3: Listing Objects"
puts "=" * 50

# Create some test objects
puts "\nCreating test objects..."
5.times do |i|
  client.put("test/file#{i}.txt", "Content #{i}")
end

puts "\nListing objects with prefix 'test/'..."
list = client.list(prefix: "test/")
puts "Found #{list.objects.size} objects:"
list.objects.each do |obj|
  puts "  - #{obj.key} (#{obj.metadata.size} bytes)"
end

# Cleanup
puts "\nCleaning up..."
list.objects.each do |obj|
  client.delete(obj.key)
end

puts "\nAll examples completed!"
