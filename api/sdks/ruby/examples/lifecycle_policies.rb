#!/usr/bin/env ruby

require_relative '../lib/objstore'

puts "Lifecycle Policies Example"
puts "=" * 50

client = ObjectStore::Client.new(protocol: :rest)

# Create a delete policy
puts "\nCreating delete policy for old logs..."
delete_policy = ObjectStore::Models::LifecyclePolicy.new(
  id: "delete-old-logs",
  prefix: "logs/",
  retention_seconds: 30 * 24 * 60 * 60, # 30 days
  action: "delete"
)

response = client.add_policy(delete_policy)
puts "Policy added: #{response[:success]}"

# Create an archive policy
puts "\nCreating archive policy for old data..."
archive_policy = ObjectStore::Models::LifecyclePolicy.new(
  id: "archive-old-data",
  prefix: "data/",
  retention_seconds: 90 * 24 * 60 * 60, # 90 days
  action: "archive",
  destination_type: "glacier",
  destination_settings: {
    "region" => "us-west-2",
    "vault" => "archive-vault"
  }
)

response = client.add_policy(archive_policy)
puts "Policy added: #{response[:success]}"

# List all policies
puts "\nListing all policies..."
response = client.get_policies
puts "Found #{response[:policies].size} policies:"
response[:policies].each do |policy|
  puts "\nPolicy: #{policy.id}"
  puts "  Prefix: #{policy.prefix}"
  puts "  Retention: #{policy.retention_seconds / 86400} days"
  puts "  Action: #{policy.action}"
  if policy.destination_type
    puts "  Destination: #{policy.destination_type}"
  end
end

# Apply policies
puts "\nApplying policies..."
response = client.apply_policies
puts "Applied #{response[:policies_count]} policies"
puts "Processed #{response[:objects_processed]} objects"

# Cleanup - remove policies
puts "\nRemoving policies..."
client.remove_policy("delete-old-logs")
client.remove_policy("archive-old-data")
puts "Policies removed"

puts "\nLifecycle policies example completed!"
