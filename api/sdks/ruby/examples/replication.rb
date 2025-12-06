#!/usr/bin/env ruby

require_relative '../lib/objstore'

puts "Replication Policies Example"
puts "=" * 50

client = ObjectStore::Client.new(protocol: :rest)

# Create a replication policy
puts "\nCreating replication policy..."
policy = ObjectStore::Models::ReplicationPolicy.new(
  id: "local-to-backup",
  source_backend: "local",
  source_settings: {
    "path" => "/data/source"
  },
  source_prefix: "important/",
  destination_backend: "local",
  destination_settings: {
    "path" => "/data/backup"
  },
  check_interval_seconds: 3600, # Check every hour
  enabled: true,
  replication_mode: "TRANSPARENT"
)

response = client.add_replication_policy(policy)
puts "Replication policy added: #{response[:success]}"

# Get all replication policies
puts "\nListing replication policies..."
response = client.get_replication_policies
puts "Found #{response[:policies].size} policies:"
response[:policies].each do |p|
  puts "\nPolicy: #{p.id}"
  puts "  Source: #{p.source_backend}"
  puts "  Destination: #{p.destination_backend}"
  puts "  Enabled: #{p.enabled}"
  puts "  Check interval: #{p.check_interval_seconds}s"
end

# Get specific policy
puts "\nGetting specific policy..."
response = client.get_replication_policy("local-to-backup")
policy = response[:policy]
puts "Policy ID: #{policy.id}"
puts "Mode: #{policy.replication_mode}"

# Trigger replication
puts "\nTriggering replication..."
response = client.trigger_replication(
  policy_id: "local-to-backup",
  parallel: true,
  worker_count: 4
)

if response[:success]
  result = response[:result]
  puts "Replication completed:"
  puts "  Synced: #{result[:synced]} objects"
  puts "  Deleted: #{result[:deleted]} objects"
  puts "  Failed: #{result[:failed]} objects"
  puts "  Bytes transferred: #{result[:bytes_total]} bytes"
  puts "  Duration: #{result[:duration_ms]}ms"
end

# Get replication status
puts "\nGetting replication status..."
response = client.get_replication_status("local-to-backup")
if response[:success]
  status = response[:status]
  puts "Status for policy: #{status.policy_id}"
  puts "  Total objects synced: #{status.total_objects_synced}"
  puts "  Total objects deleted: #{status.total_objects_deleted}"
  puts "  Total bytes synced: #{status.total_bytes_synced}"
  puts "  Total errors: #{status.total_errors}"
  puts "  Sync count: #{status.sync_count}"
  puts "  Average duration: #{status.average_sync_duration_ms}ms"
  puts "  Last sync: #{status.last_sync_time}"
end

# Cleanup
puts "\nRemoving replication policy..."
response = client.remove_replication_policy("local-to-backup")
puts "Policy removed: #{response[:success]}"

puts "\nReplication example completed!"
