#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative 'lib/objstore'

# Test ReplicationPolicy serialization
policy = ObjectStore::Models::ReplicationPolicy.new(
  id: "test-policy",
  source_backend: "local",
  source_settings: { "path" => "/tmp/source" },
  destination_backend: "local",
  destination_settings: { "path" => "/tmp/dest" },
  enabled: true
)

puts "ReplicationPolicy test:"
puts "  check_interval_seconds: #{policy.check_interval_seconds.inspect} (should be 3600)"
puts "  to_h: #{policy.to_h.inspect}"
puts "  to_json: #{policy.to_json}"

# Verify it has the default value
if policy.check_interval_seconds == 3600
  puts "\n✓ Default check_interval_seconds is correctly set to 3600"
else
  puts "\n✗ ERROR: check_interval_seconds is #{policy.check_interval_seconds.inspect}, expected 3600"
  exit 1
end

# Verify it's in the JSON output
json_hash = JSON.parse(policy.to_json)
if json_hash['check_interval_seconds'] == 3600
  puts "✓ check_interval_seconds is correctly included in JSON output"
else
  puts "✗ ERROR: check_interval_seconds not in JSON or wrong value: #{json_hash['check_interval_seconds'].inspect}"
  exit 1
end

# Test with explicit value
policy2 = ObjectStore::Models::ReplicationPolicy.new(
  id: "test-policy-2",
  source_backend: "local",
  source_settings: {},
  destination_backend: "s3",
  destination_settings: {},
  check_interval_seconds: 7200,
  enabled: false
)

if policy2.check_interval_seconds == 7200
  puts "✓ Explicit check_interval_seconds is correctly set to 7200"
else
  puts "✗ ERROR: check_interval_seconds is #{policy2.check_interval_seconds.inspect}, expected 7200"
  exit 1
end

puts "\n✓ All ReplicationPolicy tests passed!"
