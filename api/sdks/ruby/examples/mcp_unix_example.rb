#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: MCP and Unix transport clients with app-layer authentication
#
# Prerequisites (server must be running with the appropriate transport enabled):
#
#   MCP server:   go-objstore --transport mcp --port 8081
#   Unix server:  go-objstore --transport unix --socket /tmp/objstore.sock
#
# This example can be run against a live server. When no server is available
# the relevant sections raise ObjectStore::ConnectionError and the example
# handles it gracefully.

require_relative "../lib/objstore"

puts "MCP and Unix transport example"
puts "=" * 50

# ---------------------------------------------------------------------------
# MCP transport — HTTP POST JSON-RPC 2.0 to port 8081
# ---------------------------------------------------------------------------

puts "\n[MCP] Connecting (port 8081)..."

mcp_client = ObjectStore::Client.new(
  protocol: :mcp,
  host: "localhost",
  port: 8081,
  # App-layer auth: supply a token and/or tenant_id as needed.
  # When not set the fields are omitted from every request.
  token: ENV.fetch("OBJSTORE_TOKEN", nil),
  tenant_id: ENV.fetch("OBJSTORE_TENANT", nil)
)

begin
  health = mcp_client.health
  puts "[MCP] Health: #{health.status}"

  puts "[MCP] Uploading object..."
  mcp_client.put("mcp-example.txt", "Hello from MCP transport!")

  puts "[MCP] Reading object back..."
  response = mcp_client.get("mcp-example.txt")
  puts "[MCP] Content: #{response.data}"

  puts "[MCP] Checking existence..."
  puts "[MCP] Exists: #{mcp_client.exists?('mcp-example.txt')}"

  puts "[MCP] Listing objects with prefix 'mcp-'..."
  list = mcp_client.list(prefix: "mcp-")
  list.objects.each { |obj| puts "[MCP]   #{obj.key}" }

  puts "[MCP] Streaming upload..."
  require "stringio"
  io = StringIO.new("Streamed content via MCP")
  mcp_client.put_stream("mcp-stream.txt", io,
                         metadata: ObjectStore::Models::Metadata.new(content_type: "text/plain"))

  puts "[MCP] Streaming download..."
  buf = String.new
  mcp_client.get_stream("mcp-stream.txt") { |chunk| buf << chunk }
  puts "[MCP] Streamed: #{buf}"

  puts "[MCP] Cleaning up..."
  mcp_client.delete("mcp-example.txt")
  mcp_client.delete("mcp-stream.txt")

  puts "[MCP] Done."
rescue ObjectStore::ConnectionError => e
  puts "[MCP] Server not available: #{e.message}"
rescue ObjectStore::Error => e
  puts "[MCP] Error: #{e.message}"
ensure
  mcp_client.close
end

# ---------------------------------------------------------------------------
# Unix socket transport — newline-delimited JSON-RPC 2.0 over a local socket
# ---------------------------------------------------------------------------

socket_path = ENV.fetch("OBJSTORE_SOCKET", "/tmp/objstore.sock")
puts "\n[Unix] Connecting (#{socket_path})..."

unix_client = ObjectStore::Client.new(
  protocol: :unix,
  socket_path: socket_path,
  # Auth is handled server-side via peer credentials; no token needed.
  timeout: 30
)

begin
  health = unix_client.health
  puts "[Unix] Health: #{health.status}"

  puts "[Unix] Uploading object..."
  unix_client.put("unix-example.txt", "Hello from Unix socket transport!")

  puts "[Unix] Reading object back..."
  response = unix_client.get("unix-example.txt")
  puts "[Unix] Content: #{response.data}"

  puts "[Unix] Checking existence..."
  puts "[Unix] Exists: #{unix_client.exists?('unix-example.txt')}"

  puts "[Unix] Listing with prefix 'unix-'..."
  list = unix_client.list(prefix: "unix-")
  list.objects.each { |obj| puts "[Unix]   #{obj.key}" }

  puts "[Unix] Streaming upload..."
  io = StringIO.new("Streamed content via Unix socket")
  unix_client.put_stream("unix-stream.txt", io)

  puts "[Unix] Streaming download..."
  buf = String.new
  unix_client.get_stream("unix-stream.txt") { |chunk| buf << chunk }
  puts "[Unix] Streamed: #{buf}"

  puts "[Unix] Metadata operations..."
  meta = ObjectStore::Models::Metadata.new(
    content_type: "text/plain",
    custom: { "source" => "unix-example" }
  )
  unix_client.update_metadata("unix-example.txt", meta)
  meta_res = unix_client.get_metadata("unix-example.txt")
  puts "[Unix] content_type: #{meta_res.metadata.content_type}"

  puts "[Unix] Cleaning up..."
  unix_client.delete("unix-example.txt")
  unix_client.delete("unix-stream.txt")

  puts "[Unix] Done."
rescue ObjectStore::ConnectionError => e
  puts "[Unix] Server not available: #{e.message}"
rescue ObjectStore::Error => e
  puts "[Unix] Error: #{e.message}"
ensure
  unix_client.close
end

# ---------------------------------------------------------------------------
# Protocol switching: REST -> MCP -> Unix at runtime
# ---------------------------------------------------------------------------

puts "\n[Switch] Protocol switching: REST -> MCP -> Unix"
multi = ObjectStore::Client.new(
  protocol: :rest,
  token: ENV.fetch("OBJSTORE_TOKEN", nil),
  tenant_id: ENV.fetch("OBJSTORE_TENANT", nil),
  socket_path: socket_path
)
puts "[Switch] Started with :#{multi.protocol}"

multi.switch_protocol(:mcp, port: 8081)
puts "[Switch] Switched to :#{multi.protocol}"

multi.switch_protocol(:unix)
puts "[Switch] Switched to :#{multi.protocol}"

multi.switch_protocol(:rest)
puts "[Switch] Back to :#{multi.protocol}"
multi.close

puts "\nMCP and Unix transport example completed!"
