# frozen_string_literal: true

# E2E smoke spec: exercises the MCP and Unix transports against a live
# server. Skipped unless SMOKE_MCP_ADDR / SMOKE_UNIX_SOCK are set; launch a
# server with scripts/start-test-server.sh first (or use `make sdk-smoke`).

require "spec_helper"

RSpec.describe "e2e smoke", :e2e do
  mcp_addr = ENV.fetch("SMOKE_MCP_ADDR", "")
  unix_sock = ENV.fetch("SMOKE_UNIX_SOCK", "")

  before(:all) do
    skip "SMOKE_MCP_ADDR / SMOKE_UNIX_SOCK not set" if mcp_addr.empty? || unix_sock.empty?
  end

  def round_trip(name, client)
    key = "smoke/ruby/#{name}/obj.bin"
    payload = (+"\x00\x01hello from ruby #{name}\xFF\xFE").force_encoding("BINARY")

    expect(client.put(key, payload).success).to be(true)
    expect(client.exists?(key)).to be(true)

    got = client.get(key)
    expect(got.data.dup.force_encoding("BINARY")).to eq(payload)

    listing = client.list(prefix: "smoke/ruby/#{name}")
    expect(listing.objects.map(&:key)).to include(key)

    expect(client.delete(key).success).to be(true)
    expect(client.exists?(key)).to be(false)
  end

  it "mcp transport round trip" do
    host, port = mcp_addr.split(":")
    client = ObjectStore::Clients::McpClient.new(host: host, port: port.to_i)
    round_trip("mcp", client)
  end

  it "unix transport round trip" do
    client = ObjectStore::Clients::UnixClient.new(socket_path: unix_sock)
    round_trip("unix", client)
  end
end
