require "spec_helper"

RSpec.describe ObjectStore do
  describe ".configure" do
    after { described_class.configuration = nil }

    it "yields a Configuration with defaults and applies overrides" do
      described_class.configure do |config|
        expect(config.host).to eq("localhost")
        expect(config.port).to eq(8080)
        expect(config.protocol).to eq(:rest)
        expect(config.timeout).to eq(30)
        expect(config.use_ssl).to be(false)
        expect(config.token).to be_nil
        expect(config.headers).to eq({})
        expect(config.tenant_id).to be_nil
        expect(config.socket_path).to eq("/tmp/objstore.sock")

        config.host = "objstore.example.com"
        config.protocol = :grpc
      end

      expect(described_class.configuration.host).to eq("objstore.example.com")
      expect(described_class.configuration.protocol).to eq(:grpc)
    end

    it "reuses the existing configuration on subsequent calls" do
      described_class.configure { |config| config.port = 9999 }
      described_class.configure { |config| expect(config.port).to eq(9999) }
    end
  end
end
