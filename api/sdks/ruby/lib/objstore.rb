require "faraday"
require "faraday/multipart"
require "json"
require "time"

require_relative "objstore/version"
require_relative "objstore/errors"
require_relative "objstore/models"
require_relative "objstore/clients/rest_client"
require_relative "objstore/clients/grpc_client"
require_relative "objstore/clients/quic_client"
require_relative "objstore/client"

module ObjectStore
  class << self
    attr_accessor :configuration

    def configure
      self.configuration ||= Configuration.new
      yield(configuration)
    end
  end

  class Configuration
    attr_accessor :host, :port, :protocol, :timeout, :use_ssl

    def initialize
      @host = "localhost"
      @port = 8080
      @protocol = :rest
      @timeout = 30
      @use_ssl = false
    end
  end
end
