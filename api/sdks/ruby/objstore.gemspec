Gem::Specification.new do |spec|
  spec.name          = "objstore"
  spec.version       = "0.1.0"
  spec.authors       = ["Go ObjectStore Team"]
  spec.email         = ["info@go-objstore.dev"]

  spec.summary       = "Ruby SDK for go-objstore"
  spec.description   = "A comprehensive Ruby SDK for go-objstore supporting REST, gRPC, and QUIC/HTTP3 protocols"
  spec.homepage      = "https://github.com/jeremyhahn/go-objstore"
  spec.license       = "AGPL-3.0"
  spec.required_ruby_version = ">= 2.7.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/jeremyhahn/go-objstore"

  spec.files = Dir.glob("{lib,spec}/**/*") + %w[README.md LICENSE Gemfile]
  spec.require_paths = ["lib"]

  # Core dependencies
  spec.add_dependency "faraday", "~> 2.0"
  spec.add_dependency "faraday-multipart", "~> 1.0"
  spec.add_dependency "grpc", "~> 1.60"
  spec.add_dependency "grpc-tools", "~> 1.60"
  spec.add_dependency "google-protobuf", "~> 3.25"

  # Development dependencies
  spec.add_development_dependency "rspec", "~> 3.12"
  spec.add_development_dependency "webmock", "~> 3.19"
  spec.add_development_dependency "simplecov", "~> 0.22"
  spec.add_development_dependency "rubocop", "~> 1.57"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "yard", "~> 0.9"
end
