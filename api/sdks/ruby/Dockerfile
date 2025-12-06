# Dockerfile for Ruby SDK testing
FROM ruby:3.3-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy Gemfile for dependency installation (from api/sdks/ruby context)
COPY sdks/ruby/Gemfile sdks/ruby/Gemfile.lock sdks/ruby/objstore.gemspec ./
COPY sdks/ruby/lib/objstore/version.rb ./lib/objstore/

# Install Ruby dependencies
RUN bundle config set --local frozen false && bundle install

# Copy SDK source code
COPY sdks/ruby/lib ./lib

# Copy proto files and generate gRPC stubs
RUN mkdir -p lib/objstore/proto
COPY proto/objstore.proto /tmp/objstore.proto
RUN grpc_tools_ruby_protoc -I /tmp --ruby_out=lib/objstore/proto \
    --grpc_out=lib/objstore/proto /tmp/objstore.proto && \
    sed -i "s/require 'objstore_pb'/require_relative 'objstore_pb'/" lib/objstore/proto/objstore_services_pb.rb

# Copy tests
COPY sdks/ruby/spec ./spec

# Default command runs unit tests
CMD ["bundle", "exec", "rspec", "spec/unit", "--format", "documentation"]
