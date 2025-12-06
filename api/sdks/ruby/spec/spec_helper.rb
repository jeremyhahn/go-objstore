require "simplecov"

SimpleCov.start do
  add_filter "/spec/"

  # Integration tests run fewer code paths, so we disable minimum coverage thresholds
  # when running integration tests only
  if ENV["INTEGRATION_TEST"] || ARGV.any? { |arg| arg.include?("integration") }
    # No minimum coverage for integration tests - they test real endpoints, not all code paths
  else
    minimum_coverage 90
    minimum_coverage_by_file 70
  end
end

require "bundler/setup"
require "objstore"
require "webmock/rspec"
require "json"

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.filter_run_when_matching :focus
  config.example_status_persistence_file_path = "spec/examples.txt"
  config.disable_monkey_patching!
  config.warnings = false

  if config.files_to_run.one?
    config.default_formatter = "doc"
  end

  config.order = :random
  Kernel.srand config.seed

  WebMock.disable_net_connect!(allow_localhost: false)
end
