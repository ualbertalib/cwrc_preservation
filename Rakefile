require 'bundler/gem_tasks'
require 'rubocop/rake_task'

RSpec::Core::RakeTask.new do |r|
  r.verbose = false
end

RuboCop::RakeTask.new

task default: [:rubocop]
