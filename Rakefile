require 'rake/testtask'
require 'rubocop/rake_task'

RuboCop::RakeTask.new

Rake::TestTask.new(:test) do |t|
  t.test_files = FileList['*_test.rb']
end

task default: [:rubocop, :test]
