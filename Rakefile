# 
# To change this template, choose Tools | Templates
# and open the template in the editor.
 

require 'rubygems'
require 'rake'
require 'rake/clean'
require 'rake/gempackagetask'
require 'rake/rdoctask'
require 'rake/testtask'
require 'spec'
require 'tempfile'

$version_suffix=""

spec = Gem::Specification.new do |s|
  s.name = 'reliable_queue'
  s.version = '0.0.1' + $version_suffix
  s.has_rdoc = false
  s.extra_rdoc_files = ['README', 'LICENSE']
  s.summary = 'Content storage provider for quanp-core.'
  s.description = s.summary
  s.author = 'Toshiyuki Terashita'
  s.email = 'Twitter: @suzumura_ss'
  s.files = %w(LICENSE README Rakefile) + Dir.glob("{bin,lib,spec}/**/*")
  s.require_path = "lib"
  # s.bindir = "bin"
  # s.executables = ['your_executable_here']
  #s.add_dependency('gem-name', ">=version")
end

Rake::GemPackageTask.new(spec) do |p|
  p.gem_spec = spec
  p.need_tar = true
  p.need_zip = true
end

Rake::RDocTask.new do |rdoc|
  files =['README', 'LICENSE', 'lib/**/*.rb']
  rdoc.rdoc_files.add(files)
  rdoc.main = "README" # page to start on
  rdoc.title = "content_provider Docs"
  rdoc.rdoc_dir = 'doc/rdoc' # rdoc output folder
  rdoc.options << '--line-numbers'
end

namespace :test do
  desc 'Run tests for rspec tests'
  task :spec do
    Dir.glob('spec/*_spec.rb').each {|t|
      puts "=== spec testing: #{t}"
      return false unless system("spec -c #{t}")
    }
  end

  desc 'Run tests for unit tests'
  Rake::TestTask.new(:unit) do |t|
    t.pattern = 'test/*_test.rb'
  end
  
  desc 'Run all the tests'
  task :all => [:spec, :unit]
end

desc 'Run all the tests'
task :test => 'test:all'

desc 'Run test task as default'
task :default => :test
