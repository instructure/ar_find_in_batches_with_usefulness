# coding: utf-8
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "ar_find_in_batches_with_usefulness/version"

Gem::Specification.new do |spec|
  spec.name          = "ar_find_in_batches_with_usefulness"
  spec.version       = ArFindInBatchesWithUsefulness::VERSION
  spec.authors       = ["AJ Williamks", "Cody Cutrer", "James Williams", "Jake Trent"]
  spec.email         = ["jaketrent@instructure.com"]
  spec.summary       = "find_in_batches that maintains order, group, distinct"
  spec.description   = "find_in_batches, now with usefulness!"
  spec.homepage      = "http://www.instructure.com"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "activerecord", ">= 3.2", "< 4.2"

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "pg", "~> 0.18.1"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.2.0"
end
