# -*- encoding: utf-8 -*-
require File.expand_path('../lib/lolitra/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Hugo Freire"]
  gem.email         = ["hfreire@abajar.com"]
  gem.description   = %q{Lolitra, build Sagas, a kind of Long Live Transaction (LLT), in less lines}
  gem.summary       = %q{Lolitra, build Sagas, a kind of Long Live Transaction (LLT), in less lines}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "lolitra"
  gem.require_paths = ["lib"]
  gem.version       = Lolitra::VERSION

  gem.add_development_dependency("rspec")
  gem.add_dependency("amqp", ">= 0.8.4")
  gem.add_dependency("json")
  gem.add_dependency("log4r")
  gem.add_dependency("faye")
end
