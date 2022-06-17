Gem::Specification.new do |s|
  s.name          = 'logstash-output-awslogs'
  s.version       = '1.1.1'
  s.licenses      = ['Apache-2.0']
  s.summary       = 'Writes events to AWS CloudWatch logs.'
  s.homepage      = 'https://github.com/Anarhyst266/logstash-output-awslogs'
  s.authors       = ['Anton Klyba']
  s.email         = 'anarhyst266+gems@gmail.com'
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", "~> 2.0"
  s.add_runtime_dependency "logstash-codec-plain"
  s.add_runtime_dependency "aws-sdk-cloudwatchlogs", '~> 1'
  s.add_runtime_dependency "logstash-integration-aws"
  s.add_runtime_dependency "json"
  s.add_development_dependency "logstash-devutils"
end
