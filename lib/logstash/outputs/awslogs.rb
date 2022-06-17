# frozen_string_literal: true

require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/plugin_mixins/aws_config'
require 'aws-sdk-cloudwatchlogs'
require 'json'

Aws.eager_autoload!

class LogStash::Outputs::Awslogs < LogStash::Outputs::Base
  include LogStash::PluginMixins::AwsConfig::V2

  config_name 'awslogs'
  default :codec, 'line'

  config :log_group_name, validate: :string, required: true
  config :log_stream_name, validate: :string, required: true

  public
  def register
    @client = Aws::CloudWatchLogs::Client.new(aws_options_hash)
    @next_sequence_tokens = {}
  end # def register

  public
  def multi_receive(events)
    to_send = {}

    events.each do |event|
      event_log_stream_name = event.sprintf(log_stream_name)
      event_log_group_name = event.sprintf(log_group_name)

      next_sequence_token_key = [event_log_group_name, event_log_stream_name]
      unless to_send.keys.include? next_sequence_token_key
        to_send.store(next_sequence_token_key, [])
      end
      to_send[next_sequence_token_key].push(
        timestamp: (event.timestamp.time.to_f * 1000).to_int,
        message: event.to_hash.sort.to_h.to_json
      )
    end


    to_send.each do |event_log_names, log_events|
      event_log_group_name = event_log_names[0]
      event_log_stream_name = event_log_names[1]
      next_sequence_token_key = [event_log_group_name, event_log_stream_name]

      log_events.sort_by!{ |event| event[:timestamp] }

      ident_opts = {
        log_group_name: event_log_group_name,
        log_stream_name: event_log_stream_name
      }
      send_opts = ident_opts.merge(
        log_events: log_events
      )
      if @next_sequence_tokens.keys.include? next_sequence_token_key
        send_opts[:sequence_token] = @next_sequence_tokens[next_sequence_token_key]
      end
      begin
        resp = @client.put_log_events(send_opts)
        @next_sequence_tokens.store(next_sequence_token_key, resp.next_sequence_token)
      rescue Aws::CloudWatchLogs::Errors::ResourceNotFoundException
        @logger.info('Will create log group/stream and retry')
        begin
          @client.create_log_group({log_group_name: send_opts[:log_group_name]})
        rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException
          @logger.info("Log group #{send_opts[:log_group_name]} already exists")
        end
        begin
          @client.create_log_stream({log_group_name: send_opts[:log_group_name], log_stream_name: send_opts[:log_stream_name]})
        rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException
          @logger.info("Log stream #{send_opts[:log_stream_name]} already exists")
        end
        retry
      rescue  Aws::CloudWatchLogs::Errors::InvalidSequenceTokenException => e
        send_opts[:sequence_token] = e.expected_sequence_token
        retry
      rescue Aws::CloudWatchLogs::Errors::ThrottlingException
        @logger.info('Logs throttling, retry')
        retry
      end
    end
  end # def multi_receive_encodeds
end # class LogStash::Outputs::Awslogs
