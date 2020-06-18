# frozen_string_literal: true

require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/plugin_mixins/aws_config'
require 'aws-sdk'

Aws.eager_autoload!

# An awslogs output that does nothing.
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

  def multi_receive_encoded(events_and_encoded)
    to_send = {}
    sequence_tokens = {}

    events_and_encoded.each do |event, encoded|
      event_log_stream_name = event.sprintf(log_stream_name)
      event_log_group_name = event.sprintf(log_group_name)

      next_sequence_token_key = [event_log_group_name, event_log_stream_name]
      unless to_send.keys.include? next_sequence_token_key
        to_send.store(next_sequence_token_key, [])
      end
      if event.get('message') && !event.get('message').empty?
        to_send[next_sequence_token_key].push(
          timestamp: (event.timestamp.time.to_f * 1000).to_int,
          message: event.get('message')
        )
      else
        to_send[next_sequence_token_key].push(
            timestamp: (event.timestamp.time.to_f * 1000).to_int,
            message: encoded
        )
      end
    end

    group_names = []
    to_send.each do |event_log_names, _events|
      event_log_group_name = event_log_names[0]
      event_log_stream_name = event_log_names[1]
      next_sequence_token_key = [event_log_group_name, event_log_stream_name]
      next if @next_sequence_tokens.keys.include? next_sequence_token_key
      unless group_names.include? event_log_group_name
        group_names.push(event_log_group_name)
      end
    end

    group_names.each do |log_group_name|
      unless sequence_tokens.keys.include? log_group_name
        sequence_tokens.store(log_group_name, {})
      end
      begin
        @client.describe_log_streams({log_group_name: log_group_name}).each do |response|
          response.log_streams.each do |log_stream_data|
            unless log_stream_data.upload_sequence_token&.empty?
              sequence_tokens[log_group_name][log_stream_data.log_stream_name.to_s] = log_stream_data.upload_sequence_token.to_s
            end
          end
        end
      rescue Aws::CloudWatchLogs::Errors::ResourceNotFoundException => e
        @logger.info("Will create log group #{log_group_name} and retry")
        begin
          @client.create_log_group({log_group_name: log_group_name})
        rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException => e
          @logger.info("Log group #{log_group_name} already exists")
        end
        retry
      rescue Aws::CloudWatchLogs::Errors::ThrottlingException => e
        @logger.info('Describe streams throttling, retry')
        retry
      end
    end

    to_send.each do |event_log_names, log_events|
      event_log_group_name = event_log_names[0]
      event_log_stream_name = event_log_names[1]
      next_sequence_token_key = [event_log_group_name, event_log_stream_name]

      ident_opts = {
        log_group_name: event_log_group_name,
        log_stream_name: event_log_stream_name
      }
      send_opts = ident_opts.merge(
        log_events: log_events
      )

      if @next_sequence_tokens.keys.include? next_sequence_token_key
        send_opts[:sequence_token] = @next_sequence_tokens[next_sequence_token_key]
      elsif !sequence_tokens[event_log_group_name][event_log_stream_name]&.empty?
        send_opts[:sequence_token] = sequence_tokens[event_log_group_name][event_log_stream_name]
      else
        begin
          @client.create_log_stream(ident_opts)
        rescue Aws::CloudWatchLogs::Errors::ResourceNotFoundException => e
          @logger.info('Will create log group/stream and retry')
          begin
            @client.create_log_group({log_group_name: send_opts[:log_group_name]})
          rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException => e
            @logger.info("Log group #{send_opts[:log_group_name]} already exists")
          end
          begin
            @client.create_log_stream({log_group_name: send_opts[:log_group_name], log_stream_name: send_opts[:log_stream_name]})
          rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException => e
            @logger.info("Log stream #{send_opts[:log_stream_name]} already exists")
          end
          retry
        rescue Aws::CloudWatchLogs::Errors::ThrottlingException => e
          @logger.info('Creating log streams throttling, retry')
          retry
        rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException => e
          @logger.info("Log stream #{send_opts[:log_stream_name]} already exists")
        end
      end
      begin
        resp = @client.put_log_events(send_opts)
        @next_sequence_tokens.store(next_sequence_token_key, resp.next_sequence_token)
      rescue Aws::CloudWatchLogs::Errors::ResourceNotFoundException => e
        @logger.info('Will create log group/stream and retry')
        begin
          @client.create_log_group({log_group_name: send_opts[:log_group_name]})
        rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException => e
          @logger.info("Log group #{send_opts[:log_group_name]} already exists")
        end
        begin
          @client.create_log_stream({log_group_name: send_opts[:log_group_name], log_stream_name: send_opts[:log_stream_name]})
        rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException => e
          @logger.info("Log stream #{send_opts[:log_stream_name]} already exists")
        end
        retry
      rescue  Aws::CloudWatchLogs::Errors::InvalidSequenceTokenException => e
        @logger.info(e.code)
        response = @client.describe_log_streams({log_group_name: send_opts[:log_group_name], log_stream_name_prefix: send_opts[:log_stream_name]})
        response.log_streams.each do |stream_data|
          if stream_data.log_stream_name == send_opts[:log_stream_name]
            send_opts[:sequence_token] = stream_data.upload_sequence_token
            break
          end
        end
        retry
      rescue Aws::CloudWatchLogs::Errors::ThrottlingException => e
        @logger.info('Logs throttling, retry')
        retry
      end
    end
  end # def multi_receive_encodeds
end # class LogStash::Outputs::Awslogs
