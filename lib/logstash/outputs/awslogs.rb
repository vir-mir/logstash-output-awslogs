# frozen_string_literal: true

require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/plugin_mixins/aws_config'
require 'aws-sdk-cloudwatchlogs'
require 'time'


class LogStash::Outputs::Awslogs < LogStash::Outputs::Base
  include LogStash::PluginMixins::AwsConfig::V2

  config_name 'awslogs'
  default :codec, 'line'
  concurrency :shared

  PER_EVENT_OVERHEAD = 26
  MAX_BATCH_SIZE = 1024 * 1024
  MIN_DELAY = 0.2

  # Log group to send event to
  config :log_group_name, validate: :string, required: true
  # Logs stream to send event to
  config :log_stream_name, validate: :string, required: true
  # Message to be sent. Fields interpolation supported. By default the whole event will be sent as a json object
  config :message, validate: :string, default: ""

  public
  def register
    @client = Aws::CloudWatchLogs::Client.new(aws_options_hash)
    @last_flush = Time.now.to_f
  end # def register

  public
  def multi_receive(events)
    send_batches = form_event_batches(events)
    send_batches.each do |batch|
      put_events(batch)
    end
  end

  private
  def put_events(batch)
    begin
      delay = MIN_DELAY - (Time.now.to_f - @last_flush)
      sleep(delay) if delay > 0
      @client.put_log_events(
        {
          log_group_name: batch[:log_group],
          log_stream_name: batch[:log_stream],
          log_events: batch[:log_events]
        }
      )
    rescue Aws::CloudWatchLogs::Errors::ResourceNotFoundException
      @logger.info('AWSLogs: Will create log group/stream and retry')
      begin
        @client.create_log_group({log_group_name: batch[:log_group]})
      rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException
        @logger.info("AWSLogs: Log group #{batch[:log_group]} already exists")
      end
      begin
        @client.create_log_stream({log_group_name: batch[:log_group], log_stream_name: batch[:log_stream]})
      rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException
        @logger.info("AWSLogs: Log stream #{batch[:log_stream]} already exists")
      end
      retry
    end
    rescue Aws::CloudWatchLogs::Errors::LimitExceededException
      @logger.info("AWSLogs: Rate limit exceeded, retrying")
      retry
    end
    @last_flush = Time.now.to_f
  end

  private
  def form_event_batches(events_arr)
    batches = []
    events_by_stream_and_group = {}
    log_events = events_arr.sort_by { |event| event.timestamp.time.to_f}
    log_events.each do |event|
      event_log_stream_name = event.sprintf(log_stream_name)
      event_log_group_name = event.sprintf(log_group_name)

      sort_key = [event_log_group_name, event_log_stream_name]
      unless events_by_stream_and_group.keys.include? sort_key
        events_by_stream_and_group.store(sort_key, [])
      end
      if message.empty?
        events_by_stream_and_group[sort_key].push(
          timestamp: (event.timestamp.time.to_f * 1000).to_int,
          message: event.to_hash.sort.to_h.to_json
        )
      else
        events_by_stream_and_group[sort_key].push(
          timestamp: (event.timestamp.time.to_f * 1000).to_int,
          message: event.sprintf(message)
        )
      end
    end
    events_by_stream_and_group.each do |key, value|
      temp_batch = {
        log_group: key[0],
        log_stream: key[1],
        size: 0,
        log_events: []
      }
      value.each do |log_event|
        if log_event[:message].bytesize + PER_EVENT_OVERHEAD + temp_batch[:size] < MAX_BATCH_SIZE
          temp_batch[:size] += log_event[:message].bytesize + PER_EVENT_OVERHEAD
          temp_batch[:log_events] << log_event
        else
          batches << temp_batch
          temp_batch = {
            log_group: key[0],
            log_stream: key[1],
            size: 0,
            log_events: []
          }
          temp_batch[:size] += log_event[:message].bytesize + PER_EVENT_OVERHEAD
          temp_batch[:log_events] << log_event
        end
      end
      batches << temp_batch
    end
    batches
  end
end
