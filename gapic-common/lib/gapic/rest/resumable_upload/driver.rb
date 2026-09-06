# frozen_string_literal: true

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "uri"
require "gapic/logging_concerns"
require "gapic/rest/error"
require "gapic/rest/resumable_upload/core"
require "gapic/rest/resumable_upload/data_types"
require "gapic/rest/resumable_upload/errors"
require "gapic/rest/resumable_upload/events"
require "gapic/rest/resumable_upload/instructions"
require "gapic/rest/resumable_upload/retry_policies"

module Gapic
  module Rest
    module ResumableUpload
      ##
      # Synchronous execution engine for the Resumable Upload Protocol.
      # Coordinates HTTP network operations, stream buffering, monotonic deadlines,
      # and delegates state transitions to Core.
      #
      # rubocop:disable Metrics/ClassLength
      class Driver
        include Gapic::LoggingConcerns

        # Minimum assumed upload throughput in bytes per second (1 MB/s)
        MIN_ASSUMED_THROUGHPUT = 1_048_576

        # Default base timeout in seconds (1 hour)
        BASE_TIMEOUT = 3_600

        # @return [Core]
        attr_reader :core

        # @return [String, nil] Current upload session ID
        attr_reader :upload_id

        # @param client_stub [Gapic::Rest::ClientStub]
        # @param config [CompleteUploadConfig]
        # @param logger [Logger, nil] Optional logger
        def initialize client_stub:, config:, logger: nil
          @client_stub = client_stub
          @config = config
          setup_logging logger: logger || (client_stub.respond_to?(:logger) ? client_stub.logger : nil),
                        service: "ResumableUpload",
                        endpoint: client_stub.respond_to?(:endpoint) ? client_stub.endpoint : nil,
                        client_id: client_stub.object_id
          @core = Core.new config
          @buffer = "".b
          @buffer_start_offset = 0
          @start_retry_policy = config.start_retry_policy ||
                                self.class.default_start_retry_policy
          @control_plane_retry_policy = config.control_plane_retry_policy ||
                                        self.class.default_control_plane_retry_policy
          @data_plane_retry_policy = config.data_plane_retry_policy ||
                                     self.class.default_data_plane_retry_policy
        end

        ##
        # Default retry policy for session initiation requests (start).
        #
        # @return [Gapic::Common::RetryPolicy]
        def self.default_start_retry_policy
          RetryPolicies.default_start
        end

        ##
        # Default retry policy for control plane requests (query, cancel).
        #
        # @return [Gapic::Common::RetryPolicy]
        def self.default_control_plane_retry_policy
          RetryPolicies.default_control_plane
        end

        ##
        # Default retry policy for data plane requests (upload, finalize).
        #
        # @return [Gapic::Common::RetryPolicy]
        def self.default_data_plane_retry_policy
          RetryPolicies.default_data_plane
        end

        ##
        # Executes event loop until terminal state.
        # Establishes a guaranteed monotonic deadline at the start of execution
        # using {#resolve_timeout} so the upload cannot stall indefinitely.
        #
        # @return [String, Object] Final response body
        def run
          @upload_id = LoggingConcerns.random_uuid4
          @deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + resolve_timeout
          pending_event = Event::StartUpload.new

          loop do
            instructions = @core.dispatch pending_event
            log_decision @core.last_decision
            log_lifecycle @core.last_decision
            pending_event = nil

            if deadline_exceeded? && !terminal_instructions?(instructions)
              instructions = @core.dispatch Event::GlobalDeadlineExceeded.new
              log_decision @core.last_decision
              log_lifecycle @core.last_decision
            end

            instructions.each do |instruction|
              result = dispatch_instruction instruction
              pending_event = result if pending_event_type? result
              return result if instruction.is_a? Instruction::TerminateSuccess
            end
          end
        end

        private

        def pending_event_type? obj
          obj.is_a?(Event::ChunkRead) || obj.is_a?(Event::HttpResponse) ||
            obj.is_a?(Event::RequestFailed) || obj.is_a?(Event::GlobalDeadlineExceeded)
        end

        def dispatch_instruction instruction
          case instruction
          when Instruction::NotifyProgress then execute_notify_progress instruction
          when Instruction::RealignBuffer then execute_realign_buffer instruction
          when Instruction::FillBuffer then execute_fill_buffer instruction
          when Instruction::SendStart then execute_send_start instruction
          when Instruction::SendChunk then execute_send_chunk instruction
          when Instruction::SendFinalize then execute_send_finalize instruction
          when Instruction::SendQuery then execute_send_query instruction
          when Instruction::SendCancel then execute_send_cancel instruction
          when Instruction::TerminateSuccess
            instruction.response.respond_to?(:body) ? instruction.response.body : instruction.response
          when Instruction::TerminateFailure then raise instruction.error
          end
        end

        def resolve_timeout
          return @config.timeout if @config.timeout&.positive?

          if @config.upload_size
            [@config.upload_size.fdiv(MIN_ASSUMED_THROUGHPUT), BASE_TIMEOUT].max
          else
            BASE_TIMEOUT
          end
        end

        def deadline_exceeded?
          return false unless @deadline

          Process.clock_gettime(Process::CLOCK_MONOTONIC) > @deadline
        end

        def terminal_instructions? instructions
          instructions.any? do |i|
            i.is_a?(Instruction::TerminateSuccess) || i.is_a?(Instruction::TerminateFailure)
          end
        end

        def execute_notify_progress instruction
          @config.on_progress&.call instruction.progress
        end

        def execute_realign_buffer instruction
          server_offset = instruction.server_offset
          buffer_start = @buffer_start_offset
          buffer_end = @buffer_start_offset + @buffer.bytesize

          realign_case = if server_offset >= buffer_start && server_offset <= buffer_end
                           "within_buffer"
                         elsif server_offset < buffer_start
                           "rewind"
                         else
                           "fast_forward"
                         end

          stub_logger.debug do |entry|
            entry.set_system_name
            entry.set_service
            entry.set "uploadId", @upload_id
            entry.set "realignCase", realign_case
            entry.set "offset", server_offset
            entry.set "bufferStart", buffer_start
            entry.set "bufferEnd", buffer_end
            entry.message = "Realigning buffer (#{realign_case}) to offset #{server_offset}"
          end

          if server_offset >= buffer_start && server_offset <= buffer_end
            realign_within_buffer server_offset
          elsif server_offset < buffer_start
            realign_rewind_stream server_offset
          else
            realign_fast_forward_stream server_offset, buffer_end
          end
        end

        def realign_within_buffer server_offset
          slice_index = server_offset - @buffer_start_offset
          @buffer = @buffer.byteslice(slice_index..-1) || "".b
          @buffer_start_offset = server_offset
        end

        def realign_rewind_stream server_offset
          unless @config.stream.respond_to? :seek
            stub_logger.warn do |entry|
              entry.set_system_name
              entry.set_service
              entry.set "uploadId", @upload_id
              entry.set "offset", server_offset
              entry.set "bufferStart", @buffer_start_offset
              entry.message = "Cannot rewind unseekable stream to offset #{server_offset} " \
                              "(buffered from #{@buffer_start_offset})"
            end
            raise UnseekableStreamError,
                  "Cannot rewind unseekable stream to offset #{server_offset} (buffered from #{@buffer_start_offset})"
          end

          @config.stream.seek server_offset
          @buffer = "".b
          @buffer_start_offset = server_offset
        end

        def realign_fast_forward_stream server_offset, buffer_end
          @buffer = "".b
          if @config.stream.respond_to? :seek
            @config.stream.seek server_offset
          else
            needed_discard = server_offset - buffer_end
            while needed_discard.positive?
              chunk = @config.stream.read [needed_discard, 65_536].min
              break if chunk.nil? || chunk.empty?

              needed_discard -= chunk.bytesize
            end
          end
          @buffer_start_offset = server_offset
        end

        def execute_fill_buffer instruction
          target = instruction.target_bytesize
          eof = false

          while @buffer.bytesize < target
            bytes_needed = target - @buffer.bytesize
            chunk = @config.stream.read bytes_needed
            if chunk.nil? || chunk.empty?
              eof = true
              break
            end
            @buffer << chunk.b
          end

          Event::ChunkRead.new bytes_buffered: @buffer.bytesize, eof: eof
        end

        def execute_send_start instruction
          policy = @start_retry_policy.dup.start!
          headers = start_headers instruction
          attempt = 1

          loop do
            return Event::GlobalDeadlineExceeded.new if deadline_exceeded?

            event = make_post_request instruction.url, headers: headers, body: instruction.body,
                                      retry_policy: policy, method_name: "ResumableUpload.start",
                                      retry_attempt: attempt
            return event unless event.is_a? Event::HttpResponse

            status_hdr = Rules.header_value event.headers, "x-goog-upload-status"
            return event unless status_hdr.nil? || status_hdr.empty?

            err = Gapic::Common::BadResponseError.new event.status,
                                                      "Missing X-Goog-Upload-Status header in start response"
            can_retry = policy.send(:retry_with_deadline?) && policy.call(event)
            unless can_retry
              failed_event = Event::RequestFailed.new kind: :retries_exhausted, message: err.message, source_error: err
              log_wire_failure failed_event, attempt
              return failed_event
            end
            attempt += 1
          end
        end

        def start_headers instruction
          headers = { "X-Goog-Upload-Protocol" => "resumable", "X-Goog-Upload-Command" => "start" }
          headers["X-Goog-Upload-Header-Content-Type"] = @config.content_type if @config.content_type
          headers["X-Goog-Upload-Header-Content-Length"] = @config.upload_size.to_s if @config.upload_size
          headers.merge(instruction.headers || {})
        end

        def execute_send_chunk instruction
          headers = {
            "X-Goog-Upload-Command" => instruction.finalize ? "upload, finalize" : "upload",
            "X-Goog-Upload-Offset"  => instruction.offset.to_s,
            "Content-Type"          => @config.content_type || "application/octet-stream",
            "Content-Length"        => instruction.length.to_s
          }
          slice_index = instruction.offset - @buffer_start_offset
          body = @buffer.byteslice slice_index, instruction.length

          make_post_request instruction.url, headers: headers, body: body,
                            retry_policy: @data_plane_retry_policy.dup.start!,
                            method_name: "ResumableUpload.upload"
        end

        def execute_send_finalize instruction
          headers = {
            "X-Goog-Upload-Command" => "finalize",
            "X-Goog-Upload-Offset"  => @core.state.offset.to_s,
            "Content-Length"        => "0"
          }
          make_post_request instruction.url, headers: headers, body: "",
                            retry_policy: @data_plane_retry_policy.dup.start!,
                            method_name: "ResumableUpload.finalize"
        end

        def execute_send_query instruction
          headers = { "X-Goog-Upload-Command" => "query", "Content-Length" => "0" }
          make_post_request instruction.url, headers: headers, body: "",
                            retry_policy: @control_plane_retry_policy.dup.start!,
                            method_name: "ResumableUpload.query"
        end

        def execute_send_cancel instruction
          headers = { "X-Goog-Upload-Command" => "cancel", "Content-Length" => "0" }
          make_post_request instruction.url, headers: headers, body: "",
                            retry_policy: @control_plane_retry_policy.dup.start!,
                            method_name: "ResumableUpload.cancel"
        end

        def make_post_request url, headers:, body:, retry_policy:, method_name: nil, retry_attempt: 1
          options = { metadata: headers, retry_policy: retry_policy }
          log_wire_send url, headers: headers, body: body, retry_attempt: retry_attempt

          response = @client_stub.make_post_request uri: url, body: body, params: {},
                                                    options: options, method_name: method_name
          event = Event::HttpResponse.new status: response.status, headers: response.headers || {}, body: response.body
          log_wire_receive event, retry_attempt
          event
        rescue StandardError => e
          event = rescue_request_error e
          if event.is_a? Event::HttpResponse
            log_wire_receive event, retry_attempt
          else
            log_wire_failure event, retry_attempt
          end
          event
        end

        def log_wire_send url, headers:, body:, retry_attempt:
          command = Rules.header_value headers, "x-goog-upload-command"
          offset = Rules.header_value headers, "x-goog-upload-offset"
          stub_logger.debug do |entry|
            entry.set_system_name
            entry.set_service
            entry.set "uploadId", @upload_id
            entry.set "command", command
            entry.set "url", abridge_url(url)
            entry.set "offset", offset.to_i if offset
            entry.set "length", body.to_s.bytesize
            entry.set "body", abridge_bytes(body)
            entry.set "headers", abridge_headers(headers)
            entry.set "retryAttempt", retry_attempt
            entry.message = "Sending #{command}"
          end
        end

        # rubocop:disable Metrics/AbcSize
        def log_wire_receive event, retry_attempt
          stub_logger.debug do |entry|
            entry.set_system_name
            entry.set_service
            entry.set "uploadId", @upload_id
            entry.set "httpStatus", event.status
            upload_status = Rules.header_value event.headers, "x-goog-upload-status"
            entry.set "uploadStatus", upload_status if upload_status
            size_recv = Rules.header_value event.headers, "x-goog-upload-size-received"
            entry.set "sizeReceived", size_recv.to_i if size_recv
            gran = Rules.header_value event.headers, "x-goog-upload-chunk-granularity"
            entry.set "granularity", gran.to_i if gran
            entry.set "headers", abridge_headers(event.headers)
            entry.set "body", event.status >= 400 ? abridge_error_body(event.body) : abridge_bytes(event.body)
            entry.set "retryAttempt", retry_attempt
            entry.message = "Received #{event.status}"
          end
        end
        # rubocop:enable Metrics/AbcSize

        def log_wire_failure event, retry_attempt
          stub_logger.debug do |entry|
            entry.set_system_name
            entry.set_service
            entry.set "uploadId", @upload_id
            entry.set "kind", event.kind
            entry.set "message", event.message
            entry.set "retryAttempt", retry_attempt
            entry.message = "Request Failed"
          end
        end

        # rubocop:disable Metrics/AbcSize
        def log_decision decision
          return unless decision

          stub_logger.debug do |entry|
            entry.set_system_name
            entry.set_service
            entry.set "uploadId", @upload_id
            entry.set "fromStatus", decision.from_status
            entry.set "shape", decision.shape
            entry.set "recipe", decision.recipe
            entry.set "toStatus", decision.next_state.status
            entry.set "offset", decision.next_state.offset
            entry.set "inFlightLength", decision.next_state.in_flight_length
            entry.set("instructions", decision.instructions.map { |i| summarize_instruction i })
            entry.message = "Rules: #{decision.from_status} + #{decision.shape} -> " \
                            "#{decision.recipe} -> #{decision.next_state.status}"
          end
        end
        # rubocop:enable Metrics/AbcSize

        # rubocop:disable Metrics/MethodLength,Metrics/AbcSize,Metrics/BlockLength
        def log_lifecycle decision
          return unless decision

          recipe = decision.recipe
          if recipe == :send_chunk
            stub_logger.debug do |entry|
              entry.set_system_name
              entry.set_service
              entry.set "uploadId", @upload_id
              entry.set "recipe", recipe
              entry.set "offset", decision.next_state.offset
              entry.set "length", decision.next_state.in_flight_length
              entry.message = "Sending upload chunk"
            end
            return
          end

          stub_logger.info do |entry|
            entry.set_system_name
            entry.set_service
            entry.set "uploadId", @upload_id
            entry.set "recipe", recipe

            case recipe
            when :start_session
              entry.set "uploadSize", @config.upload_size
              entry.set "requestedChunkSize", @config.chunk_size
              entry.message = "Initiating resumable upload"
            when :begin_transmission
              entry.set "effectiveChunkSize", decision.next_state.chunk_size
              entry.set "granularity", decision.next_state.chunk_granularity
              entry.set "uploadUrl", abridge_url(decision.next_state.upload_url)
              entry.message = "Upload session established"
            when :send_upload_finalize
              entry.set "offset", decision.next_state.offset
              entry.set "length", decision.next_state.in_flight_length
              entry.message = "Sending final upload chunk and finalizing"
            when :send_finalize
              entry.set "offset", decision.next_state.offset
              entry.message = "Finalizing upload session"
            when :enter_recovery
              entry.set "offset", decision.next_state.offset
              entry.message = "Entering upload recovery"
            when :retry_recovery
              entry.set "offset", decision.next_state.offset
              entry.message = "Retrying upload recovery query"
            when :realign_from_recovery
              entry.set "serverOffset", decision.next_state.offset
              entry.message = "Realigning upload offset from recovery"
            when :complete_upload_with_data, :complete_upload_finalized
              entry.set "bytesUploaded", decision.next_state.offset
              entry.message = "Resumable upload completed"
            when :cancel_session
              entry.message = "Cancelling resumable upload"
            when :complete_cancellation
              entry.message = "Resumable upload cancelled"
            else
              err_msg = decision.next_state.last_error&.to_s
              entry.set "error", err_msg if err_msg
              entry.message = "Resumable upload transition: #{recipe}"
            end
          end
        end
        # rubocop:enable Metrics/MethodLength,Metrics/AbcSize,Metrics/BlockLength

        # rubocop:disable Metrics/MethodLength
        def summarize_instruction instruction
          case instruction
          when Instruction::SendStart
            { "type" => "SendStart", "url" => abridge_url(instruction.url) }
          when Instruction::SendChunk
            {
              "type"     => "SendChunk",
              "url"      => abridge_url(instruction.url),
              "offset"   => instruction.offset,
              "length"   => instruction.length,
              "finalize" => instruction.finalize
            }
          when Instruction::SendFinalize
            { "type" => "SendFinalize", "url" => abridge_url(instruction.url) }
          when Instruction::SendQuery
            { "type" => "SendQuery", "url" => abridge_url(instruction.url) }
          when Instruction::SendCancel
            { "type" => "SendCancel", "url" => abridge_url(instruction.url) }
          when Instruction::RealignBuffer
            { "type" => "RealignBuffer", "serverOffset" => instruction.server_offset }
          when Instruction::FillBuffer
            { "type" => "FillBuffer", "targetBytesize" => instruction.target_bytesize }
          when Instruction::NotifyProgress
            {
              "type"          => "NotifyProgress",
              "bytesUploaded" => instruction.progress.bytes_uploaded,
              "totalBytes"    => instruction.progress.total_bytes
            }
          when Instruction::TerminateSuccess
            { "type" => "TerminateSuccess" }
          when Instruction::TerminateFailure
            { "type" => "TerminateFailure", "error" => instruction.error.to_s }
          else
            { "type" => instruction.class.name }
          end
        end
        # rubocop:enable Metrics/MethodLength

        def abridge_bytes data
          return "<empty>" if data.nil? || data.empty?
          return data if data.bytesize <= 64

          first_bytes = data.byteslice 0, 32
          "<#{data.bytesize} bytes; first 32: #{first_bytes}>"
        end

        def abridge_error_body data
          return "<empty>" if data.nil? || data.empty?

          data.to_s[0, 512]
        end

        def abridge_url url
          return nil if url.nil?

          uri = URI.parse url.to_s
          if uri.query && !uri.query.empty?
            elided = uri.query.split("&").map do |pair|
              key, _val = pair.split "=", 2
              "#{key}=<...>"
            end.join "&"
            uri.query = nil
            return "#{uri}?#{elided}"
          end
          uri.to_s
        rescue URI::InvalidURIError
          url.to_s
        end

        def abridge_headers headers
          return {} unless headers.is_a? Hash

          headers.each_with_object({}) do |(k, v), acc|
            key_str = k.to_s
            acc[key_str] = key_str.downcase.start_with?("x-goog-upload-") ? v : "<...>"
          end
        end

        def rescue_request_error err
          case err
          when Gapic::Rest::DeadlineExceededError
            Event::RequestFailed.new kind: :timeout, message: err.message, source_error: err
          when Gapic::Rest::Error
            if err.status_code
              Event::HttpResponse.new status: err.status_code, headers: err.headers || {}, body: err.message
            else
              Event::RequestFailed.new kind: :connection_failed, message: err.message, source_error: err
            end
          when Faraday::Error
            rescue_faraday_error err
          else
            Event::RequestFailed.new kind: :connection_failed, message: err.message, source_error: err
          end
        end

        def rescue_faraday_error err
          if err.response && err.response[:status]
            Event::HttpResponse.new(
              status:  err.response[:status],
              headers: err.response[:headers] || {},
              body:    err.response[:body]
            )
          elsif err.is_a? Faraday::TimeoutError
            Event::RequestFailed.new kind: :timeout, message: err.message, source_error: err
          elsif err.is_a? Faraday::ConnectionFailed
            Event::RequestFailed.new kind: :connection_failed, message: err.message, source_error: err
          else
            Event::RequestFailed.new kind: :retries_exhausted, message: err.message, source_error: err
          end
        end
      end
      # rubocop:enable Metrics/ClassLength
    end
  end
end
