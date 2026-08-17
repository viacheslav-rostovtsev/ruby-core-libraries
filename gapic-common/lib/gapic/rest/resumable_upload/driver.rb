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
      class Driver
        include Gapic::LoggingConcerns

        # @return [Core]
        attr_reader :core

        # @param client_stub [Gapic::Rest::ClientStub]
        # @param config [CompleteUploadConfig]
        # @param logger [Logger, nil] Optional logger
        def initialize client_stub:, config:, logger: nil
          @client_stub = client_stub
          @config = config
          @logger = logger
          @core = Core.new config
          @buffer = "".b
          @buffer_start_offset = 0
          @control_plane_retry_policy = config.control_plane_retry_policy ||
                                        self.class.default_control_plane_retry_policy
          @data_plane_retry_policy = config.data_plane_retry_policy ||
                                     self.class.default_data_plane_retry_policy
        end

        ##
        # Default retry policy for control plane requests (start, query, cancel).
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
        #
        # @return [Faraday::Response, Object] Final response
        def run
          pending_event = Event::StartUpload.new

          loop do
            instructions = @core.dispatch pending_event
            pending_event = nil

            if deadline_exceeded? && !terminal_instructions?(instructions)
              instructions = @core.dispatch Event::GlobalDeadlineExceeded.new
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
          obj.is_a?(Event::ChunkRead) || obj.is_a?(Event::HttpResponse) || obj.is_a?(Event::RequestFailed)
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
          when Instruction::TerminateSuccess then instruction.response
          when Instruction::TerminateFailure then raise instruction.error
          end
        end

        def deadline_exceeded?
          return false unless @config.deadline

          Process.clock_gettime(Process::CLOCK_MONOTONIC) > @config.deadline
        end

        def terminal_instructions? instructions
          instructions.any? do |i|
            i.is_a?(Instruction::TerminateSuccess) || i.is_a?(Instruction::TerminateFailure)
          end
        end

        def execute_notify_progress instruction
          @config.on_progress&.call instruction.bytes_uploaded, instruction.total_bytes
        rescue StandardError => e
          @logger&.warn { "User progress callback raised exception: #{e.message}" }
        end

        def execute_realign_buffer instruction
          server_offset = instruction.server_offset
          buffer_start = @buffer_start_offset
          buffer_end = @buffer_start_offset + @buffer.bytesize

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
          policy = @config.user_override_start_retry_policy || @control_plane_retry_policy
          headers = { "X-Goog-Upload-Protocol" => "resumable", "X-Goog-Upload-Command" => "start" }
          headers["X-Goog-Upload-Header-Content-Type"] = @config.content_type if @config.content_type
          headers["X-Goog-Upload-Header-Content-Length"] = @config.upload_size.to_s if @config.upload_size
          headers = headers.merge(instruction.headers || {})

          make_post_request instruction.url, headers: headers, body: instruction.body, retry_policy: policy
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

          make_post_request instruction.url, headers: headers, body: body, retry_policy: @data_plane_retry_policy
        end

        def execute_send_finalize instruction
          headers = {
            "X-Goog-Upload-Command" => "finalize",
            "X-Goog-Upload-Offset"  => @core.state.offset.to_s,
            "Content-Length"        => "0"
          }
          make_post_request instruction.url, headers: headers, body: "", retry_policy: @data_plane_retry_policy
        end

        def execute_send_query instruction
          headers = { "X-Goog-Upload-Command" => "query", "Content-Length" => "0" }
          make_post_request instruction.url, headers: headers, body: "", retry_policy: @control_plane_retry_policy
        end

        def execute_send_cancel instruction
          headers = { "X-Goog-Upload-Command" => "cancel", "Content-Length" => "0" }
          make_post_request instruction.url, headers: headers, body: "", retry_policy: @control_plane_retry_policy
        end

        def make_post_request url, headers:, body:, retry_policy:
          options = { metadata: headers, retry_policy: retry_policy.dup.start! }
          @logger&.debug do
            "ResumableUpload::Driver: POST #{url} (offset: #{@core.state.offset}, " \
              "chunk_size: #{@core.state.chunk_size})"
          end
          response = @client_stub.make_post_request uri: url, body: body, params: {}, options: options
          Event::HttpResponse.new status: response.status, headers: response.headers || {}, body: response.body
        rescue StandardError => e
          rescue_request_error e
        end

        def rescue_request_error err
          case err
          when Gapic::Rest::DeadlineExceededError
            Event::RequestFailed.new kind: :retries_exhausted, message: err.message, source_error: err
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
          elsif err.is_a?(Faraday::TimeoutError) || err.is_a?(Faraday::ConnectionFailed)
            Event::RequestFailed.new kind: :connection_failed, message: err.message, source_error: err
          else
            Event::RequestFailed.new kind: :retries_exhausted, message: err.message, source_error: err
          end
        end
      end
    end
  end
end
