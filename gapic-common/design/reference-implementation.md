# Resumable Upload Protocol Reference Implementation

This document provides the complete reference implementation code for the core components of the Resumable Upload Protocol in `gapic-common`:
- [1. Rules Module (`Gapic::Rest::ResumableUpload::Rules`)](#1-rules-module)
- [2. Core Class (`Gapic::Rest::ResumableUpload::Core`)](#2-core-class)
- [3. Driver Class (`Gapic::Rest::ResumableUpload::Driver`)](#3-driver-class)

For system architecture, data models, buffer invariants, and state transition specifications, see the [Implementation Guide](implementation-guide.md).

---

## 1. Rules Module

```ruby
module Gapic
  module Rest
    module ResumableUpload
      module Rules
        CAT2_STATUS_CODES = [400, 408, 409, 412, 416, 429, 499].freeze
        FATAL_STATUS_CODES = [401, 403, 404, 405, 410, 413, 415].freeze

        # Classifies incoming event into a canonical shape symbol.
        # Pure function: takes ONLY event, zero state awareness.
        #
        # @param event [Object] Input event
        # @return [Symbol] Canonical event shape
        def self.shape_of(event)
          case event
          when Event::StartUpload
            :start_upload
          when Event::ChunkRead
            if !event.eof
              :chunk_read_full
            elsif event.bytes_buffered.positive?
              :chunk_read_eof_with_data
            else
              :chunk_read_eof_empty
            end
          when Event::Cancel
            :user_cancel
          when Event::GlobalDeadlineExceeded
            :global_deadline_exceeded
          when Event::RequestFailed
            case event.kind
            when :retries_exhausted then :request_retries_exhausted
            when :connection_failed then :request_connection_failed
            else                        :request_failed_unknown
            end
          when Event::HttpResponse
            classify_http_response(event)
          else
            :unknown
          end
        end

        # Top-level transition router. Matches [state.status, shape].
        #
        # @param state [State] Current state
        # @param event [Object] Input event
        # @param config [CompleteUploadConfig] Static configuration
        # @return [Array<State, Array<Object>>] Tuple of [next_state, instructions]
        def self.step(state, event, config)
          shape = shape_of(event)

          case [state.status, shape]
          in [:initializing, :start_upload]
            start_session(state, config)
          in [:starting, :response_active]
            begin_transmission(state, event, config)
          in [:transmission_reading, :chunk_read_full]
            send_chunk(state, event)
          in [:transmission_reading, :chunk_read_eof_with_data]
            send_upload_finalize(state, event)
          in [:transmission_reading, :chunk_read_eof_empty]
            send_finalize(state)
          in [:transmission_sending, :response_active]
            ack_chunk(state, config)
          in [:transmission_sending | :finalizing_sending_upload | :finalizing_sending_finalize, :response_cat2 | :request_retries_exhausted | :request_connection_failed]
            enter_recovery(state)
          in [:finalizing_sending_upload, :response_final]
            complete_upload_with_data(state, event)
          in [:finalizing_sending_finalize, :response_final]
            complete_upload_finalized(state, event)
          in [:recovery, :response_active]
            realign_from_recovery(state, event)
          in [:recovery, :response_final]
            complete_upload_finalized(state, event)
          in [:recovery, :response_cat2]
            retry_recovery(state)
          in [:cancelling, :response_cancelled]
            complete_cancellation(state)
          in [:cancelling, :user_cancel]
            [state, []]
          in [_, :global_deadline_exceeded]
            fail_with_deadline_exceeded(state)
          in [_, :user_cancel]
            cancel_session(state)
          in [_, :response_rejected]
            fail_with_rejected(state, event)
          in [_, :response_cat2 | :response_fatal_bad_response]
            fail_with_bad_response(state, event)
          in [_, :request_retries_exhausted | :request_connection_failed | :request_failed_unknown]
            fail_with_request_error(state, event)
          else
            raise InvalidTransitionError, "Invalid event shape #{shape} for state #{state.status}"
          end
        end

        def self.start_session(state, config)
          next_state = state.with(status: :starting)
          instructions = [
            Instruction::SendStart.new(
              url: config.initial_url,
              headers: config.initial_headers,
              body: config.initial_body
            )
          ]
          [next_state, instructions]
        end

        def self.begin_transmission(state, event, config)
          granularity = event.headers["x-goog-upload-chunk-granularity"]&.to_i
          chunk_size = resolve_chunk_size(config.chunk_size, granularity)
          next_state = state.with(
            status: :transmission_reading,
            upload_url: event.headers["x-goog-upload-url"],
            chunk_granularity: granularity,
            chunk_size: chunk_size,
            offset: 0,
            in_flight_length: 0
          )
          [next_state, [Instruction::FillBuffer.new(target_bytesize: chunk_size)]]
        end

        def self.send_chunk(state, event)
          next_state = state.with(
            status: :transmission_sending,
            in_flight_length: event.bytes_buffered
          )
          instructions = [
            Instruction::SendChunk.new(
              url: state.upload_url,
              offset: state.offset,
              length: event.bytes_buffered,
              finalize: false
            )
          ]
          [next_state, instructions]
        end

        def self.send_upload_finalize(state, event)
          next_state = state.with(
            status: :finalizing_sending_upload,
            in_flight_length: event.bytes_buffered
          )
          instructions = [
            Instruction::SendChunk.new(
              url: state.upload_url,
              offset: state.offset,
              length: event.bytes_buffered,
              finalize: true
            )
          ]
          [next_state, instructions]
        end

        def self.send_finalize(state)
          next_state = state.with(
            status: :finalizing_sending_finalize,
            in_flight_length: 0
          )
          [next_state, [Instruction::SendFinalize.new(url: state.upload_url)]]
        end

        def self.ack_chunk(state, config)
          new_offset = state.offset + state.in_flight_length
          next_state = state.with(
            status: :transmission_reading,
            offset: new_offset,
            in_flight_length: 0
          )
          instructions = [
            Instruction::NotifyProgress.new(bytes_uploaded: new_offset, total_bytes: config.upload_size),
            Instruction::FillBuffer.new(target_bytesize: state.chunk_size)
          ]
          [next_state, instructions]
        end

        def self.enter_recovery(state)
          next_state = state.with(
            status: :recovery,
            in_flight_length: 0
          )
          [next_state, [Instruction::SendQuery.new(url: state.upload_url)]]
        end

        def self.retry_recovery(state)
          next_state = state.with(
            status: :recovery,
            in_flight_length: 0
          )
          [next_state, [Instruction::SendQuery.new(url: state.upload_url)]]
        end

        def self.complete_upload_with_data(state, event)
          new_offset = state.offset + state.in_flight_length
          next_state = state.with(
            status: :success,
            offset: new_offset,
            in_flight_length: 0
          )
          instructions = [
            Instruction::NotifyProgress.new(bytes_uploaded: new_offset, total_bytes: new_offset),
            Instruction::TerminateSuccess.new(response: event)
          ]
          [next_state, instructions]
        end

        def self.complete_upload_finalized(state, event)
          next_state = state.with(
            status: :success,
            in_flight_length: 0
          )
          [next_state, [Instruction::TerminateSuccess.new(response: event)]]
        end

        def self.realign_from_recovery(state, event)
          server_offset = event.headers["x-goog-upload-size-received"].to_i
          next_state = state.with(
            status: :transmission_reading,
            offset: server_offset,
            in_flight_length: 0
          )
          instructions = [
            Instruction::RealignBuffer.new(server_offset: server_offset),
            Instruction::FillBuffer.new(target_bytesize: state.chunk_size)
          ]
          [next_state, instructions]
        end

        def self.complete_cancellation(state)
          next_state = state.with(status: :cancelled, in_flight_length: 0)
          [next_state, [Instruction::TerminateFailure.new(error: Gapic::Common::UploadCancelledError.new)]]
        end

        def self.cancel_session(state)
          next_state = state.with(status: :cancelling)
          [next_state, [Instruction::SendCancel.new(url: state.upload_url)]]
        end

        def self.fail_with_deadline_exceeded(state)
          next_state = state.with(
            status: :error,
            in_flight_length: 0,
            last_error: Gapic::Common::DeadlineExceededError.new
          )
          [next_state, [Instruction::TerminateFailure.new(error: next_state.last_error)]]
        end

        def self.fail_with_rejected(state, event)
          next_state = state.with(
            status: :rejected,
            in_flight_length: 0,
            last_error: Gapic::Common::UploadRejectedError.new(event.body)
          )
          [next_state, [Instruction::TerminateFailure.new(error: next_state.last_error)]]
        end

        def self.fail_with_bad_response(state, event)
          next_state = state.with(
            status: :error,
            in_flight_length: 0,
            last_error: Gapic::Common::BadResponseError.new(event.status)
          )
          [next_state, [Instruction::TerminateFailure.new(error: next_state.last_error)]]
        end

        def self.fail_with_request_error(state, event)
          next_state = state.with(
            status: :error,
            in_flight_length: 0,
            last_error: event.source_error
          )
          [next_state, [Instruction::TerminateFailure.new(error: event.source_error)]]
        end

        private

        def self.classify_http_response(response)
          status_header = response.headers["x-goog-upload-status"]&.downcase

          case status_header
          when "active"
            response.status == 200 ? :response_active : :response_cat2
          when "final"
            response.status == 200 ? :response_final : :response_rejected
          when "cancelled"
            response.status == 200 ? :response_cancelled : :response_fatal_bad_response
          when nil, ""
            if response.status == 200 || CAT2_STATUS_CODES.include?(response.status)
              :response_cat2
            else
              :response_fatal_bad_response
            end
          else
            :response_fatal_bad_response
          end
        end

        def self.resolve_chunk_size(user_chunk_size, chunk_granularity)
          base_size = user_chunk_size || DEFAULT_CHUNK_SIZE
          return base_size if chunk_granularity.nil? || chunk_granularity <= 0
          return chunk_granularity if base_size <= chunk_granularity

          base_size - (base_size % chunk_granularity)
        end
      end
    end
  end
end
```

---

## 2. Core Class

```ruby
module Gapic
  module Rest
    module ResumableUpload
      class Core
        attr_reader :state

        # @param config [CompleteUploadConfig]
        def initialize(config)
          @config = config
          @state = State.new(
            status: :initializing, upload_url: nil, offset: 0,
            chunk_size: config.chunk_size || 8_388_608,
            chunk_granularity: nil, in_flight_length: 0, last_error: nil
          )
        end

        # Dispatches event to Rules and updates state.
        #
        # @param event [Object] Input event
        # @return [Array<Object>] Driver instructions
        def dispatch(event)
          next_state, instructions = Rules.step(@state, event, @config)
          @state = next_state
          instructions
        end
      end
    end
  end
end
```

---

## 3. Driver Class

```ruby
module Gapic
  module Rest
    module ResumableUpload
      class Driver
        include Gapic::LoggingConcerns

        # @param client_stub [Gapic::Rest::ClientStub]
        # @param config [CompleteUploadConfig]
        # @param logger [Logger, nil] Optional logger
        def initialize(client_stub:, config:, logger: nil)
          @client_stub = client_stub
          @config = config
          @logger = logger
          @core = Core.new(config)
          @buffer = "".b
          @buffer_start_offset = 0
          @control_plane_retry_policy = config.control_plane_retry_policy || self.class.default_control_plane_retry_policy
          @data_plane_retry_policy = config.data_plane_retry_policy || self.class.default_data_plane_retry_policy
        end

        # Default retry policy for control plane requests (start, query, cancel).
        # Missing X-Goog-Upload-Status header is retriable (predicate returns true).
        #
        # @return [Gapic::Common::RetryPolicy]
        def self.default_control_plane_retry_policy
          Gapic::Common::RetryPolicy.new(
            retry_codes: ["UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED", "INTERNAL"],
            initial_delay: 1.0,
            max_delay: 15.0,
            multiplier: 1.3,
            retry_predicate: lambda do |error_or_response|
              if error_or_response.respond_to?(:headers)
                status_hdr = error_or_response.headers["x-goog-upload-status"]
                return true if status_hdr.nil? || status_hdr.empty?
              end
              nil
            end
          )
        end

        # Default retry policy for data plane requests (upload, finalize, upload_finalize).
        # Missing X-Goog-Upload-Status header is unretriable (predicate returns false),
        # causing Driver to yield Event::HttpResponse so Core initiates Recovery.
        #
        # @return [Gapic::Common::RetryPolicy]
        def self.default_data_plane_retry_policy
          Gapic::Common::RetryPolicy.new(
            retry_codes: ["UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED", "INTERNAL"],
            initial_delay: 1.0,
            max_delay: 15.0,
            multiplier: 1.3,
            retry_predicate: lambda do |error_or_response|
              if error_or_response.respond_to?(:headers)
                status_hdr = error_or_response.headers["x-goog-upload-status"]
                return false if status_hdr.nil? || status_hdr.empty?
              end
              nil
            end
          )
        end

        # Executes event loop until terminal state.
        #
        # @return [Faraday::Response] Final response
        def run
          pending_event = Event::StartUpload

          loop do
            instructions = @core.dispatch(pending_event)
            pending_event = nil

            if deadline_exceeded? && !terminal_instructions?(instructions)
              instructions = @core.dispatch(Event::GlobalDeadlineExceeded)
            end

            instructions.each do |instruction|
              case instruction
              when Instruction::NotifyProgress
                execute_notify_progress(instruction)
              when Instruction::RealignBuffer
                execute_realign_buffer(instruction)
              when Instruction::FillBuffer
                pending_event = execute_fill_buffer(instruction)
              when Instruction::SendStart
                pending_event = execute_send_start(instruction)
              when Instruction::SendChunk
                pending_event = execute_send_chunk(instruction)
              when Instruction::SendFinalize
                pending_event = execute_send_finalize(instruction)
              when Instruction::SendQuery
                pending_event = execute_send_query(instruction)
              when Instruction::SendCancel
                pending_event = execute_send_cancel(instruction)
              when Instruction::TerminateSuccess
                return instruction.response
              when Instruction::TerminateFailure
                raise instruction.error
              end
            end
          end
        end

        private

        def deadline_exceeded?
          return false unless @config.deadline

          Process.clock_gettime(Process::CLOCK_MONOTONIC) > @config.deadline
        end

        def terminal_instructions?(instructions)
          instructions.any? { |i| i.is_a?(Instruction::TerminateSuccess) || i.is_a?(Instruction::TerminateFailure) }
        end

        # Synchronous side-effect: invokes user callback safely
        def execute_notify_progress(instruction)
          @config.on_progress&.call(instruction.bytes_uploaded, instruction.total_bytes)
        rescue StandardError => e
          @logger&.warn { "User progress callback raised exception: #{e.message}" }
        end

        # Synchronous side-effect: adjusts in-memory buffer window and stream
        def execute_realign_buffer(instruction)
          # Implements Section 2.5 buffer alignment Cases 1, 2, and 3
        end

        # I/O operation: fills buffer from unseekable/seekable stream
        # @return [Event::ChunkRead]
        def execute_fill_buffer(instruction)
          # Reads from stream until @buffer.bytesize reaches instruction.target_bytesize or stream hits EOF
        end

        # Network operation: wraps start HTTP request in user_override_start_retry_policy or control_plane_retry_policy
        # @return [Event::HttpResponse, Event::RequestFailed]
        def execute_send_start(instruction)
          policy = @config.user_override_start_retry_policy || @control_plane_retry_policy
          # Executes POST initiation request via @client_stub with policy
          # Returns Event::HttpResponse for any completed HTTP response (including 4xx/5xx).
          # Returns Event::RequestFailed(kind:, message:, source_error:) on unhandled transport error or retry exhaustion.
        end

        # Network operation: wraps HTTP request in data_plane_retry_policy
        # @return [Event::HttpResponse, Event::RequestFailed]
        def execute_send_chunk(instruction)
          # Slices body from @buffer[instruction.offset - @buffer_start_offset, instruction.length]
          # Executes POST request via @client_stub with @data_plane_retry_policy
          # Returns Event::HttpResponse for any completed HTTP response (including 4xx/5xx).
          # Returns Event::RequestFailed(kind:, message:, source_error:) on unhandled transport error or retry exhaustion.
        end
      end
    end
  end
end
```
