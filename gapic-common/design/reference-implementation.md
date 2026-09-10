# Resumable Upload Protocol Reference Implementation

This document provides the complete reference implementation code for the core components of the Resumable Upload Protocol in `gapic-common`:
- [1. Rules Module (`Gapic::Rest::ResumableUpload::Rules`)](#1-rules-module)
- [2. Core Class (`Gapic::Rest::ResumableUpload::Core`)](#2-core-class)
- [3. Driver Class (`Gapic::Rest::ResumableUpload::Driver`)](#3-driver-class)
- [4. Session Class (`Gapic::Rest::ResumableUpload::Session`)](#4-session-class)

For system architecture, data models, buffer invariants, and state transition specifications, see the [Implementation Guide](implementation-guide.md).

---

## 1. Rules Module

```ruby
module Gapic
  module Rest
    module ResumableUpload
      module Rules
        DEFAULT_CHUNK_SIZE = 8_388_608 # 8 MB
        CAT2_STATUS_CODES = [400, 408, 409, 412, 416, 429, 499].freeze
        FATAL_STATUS_CODES = [401, 403, 404, 405, 410, 413, 415].freeze

        STATE_DESCRIPTIONS = {
          initializing:                "initializing upload",
          starting:                    "initiating upload session",
          transmission_reading:        "reading chunk from stream",
          transmission_sending:        "sending a chunk of data",
          finalizing_sending_upload:   "sending final data chunk",
          finalizing_sending_finalize: "sending finalize command",
          recovery:                    "querying upload offset for recovery",
          cancelling:                  "cancelling upload session",
          success:                     "in completed upload state",
          cancelled:                   "in cancelled upload state",
          error:                       "in error state",
          rejected:                    "in rejected upload state"
        }.freeze

        RECIPES = [
          :start_session,
          :resume_session,
          :begin_transmission,
          :send_chunk,
          :send_upload_finalize,
          :send_finalize,
          :ack_chunk,
          :enter_recovery,
          :complete_upload_with_data,
          :complete_upload_finalized,
          :realign_from_recovery,
          :retry_recovery,
          :complete_cancellation,
          :ignore_duplicate_cancel,
          :cancel_session,
          :fail_with_deadline_exceeded,
          :fail_with_rejected,
          :fail_with_bad_response,
          :fail_with_request_error,
          :fail_with_unmatched_transition
        ].freeze

        # Classifies incoming event into a canonical shape symbol.
        # Pure function: takes ONLY event, zero state awareness.
        #
        # @param event [Object] Input event
        # @return [Symbol] Canonical event shape
        def self.shape_of(event)
          case event
          when Event::StartUpload, Event::StartUpload.singleton_class
            :start_upload
          when Event::ResumeUpload, Event::ResumeUpload.singleton_class
            :resume_upload
          when Event::ChunkRead
            classify_chunk_read(event)
          when Event::Cancel, Event::Cancel.singleton_class
            :user_cancel
          when Event::GlobalDeadlineExceeded, Event::GlobalDeadlineExceeded.singleton_class
            :global_deadline_exceeded
          when Event::RequestFailed
            classify_request_failed(event)
          when Event::HttpResponse
            classify_http_response(event)
          when Class
            classify_event_class(event)
          else
            :unknown
          end
        end

        # Top-level transition decision engine. Matches [state.status, shape].
        # Pure function: computes next immutable State and driver instructions.
        #
        # @param state [State] Current state
        # @param event [Object] Input event
        # @param config [CompleteUploadConfig, ResumeUploadConfig] Static configuration
        # @return [Decision] Decision snapshot
        def self.decide(state, event, config)
          shape = shape_of(event)

          recipe = case [state.status, shape]
                   in [:initializing, :start_upload]
                     :start_session
                   in [:initializing, :resume_upload]
                     :resume_session
                   in [:starting, :response_active]
                     :begin_transmission
                   in [:transmission_reading, :chunk_read_full]
                     :send_chunk
                   in [:transmission_reading, :chunk_read_eof_with_data]
                     :send_upload_finalize
                   in [:transmission_reading, :chunk_read_eof_empty]
                     :send_finalize
                   in [:transmission_sending, :response_active]
                     :ack_chunk
                   in [:transmission_sending | :finalizing_sending_upload | :finalizing_sending_finalize,
                       :response_cat2 | :request_connection_failed | :request_timeout]
                     :enter_recovery
                   in [:finalizing_sending_upload, :response_final]
                     :complete_upload_with_data
                   in [:finalizing_sending_finalize | :recovery, :response_final]
                     :complete_upload_finalized
                   in [:recovery, :response_active]
                     :realign_from_recovery
                   in [:recovery, :response_cat2]
                     :retry_recovery
                   in [:cancelling, :response_cancelled]
                     :complete_cancellation
                   in [:cancelling, :user_cancel]
                     :ignore_duplicate_cancel
                   in [_, :global_deadline_exceeded]
                     :fail_with_deadline_exceeded
                   in [_, :user_cancel]
                     :cancel_session
                   in [:starting | :transmission_sending | :finalizing_sending_upload |
                       :finalizing_sending_finalize | :recovery | :cancelling, :response_rejected]
                     :fail_with_rejected
                   in [:starting | :cancelling, :response_cat2] |
                      [:starting | :transmission_sending | :finalizing_sending_upload |
                       :finalizing_sending_finalize | :recovery | :cancelling, :response_fatal_bad_response]
                     :fail_with_bad_response
                   in [:starting | :transmission_sending | :finalizing_sending_upload |
                       :finalizing_sending_finalize | :recovery | :cancelling,
                       :request_retries_exhausted | :request_connection_failed | :request_timeout |
                       :request_failed_unknown]
                     :fail_with_request_error
                   else
                     :fail_with_unmatched_transition
                   end

          next_state, instructions = public_send(recipe, state, event, config)
          Decision.new(
            from_status:  state.status,
            shape:        shape,
            recipe:       recipe,
            next_state:   next_state,
            instructions: instructions
          )
        end

        def self.step(state, event, config)
          decision = decide(state, event, config)
          [decision.next_state, decision.instructions]
        end

        def self.start_session(state, _event, config)
          next_state = state.with(status: :starting)
          progress = Progress.new(phase: :initiating, bytes_uploaded: next_state.offset, total_bytes: config.upload_size)
          instructions = [
            Instruction::NotifyProgress.new(progress: progress),
            Instruction::SendStart.new(
              url:     config.initial_url,
              headers: config.initial_headers,
              body:    config.initial_body
            )
          ]
          [next_state, instructions]
        end

        def self.resume_session(state, _event, config)
          next_state = state.with(
            status:     :recovery,
            upload_url: config.upload_url,
            chunk_size: config.chunk_size,
            offset:     0
          )
          progress = Progress.new(
            phase:          :initiating,
            bytes_uploaded: 0,
            total_bytes:    config.upload_size
          )
          instructions = [
            Instruction::NotifyProgress.new(progress: progress),
            Instruction::SendQuery.new(url: config.upload_url)
          ]
          [next_state, instructions]
        end

        def self.begin_transmission(state, event, config)
          granularity_str = header_value(event.headers, "x-goog-upload-chunk-granularity")
          granularity = granularity_str&.to_i
          chunk_size = resolve_chunk_size(config.chunk_size, granularity)
          upload_url = header_value(event.headers, "x-goog-upload-url")
          next_state = state.with(
            status:            :transmission_reading,
            upload_url:        upload_url,
            chunk_granularity: granularity,
            chunk_size:        chunk_size,
            offset:            0,
            in_flight_length:  0
          )
          progress = Progress.new(phase: :uploading, bytes_uploaded: next_state.offset, total_bytes: config.upload_size)
          instructions = [
            Instruction::NotifyProgress.new(progress: progress),
            Instruction::FillBuffer.new(target_bytesize: chunk_size)
          ]
          [next_state, instructions]
        end

        def self.send_chunk(state, event, _config)
          next_state = state.with(
            status:           :transmission_sending,
            in_flight_length: event.bytes_buffered
          )
          instructions = [
            Instruction::SendChunk.new(
              url:      state.upload_url,
              offset:   state.offset,
              length:   event.bytes_buffered,
              finalize: false
            )
          ]
          [next_state, instructions]
        end

        def self.send_upload_finalize(state, event, config)
          next_state = state.with(
            status:           :finalizing_sending_upload,
            in_flight_length: event.bytes_buffered
          )
          progress = Progress.new(phase: :finalizing, bytes_uploaded: next_state.offset, total_bytes: config.upload_size)
          instructions = [
            Instruction::NotifyProgress.new(progress: progress),
            Instruction::SendChunk.new(
              url:      state.upload_url,
              offset:   state.offset,
              length:   event.bytes_buffered,
              finalize: true
            )
          ]
          [next_state, instructions]
        end

        def self.send_finalize(state, _event, config)
          next_state = state.with(
            status:           :finalizing_sending_finalize,
            in_flight_length: 0
          )
          progress = Progress.new(phase: :finalizing, bytes_uploaded: next_state.offset, total_bytes: config.upload_size)
          instructions = [
            Instruction::NotifyProgress.new(progress: progress),
            Instruction::SendFinalize.new(url: state.upload_url)
          ]
          [next_state, instructions]
        end

        def self.ack_chunk(state, _event, config)
          new_offset = state.offset + state.in_flight_length
          next_state = state.with(
            status:           :transmission_reading,
            offset:           new_offset,
            in_flight_length: 0
          )
          progress = Progress.new(phase: :uploading, bytes_uploaded: new_offset, total_bytes: config.upload_size)
          instructions = [
            Instruction::NotifyProgress.new(progress: progress),
            Instruction::RealignBuffer.new(server_offset: new_offset),
            Instruction::FillBuffer.new(target_bytesize: state.chunk_size)
          ]
          [next_state, instructions]
        end

        def self.enter_recovery(state, _event, config)
          next_state = state.with(
            status:           :recovery,
            in_flight_length: 0
          )
          progress = Progress.new(phase: :recovering, bytes_uploaded: next_state.offset, total_bytes: config.upload_size)
          instructions = [
            Instruction::NotifyProgress.new(progress: progress),
            Instruction::SendQuery.new(url: state.upload_url)
          ]
          [next_state, instructions]
        end

        def self.retry_recovery(state, _event, _config)
          next_state = state.with(
            status:           :recovery,
            in_flight_length: 0
          )
          [next_state, [Instruction::SendQuery.new(url: state.upload_url)]]
        end

        def self.complete_upload_with_data(state, event, _config)
          new_offset = state.offset + state.in_flight_length
          next_state = state.with(
            status:           :success,
            offset:           new_offset,
            in_flight_length: 0
          )
          progress = Progress.new(phase: :completed, bytes_uploaded: new_offset, total_bytes: new_offset)
          instructions = [
            Instruction::NotifyProgress.new(progress: progress),
            Instruction::TerminateSuccess.new(response: event)
          ]
          [next_state, instructions]
        end

        def self.complete_upload_finalized(state, event, _config)
          next_state = state.with(
            status:           :success,
            in_flight_length: 0
          )
          progress = Progress.new(phase: :completed, bytes_uploaded: next_state.offset, total_bytes: next_state.offset)
          instructions = [
            Instruction::NotifyProgress.new(progress: progress),
            Instruction::TerminateSuccess.new(response: event)
          ]
          [next_state, instructions]
        end

        def self.realign_from_recovery(state, event, config)
          server_offset_str = header_value(event.headers, "x-goog-upload-size-received")
          server_offset = server_offset_str.to_i
          next_state = state.with(
            status:           :transmission_reading,
            offset:           server_offset,
            in_flight_length: 0
          )
          progress = Progress.new(phase: :uploading, bytes_uploaded: server_offset, total_bytes: config.upload_size)
          instructions = [
            Instruction::NotifyProgress.new(progress: progress),
            Instruction::RealignBuffer.new(server_offset: server_offset),
            Instruction::FillBuffer.new(target_bytesize: state.chunk_size)
          ]
          [next_state, instructions]
        end

        def self.complete_cancellation(state, event, _config)
          err = UploadCancelledError.from(event)
          next_state = state.with(status: :cancelled, in_flight_length: 0, last_error: err)
          [next_state, [Instruction::TerminateFailure.new(error: err)]]
        end

        def self.ignore_duplicate_cancel(state, _event, _config)
          [state, []]
        end

        def self.cancel_session(state, _event, config)
          next_state = state.with(status: :cancelling)
          progress = Progress.new(phase: :cancelling, bytes_uploaded: next_state.offset, total_bytes: config.upload_size)
          instructions = [
            Instruction::NotifyProgress.new(progress: progress),
            Instruction::SendCancel.new(url: state.upload_url)
          ]
          [next_state, instructions]
        end

        def self.resume_handle_from(state)
          return nil if state.nil? || state.upload_url.nil? || [:rejected, :cancelled, :success].include?(state.status)

          ResumeHandle.new(upload_url: state.upload_url, chunk_size: state.chunk_size)
        end

        def self.fail_with_deadline_exceeded(state, _event, _config)
          handle = resume_handle_from(state)
          err = DeadlineExceededError.new(resume_handle: handle)
          next_state = state.with(
            status:           :error,
            in_flight_length: 0,
            last_error:       err
          )
          [next_state, [Instruction::TerminateFailure.new(error: err)]]
        end

        def self.fail_with_rejected(state, event, _config)
          handle = resume_handle_from(state)
          err = UploadRejectedError.from(event, resume_handle: handle)
          next_state = state.with(
            status:           :rejected,
            in_flight_length: 0,
            last_error:       err
          )
          [next_state, [Instruction::TerminateFailure.new(error: err)]]
        end

        def self.fail_with_bad_response(state, event, _config)
          handle = resume_handle_from(state)
          msg = "Unexpected response from server while #{STATE_DESCRIPTIONS[state.status]}"
          err = BadResponseError.new(msg, event.status, headers: event.headers, resume_handle: handle)
          next_state = state.with(
            status:           :error,
            in_flight_length: 0,
            last_error:       err
          )
          [next_state, [Instruction::TerminateFailure.new(error: err)]]
        end

        def self.fail_with_request_error(state, event, _config)
          handle = resume_handle_from(state)
          msg = "Request failed while #{STATE_DESCRIPTIONS[state.status]}: #{event.message}"
          err = RequestFailedError.new(msg, source_error: event.source_error, resume_handle: handle)
          next_state = state.with(
            status:           :error,
            in_flight_length: 0,
            last_error:       err
          )
          [next_state, [Instruction::TerminateFailure.new(error: err)]]
        end

        def self.fail_with_unmatched_transition(state, event, _config)
          err = InvalidTransitionError.new(state: state, event: event)
          next_state = state.with(
            status:           :error,
            in_flight_length: 0,
            last_error:       err
          )
          [next_state, [Instruction::TerminateFailure.new(error: err)]]
        end

        def self.resolve_chunk_size(requested_size, granularity)
          base_size = requested_size || DEFAULT_CHUNK_SIZE
          return base_size if granularity.nil? || !granularity.positive?

          (base_size / granularity) * granularity
        end

        def self.header_value(headers, key)
          return nil unless headers.is_a?(Hash)
          return headers[key] if headers.key?(key)

          target = key.downcase
          _, val = headers.find { |k, _| k.to_s.downcase == target }
          val
        end

        def self.classify_chunk_read(event)
          if !event.eof
            :chunk_read_full
          elsif event.bytes_buffered.positive?
            :chunk_read_eof_with_data
          else
            :chunk_read_eof_empty
          end
        end

        def self.classify_request_failed(event)
          case event.kind
          when :timeout           then :request_timeout
          when :retries_exhausted then :request_retries_exhausted
          when :connection_failed then :request_connection_failed
          else                         :request_failed_unknown
          end
        end

        def self.classify_http_response(event)
          case event.status
          when 200..299
            status_hdr = header_value(event.headers, "x-goog-upload-status")
            case status_hdr
            when "active"    then :response_active
            when "final"     then :response_final
            when "cancelled" then :response_cancelled
            else                  :response_fatal_bad_response
            end
          when *CAT2_STATUS_CODES
            status_hdr = header_value(event.headers, "x-goog-upload-status")
            if status_hdr.nil? || status_hdr.empty? || status_hdr == "active"
              :response_cat2
            else
              :response_fatal_bad_response
            end
          when *FATAL_STATUS_CODES
            :response_fatal_bad_response
          else
            :response_rejected
          end
        end

        def self.classify_event_class(klass)
          if klass <= Event::StartUpload
            :start_upload
          elsif klass <= Event::ResumeUpload
            :resume_upload
          elsif klass <= Event::Cancel
            :user_cancel
          elsif klass <= Event::GlobalDeadlineExceeded
            :global_deadline_exceeded
          else
            :unknown
          end
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
        attr_reader :state, :last_decision

        # @param config [CompleteUploadConfig, ResumeUploadConfig]
        def initialize(config)
          @config = config
          @last_decision = nil
          @state = State.new(
            status:            :initializing,
            upload_url:        nil,
            offset:            0,
            chunk_size:        config.chunk_size || Rules::DEFAULT_CHUNK_SIZE,
            chunk_granularity: nil,
            in_flight_length:  0,
            last_error:        nil
          )
        end

        # Dispatches event to Rules and updates state.
        #
        # @param event [Object] Input event
        # @return [Array<Object>] Driver instructions
        def dispatch(event)
          decision = Rules.decide(@state, event, @config)
          @state = decision.next_state
          @last_decision = decision
          decision.instructions
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

        # Minimum assumed upload throughput in bytes per second (1 MB/s)
        MIN_ASSUMED_THROUGHPUT = 1_048_576

        # Default base timeout in seconds (1 hour)
        BASE_TIMEOUT = 3_600

        attr_reader :core

        # @param client_stub [Gapic::Rest::ClientStub]
        # @param config [CompleteUploadConfig, ResumeUploadConfig]
        # @param core [Core, nil] Optional Core state machine (defaults to new Core with config)
        # @param logger [Logger, nil] Optional logger override
        def initialize(client_stub:, config:, core: nil, logger: nil)
          @client_stub = client_stub
          @config = config
          @core = core || Core.new(config)
          @buffer = "".b
          @buffer_start_offset = 0

          endpoint = client_stub.respond_to?(:endpoint) ? client_stub.endpoint : nil
          setup_logging(
            logger:      logger || (client_stub.respond_to?(:logger) ? client_stub.logger : nil),
            system_name: "gapic-common",
            service:     "ResumableUpload",
            endpoint:    endpoint,
            client_id:   client_stub.object_id
          )
          @upload_log = UploadLog.new(stub_logger, upload_id: "unstarted")

          @start_retry_policy = resolve_retry_policy(config.start_retry_policy, RetryPolicies::START_DEFAULTS)
          @control_plane_retry_policy = resolve_retry_policy(
            config.control_plane_retry_policy,
            RetryPolicies::CONTROL_PLANE_DEFAULTS
          )
          @data_plane_retry_policy = resolve_retry_policy(
            config.data_plane_retry_policy,
            RetryPolicies::DATA_PLANE_DEFAULTS
          )
        end

        def self.default_start_retry_policy
          RetryPolicies.default_start
        end

        def self.default_control_plane_retry_policy
          RetryPolicies.default_control_plane
        end

        def self.default_data_plane_retry_policy
          RetryPolicies.default_data_plane
        end

        # Returns current resume handle, or nil if initiation is pending or session is finalized.
        #
        # @return [ResumeHandle, nil]
        def resume_handle
          Rules.resume_handle_from(@core.state)
        end

        # Returns raw session upload URL.
        #
        # @return [String, nil]
        def upload_url
          @core.state.upload_url
        end

        # Executes event loop until terminal state.
        #
        # @return [String, Object] Final response body
        def run
          @upload_log = UploadLog.new(stub_logger, upload_id: LoggingConcerns.random_uuid4)
          @deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + resolve_timeout
          pending_event = initial_event

          loop do
            instructions = dispatch_event(pending_event)
            pending_event = nil

            if deadline_exceeded? && !terminal_instructions?(instructions)
              instructions = dispatch_event(Event::GlobalDeadlineExceeded.new)
            end

            instructions.each do |instruction|
              result = dispatch_instruction(instruction)
              pending_event = result if pending_event_type?(result)
              return result if instruction.is_a?(Instruction::TerminateSuccess)
            end
          end
        end

        private

        def initial_event
          if @config.is_a?(ResumeUploadConfig)
            Event::ResumeUpload.new
          else
            Event::StartUpload.new
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
          Process.clock_gettime(Process::CLOCK_MONOTONIC) >= @deadline
        end

        def remaining_time
          [@deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0.0].max
        end

        def terminal_instructions?(instructions)
          instructions.any? { |i| i.is_a?(Instruction::TerminateSuccess) || i.is_a?(Instruction::TerminateFailure) }
        end

        def dispatch_event(event)
          instructions = begin
            @core.dispatch(event)
          rescue InvalidTransitionError => e
            @upload_log.unmatched_transition(@core.state, event, e)
            raise
          end
          @upload_log.decision(@core.last_decision)
          @upload_log.lifecycle(@core.last_decision, @config)
          instructions
        end

        def dispatch_instruction(instruction)
          case instruction
          when Instruction::NotifyProgress then execute_notify_progress(instruction)
          when Instruction::RealignBuffer  then execute_realign_buffer(instruction)
          when Instruction::FillBuffer     then execute_fill_buffer(instruction)
          when Instruction::SendStart      then execute_send_start(instruction)
          when Instruction::SendChunk      then execute_send_chunk(instruction)
          when Instruction::SendFinalize   then execute_send_finalize(instruction)
          when Instruction::SendQuery      then execute_send_query(instruction)
          when Instruction::SendCancel     then execute_send_cancel(instruction)
          when Instruction::TerminateSuccess
            instruction.response.respond_to?(:body) ? instruction.response.body : instruction.response
          when Instruction::TerminateFailure then raise instruction.error
          end
        end

        def execute_notify_progress(instruction)
          @config.on_progress&.call(instruction.progress)
        end

        def execute_realign_buffer(instruction)
          server_offset = instruction.server_offset
          if @config.upload_size && server_offset > @config.upload_size
            raise StreamMismatchError.new(
              "Server reported offset #{server_offset} exceeds total upload size #{@config.upload_size}",
              resume_handle: resume_handle
            )
          end

          buffer_start = @buffer_start_offset
          buffer_end = @buffer_start_offset + @buffer.bytesize

          realign_case = if server_offset >= buffer_start && server_offset <= buffer_end
                           "within_buffer"
                         elsif server_offset < buffer_start
                           "rewind"
                         else
                           "fast_forward"
                         end

          unseekable = realign_case == "rewind" && !@config.stream.respond_to?(:seek)
          @upload_log.buffer_realign(
            realign_case, server_offset:  server_offset,
                          current_offset: buffer_start,
                          unseekable:     unseekable
          )

          if server_offset >= buffer_start && server_offset <= buffer_end
            realign_within_buffer(server_offset)
          elsif server_offset < buffer_start
            realign_rewind_stream(server_offset)
          else
            realign_fast_forward_stream(server_offset, buffer_end)
          end
        end

        def realign_within_buffer(server_offset)
          slice_index = server_offset - @buffer_start_offset
          @buffer = @buffer.byteslice(slice_index..-1) || "".b
          @buffer_start_offset = server_offset
        end

        def realign_rewind_stream(server_offset)
          unless @config.stream.respond_to?(:seek)
            raise UnseekableStreamError.new(
              "Cannot rewind unseekable stream to offset #{server_offset} (buffered from #{@buffer_start_offset})",
              resume_handle: resume_handle
            )
          end

          if @config.upload_size.nil? && @config.stream.respond_to?(:size) && server_offset > @config.stream.size
            raise StreamMismatchError.new(
              "Server reported offset #{server_offset} exceeds stream size #{@config.stream.size}",
              resume_handle: resume_handle
            )
          end

          @config.stream.seek(server_offset)
          @buffer = "".b
          @buffer_start_offset = server_offset
        end

        def realign_fast_forward_stream(server_offset, buffer_end)
          @buffer = "".b
          if @config.stream.respond_to?(:seek)
            if @config.upload_size.nil? && @config.stream.respond_to?(:size) && server_offset > @config.stream.size
              raise StreamMismatchError.new(
                "Server reported offset #{server_offset} exceeds stream size #{@config.stream.size}",
                resume_handle: resume_handle
              )
            end
            @config.stream.seek(server_offset)
          else
            needed_discard = server_offset - buffer_end
            while needed_discard.positive?
              chunk_len = [needed_discard, 65_536].min
              discarded = @config.stream.read(chunk_len)
              if discarded.nil? || discarded.empty?
                raise StreamMismatchError.new(
                  "Stream ended prematurely at offset #{server_offset - needed_discard} before reaching server offset #{server_offset}",
                  resume_handle: resume_handle
                )
              end
              needed_discard -= discarded.bytesize
            end
          end
          @buffer_start_offset = server_offset
        end

        def execute_fill_buffer(instruction)
          target = instruction.target_bytesize
          eof = false

          while @buffer.bytesize < target
            bytes_needed = target - @buffer.bytesize
            chunk = @config.stream.read(bytes_needed)
            if chunk.nil? || chunk.empty?
              eof = true
              break
            end
            @buffer << chunk.b
          end

          Event::ChunkRead.new(bytes_buffered: @buffer.bytesize, eof: eof)
        end

        def execute_send_start(instruction)
          policy = @start_retry_policy.dup.start!
          headers = start_headers(instruction)
          attempt = 1

          loop do
            return Event::GlobalDeadlineExceeded.new if deadline_exceeded?

            event = make_post_request(
              instruction.url, headers: headers, body: instruction.body,
                               retry_policy: policy, method_name: "ResumableUpload.start",
                               start_attempt: attempt
            )
            return event unless event.is_a?(Event::HttpResponse)

            status_hdr = Rules.header_value(event.headers, "x-goog-upload-status")
            return event unless status_hdr.nil? || status_hdr.empty?
            return event if Rules::FATAL_STATUS_CODES.include?(event.status)

            err = BadResponseError.new(
              "Missing X-Goog-Upload-Status header in start response",
              event.status,
              headers: event.headers
            )
            can_retry = policy.send(:retry_with_deadline?) && policy.call(event)
            unless can_retry
              if event.status == 200
                failed_event = Event::RequestFailed.new(
                  kind: :retries_exhausted, message: err.message, source_error: err
                )
                @upload_log.wire_failure(failed_event)
                return failed_event
              end
              return event
            end
            attempt += 1
          end
        end

        def start_headers(instruction)
          headers = { "X-Goog-Upload-Protocol" => "resumable", "X-Goog-Upload-Command" => "start" }
          headers["X-Goog-Upload-Header-Content-Type"] = @config.content_type if @config.content_type
          headers["X-Goog-Upload-Header-Content-Length"] = @config.upload_size.to_s if @config.upload_size
          headers.merge(instruction.headers || {})
        end

        def execute_send_chunk(instruction)
          headers = {
            "X-Goog-Upload-Command" => instruction.finalize ? "upload, finalize" : "upload",
            "X-Goog-Upload-Offset"  => instruction.offset.to_s,
            "Content-Type"          => @config.content_type || "application/octet-stream",
            "Content-Length"        => instruction.length.to_s
          }
          slice_index = instruction.offset - @buffer_start_offset
          body = @buffer.byteslice(slice_index, instruction.length)

          make_post_request(
            instruction.url, headers: headers, body: body,
                             retry_policy: @data_plane_retry_policy.dup.start!,
                             method_name: "ResumableUpload.upload"
          )
        end

        def execute_send_finalize(instruction)
          headers = {
            "X-Goog-Upload-Command" => "finalize",
            "X-Goog-Upload-Offset"  => @core.state.offset.to_s,
            "Content-Length"        => "0"
          }
          make_post_request(
            instruction.url, headers: headers, body: "",
                             retry_policy: @data_plane_retry_policy.dup.start!,
                             method_name: "ResumableUpload.finalize"
          )
        end

        def execute_send_query(instruction)
          headers = { "X-Goog-Upload-Command" => "query" }
          make_post_request(
            instruction.url, headers: headers, body: "",
                             retry_policy: @control_plane_retry_policy.dup.start!,
                             method_name: "ResumableUpload.query"
          )
        end

        def execute_send_cancel(instruction)
          headers = { "X-Goog-Upload-Command" => "cancel" }
          make_post_request(
            instruction.url, headers: headers, body: "",
                             retry_policy: @control_plane_retry_policy.dup.start!,
                             method_name: "ResumableUpload.cancel"
          )
        end

        def make_post_request(url, headers:, body:, retry_policy:, method_name: nil, start_attempt: 1)
          return Event::GlobalDeadlineExceeded.new if deadline_exceeded?

          options = {
            metadata:     headers,
            retry_policy: retry_policy,
            timeout:      request_timeout(retry_policy)
          }
          @upload_log.wire_send(
            method: "POST", url: url, headers: headers,
            start_attempt: start_attempt, body_size: body.to_s.bytesize, body: body
          )

          response = @client_stub.make_post_request(
            uri: url, body: body, params: {},
            options: options, method_name: method_name
          )
          event = Event::HttpResponse.new(status: response.status, headers: response.headers || {}, body: response.body)
          @upload_log.wire_receive(event)
          event
        rescue StandardError => e
          return Event::GlobalDeadlineExceeded.new if deadline_exceeded?

          event = rescue_request_error(e)
          if event.is_a?(Event::HttpResponse)
            @upload_log.wire_receive(event)
          else
            @upload_log.wire_failure(event)
          end
          event
        end

        def request_timeout(retry_policy)
          policy_timeout = retry_policy.respond_to?(:timeout) ? retry_policy.timeout : nil
          [remaining_time, policy_timeout].compact.min
        end

        def rescue_request_error(err)
          case err
          when Gapic::Rest::DeadlineExceededError
            Event::RequestFailed.new(kind: :timeout, message: err.message, source_error: err)
          when Gapic::Rest::Error
            if err.status_code
              Event::HttpResponse.new(
                status:  err.status_code,
                headers: err.headers || {},
                body:    err.message,
                error:   err
              )
            else
              Event::RequestFailed.new(kind: :connection_failed, message: err.message, source_error: err)
            end
          when Faraday::Error
            rescue_faraday_error(err)
          else
            Event::RequestFailed.new(kind: :connection_failed, message: err.message, source_error: err)
          end
        end

        def rescue_faraday_error(err)
          if err.response && err.response[:status]
            rest_err = Gapic::Rest::Error.wrap_faraday_error(err)
            Event::HttpResponse.new(
              status:  err.response[:status],
              headers: err.response[:headers] || {},
              body:    err.response[:body],
              error:   rest_err
            )
          elsif err.is_a?(Faraday::TimeoutError)
            Event::RequestFailed.new(kind: :timeout, message: err.message, source_error: err)
          elsif err.is_a?(Faraday::ConnectionFailed)
            Event::RequestFailed.new(kind: :connection_failed, message: err.message, source_error: err)
          else
            Event::RequestFailed.new(kind: :retries_exhausted, message: err.message, source_error: err)
          end
        end

        def resolve_retry_policy(value, defaults)
          case value
          when Gapic::Common::RetryPolicy
            value
          when Hash
            Gapic::Common::RetryPolicy.new(**value).apply_defaults(defaults)
          when nil
            Gapic::Common::RetryPolicy.new(**defaults)
          else
            raise ArgumentError, "Expected RetryPolicy, Hash, or nil, got #{value.class}"
          end
        end
      end
    end
  end
end
```

---

## 4. Session Class

```ruby
module Gapic
  module Rest
    module ResumableUpload
      class Session
        attr_reader :client_stub, :stream, :initial_url, :initial_body, :initial_headers,
                    :upload_size, :chunk_size, :content_type, :timeout, :start_retry_policy,
                    :control_plane_retry_policy, :data_plane_retry_policy, :on_progress, :logger

        def initialize(client_stub:,
                       stream:,
                       initial_url:,
                       initial_body: nil,
                       initial_headers: {},
                       upload_size: nil,
                       chunk_size: nil,
                       content_type: nil,
                       timeout: nil,
                       start_retry_policy: nil,
                       control_plane_retry_policy: nil,
                       data_plane_retry_policy: nil,
                       on_progress: nil,
                       logger: nil)
          @client_stub = client_stub
          @stream = stream
          @initial_url = initial_url
          @initial_body = initial_body
          @initial_headers = initial_headers || {}
          @upload_size = upload_size
          @chunk_size = chunk_size
          @content_type = content_type
          @timeout = timeout
          @start_retry_policy = start_retry_policy
          @control_plane_retry_policy = control_plane_retry_policy
          @data_plane_retry_policy = data_plane_retry_policy
          @on_progress = on_progress
          @logger = logger

          @mutex = Mutex.new
          @running = false
          @executed = false
          @upload_url = nil
          @last_driver = nil
        end

        # Returns the raw upload session URL if established.
        #
        # @return [String, nil]
        def upload_url
          @mutex.synchronize { upload_url_internal }
        end

        # Returns whether the session is bound to a server-side upload.
        #
        # @return [Boolean]
        def bound?
          @mutex.synchronize { bound_internal? }
        end

        # Returns the current ResumeHandle if the session is alive and resumable.
        #
        # @return [ResumeHandle, nil]
        def resume_handle
          @mutex.synchronize { resume_handle_internal }
        end

        # Returns whether a new session can resume the upload.
        # Completed uploads are not resumable (returns false).
        #
        # @return [Boolean]
        def resumable?
          !resume_handle.nil?
        end

        # Returns whether a run is currently executing.
        #
        # @return [Boolean]
        def running?
          @mutex.synchronize { @running }
        end

        # Starts a new upload session on the server.
        #
        # @return [String, Object] Final response body upon completion
        # @raise [SessionStateError] If already bound/executed or if a run is currently in progress
        def start
          driver = nil
          @mutex.synchronize do
            raise SessionStateError, "A run is already in progress for this session" if @running
            raise SessionStateError, "Session has already executed a run" if bound_internal?

            @executed = true
            @running = true
            config = build_start_config
            driver = Driver.new(client_stub: @client_stub, config: config, logger: @logger)
          end

          execute_run(driver)
        end

        # Resumes an upload session using one of two explicit keyword forms:
        # 1. `resume(upload_url:, chunk_size:)`: Resumes with explicit URL and chunk size.
        # 2. `resume(resume_handle:)`: Resumes via ResumeHandle.
        #
        # Precondition: stream must be positioned at byte 0.
        #
        # @param upload_url [String, nil] Explicit upload URL
        # @param chunk_size [Integer, nil] Explicit chunk size
        # @param resume_handle [ResumeHandle, nil] Explicit resume handle
        # @return [String, Object] Final response body upon completion
        # @raise [ArgumentError] If argument shape is invalid, target upload is missing, or stream.pos != 0
        # @raise [SessionStateError] If already bound/executed or if a run is currently in progress
        def resume(upload_url: nil, chunk_size: nil, resume_handle: nil)
          target_url, target_chunk_size = resolve_resume_args(
            upload_url:    upload_url,
            chunk_size:    chunk_size,
            resume_handle: resume_handle
          )

          driver = nil
          @mutex.synchronize do
            raise SessionStateError, "A run is already in progress for this session" if @running
            raise SessionStateError, "Session has already executed a run" if bound_internal?

            if @stream.respond_to?(:pos) && !@stream.pos.zero?
              raise ArgumentError, "Stream must be positioned at byte 0 to resume an upload (got pos #{@stream.pos})"
            end

            @executed = true
            @running = true
            config = build_resume_config(target_url, target_chunk_size)
            driver = Driver.new(client_stub: @client_stub, config: config, logger: @logger)
          end

          execute_run(driver)
        end

        private

        def upload_url_internal
          @upload_url || @last_driver&.upload_url
        end

        def bound_internal?
          @executed || !upload_url_internal.nil?
        end

        def resume_handle_internal
          @last_driver&.resume_handle
        end

        def build_start_config
          CompleteUploadConfig.new(
            initial_url:                @initial_url,
            initial_body:               @initial_body,
            initial_headers:            @initial_headers,
            stream:                     @stream,
            upload_size:                @upload_size,
            chunk_size:                 @chunk_size,
            content_type:               @content_type,
            timeout:                    @timeout,
            start_retry_policy:         @start_retry_policy,
            control_plane_retry_policy: @control_plane_retry_policy,
            data_plane_retry_policy:    @data_plane_retry_policy,
            on_progress:                @on_progress
          )
        end

        def build_resume_config(target_url, target_chunk_size)
          ResumeUploadConfig.new(
            upload_url:                 target_url,
            chunk_size:                 target_chunk_size,
            stream:                     @stream,
            upload_size:                @upload_size,
            content_type:               @content_type,
            timeout:                    @timeout,
            start_retry_policy:         @start_retry_policy,
            control_plane_retry_policy: @control_plane_retry_policy,
            data_plane_retry_policy:    @data_plane_retry_policy,
            on_progress:                @on_progress
          )
        end

        def resolve_resume_args(upload_url:, chunk_size:, resume_handle:)
          if resume_handle
            raise ArgumentError, "Cannot pass both resume_handle and upload_url/chunk_size" if upload_url || chunk_size
            [resume_handle.upload_url, resume_handle.chunk_size]
          elsif upload_url
            raise ArgumentError, "Must provide chunk_size with upload_url" if chunk_size.nil?
            [upload_url, chunk_size]
          elsif chunk_size
            raise ArgumentError, "Cannot pass chunk_size without upload_url"
          else
            raise ArgumentError, "Must provide either resume_handle or upload_url and chunk_size"
          end
        end

        def execute_run(driver)
          @mutex.synchronize { @last_driver = driver }
          result = driver.run
          @mutex.synchronize do
            @upload_url ||= driver.upload_url
            @running = false
          end
          result
        rescue StandardError
          @mutex.synchronize do
            @upload_url ||= driver.upload_url
            @running = false
          end
          raise
        end
      end
    end
  end
end
```
