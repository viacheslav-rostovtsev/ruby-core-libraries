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

require "gapic/common/error"
require "gapic/rest/resumable_upload/errors"
require "gapic/rest/resumable_upload/data_types"
require "gapic/rest/resumable_upload/events"
require "gapic/rest/resumable_upload/instructions"

module Gapic
  module Rest
    module ResumableUpload
      ##
      # Pure functional transition engine for the Resumable Upload Protocol.
      # Contains zero side-effects and zero persistent state.
      #
      # rubocop:disable Metrics/ModuleLength
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

        ##
        # Classifies incoming event into a canonical shape symbol.
        #
        # @param event [Object] Input event
        # @return [Symbol] Canonical event shape
        def self.shape_of event
          case event
          when Event::StartUpload, Event::StartUpload.singleton_class
            :start_upload
          when Event::ChunkRead
            classify_chunk_read event
          when Event::Cancel, Event::Cancel.singleton_class
            :user_cancel
          when Event::GlobalDeadlineExceeded, Event::GlobalDeadlineExceeded.singleton_class
            :global_deadline_exceeded
          when Event::RequestFailed
            classify_request_failed event
          when Event::HttpResponse
            classify_http_response event
          when Class
            classify_event_class event
          else
            :unknown
          end
        end

        ##
        # Top-level transition router. Matches [state.status, shape].
        #
        # @param state [State] Current state
        # @param event [Object] Input event
        # @param config [CompleteUploadConfig] Static configuration
        # @return [Array<State, Array<Object>>] Tuple of [next_state, instructions]
        #
        # rubocop:disable Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity,Metrics/MethodLength
        def self.step state, event, config
          shape = shape_of event

          case [state.status, shape]
          in [:initializing, :start_upload]
            start_session state, config
          in [:starting, :response_active]
            begin_transmission state, event, config
          in [:transmission_reading, :chunk_read_full]
            send_chunk state, event
          in [:transmission_reading, :chunk_read_eof_with_data]
            send_upload_finalize state, event
          in [:transmission_reading, :chunk_read_eof_empty]
            send_finalize state
          in [:transmission_sending, :response_active]
            ack_chunk state, config
          in [:transmission_sending | :finalizing_sending_upload | :finalizing_sending_finalize,
              :response_cat2 | :request_connection_failed]
            enter_recovery state
          in [:finalizing_sending_upload, :response_final]
            complete_upload_with_data state, event
          in [:finalizing_sending_finalize | :recovery, :response_final]
            complete_upload_finalized state, event
          in [:recovery, :response_active]
            realign_from_recovery state, event
          in [:recovery, :response_cat2]
            retry_recovery state
          in [:cancelling, :response_cancelled]
            complete_cancellation state
          in [:cancelling, :user_cancel]
            [state, []]
          in [_, :global_deadline_exceeded]
            fail_with_deadline_exceeded state
          in [_, :user_cancel]
            cancel_session state
          in [:starting | :transmission_sending | :finalizing_sending_upload |
              :finalizing_sending_finalize | :recovery | :cancelling, :response_rejected]
            fail_with_rejected state, event
          in [:starting | :cancelling, :response_cat2] |
             [:starting | :transmission_sending | :finalizing_sending_upload |
              :finalizing_sending_finalize | :recovery | :cancelling, :response_fatal_bad_response]
            fail_with_bad_response state, event
          in [:starting | :transmission_sending | :finalizing_sending_upload |
              :finalizing_sending_finalize | :recovery | :cancelling,
              :request_retries_exhausted | :request_connection_failed | :request_failed_unknown]
            fail_with_request_error state, event
          else
            fail_with_unmatched_transition state, event
          end
        end
        # rubocop:enable Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity,Metrics/MethodLength

        def self.start_session state, config
          next_state = state.with status: :starting
          instructions = [
            Instruction::SendStart.new(
              url:     config.initial_url,
              headers: config.initial_headers,
              body:    config.initial_body
            )
          ]
          [next_state, instructions]
        end

        def self.begin_transmission state, event, config
          granularity_str = header_value event.headers, "x-goog-upload-chunk-granularity"
          granularity = granularity_str&.to_i
          chunk_size = resolve_chunk_size config.chunk_size, granularity
          upload_url = header_value event.headers, "x-goog-upload-url"
          next_state = state.with(
            status:            :transmission_reading,
            upload_url:        upload_url,
            chunk_granularity: granularity,
            chunk_size:        chunk_size,
            offset:            0,
            in_flight_length:  0
          )
          [next_state, [Instruction::FillBuffer.new(target_bytesize: chunk_size)]]
        end

        def self.send_chunk state, event
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

        def self.send_upload_finalize state, event
          next_state = state.with(
            status:           :finalizing_sending_upload,
            in_flight_length: event.bytes_buffered
          )
          instructions = [
            Instruction::SendChunk.new(
              url:      state.upload_url,
              offset:   state.offset,
              length:   event.bytes_buffered,
              finalize: true
            )
          ]
          [next_state, instructions]
        end

        def self.send_finalize state
          next_state = state.with(
            status:           :finalizing_sending_finalize,
            in_flight_length: 0
          )
          [next_state, [Instruction::SendFinalize.new(url: state.upload_url)]]
        end

        def self.ack_chunk state, config
          new_offset = state.offset + state.in_flight_length
          next_state = state.with(
            status:           :transmission_reading,
            offset:           new_offset,
            in_flight_length: 0
          )
          instructions = [
            Instruction::NotifyProgress.new(bytes_uploaded: new_offset, total_bytes: config.upload_size),
            Instruction::RealignBuffer.new(server_offset: new_offset),
            Instruction::FillBuffer.new(target_bytesize: state.chunk_size)
          ]
          [next_state, instructions]
        end

        def self.enter_recovery state
          next_state = state.with(
            status:           :recovery,
            in_flight_length: 0
          )
          [next_state, [Instruction::SendQuery.new(url: state.upload_url)]]
        end

        def self.retry_recovery state
          next_state = state.with(
            status:           :recovery,
            in_flight_length: 0
          )
          [next_state, [Instruction::SendQuery.new(url: state.upload_url)]]
        end

        def self.complete_upload_with_data state, event
          new_offset = state.offset + state.in_flight_length
          next_state = state.with(
            status:           :success,
            offset:           new_offset,
            in_flight_length: 0
          )
          instructions = [
            Instruction::NotifyProgress.new(bytes_uploaded: new_offset, total_bytes: new_offset),
            Instruction::TerminateSuccess.new(response: event)
          ]
          [next_state, instructions]
        end

        def self.complete_upload_finalized state, event
          next_state = state.with(
            status:           :success,
            in_flight_length: 0
          )
          [next_state, [Instruction::TerminateSuccess.new(response: event)]]
        end

        def self.realign_from_recovery state, event
          server_offset_str = header_value event.headers, "x-goog-upload-size-received"
          server_offset = server_offset_str.to_i
          next_state = state.with(
            status:           :transmission_reading,
            offset:           server_offset,
            in_flight_length: 0
          )
          instructions = [
            Instruction::RealignBuffer.new(server_offset: server_offset),
            Instruction::FillBuffer.new(target_bytesize: state.chunk_size)
          ]
          [next_state, instructions]
        end

        def self.complete_cancellation state
          err = Gapic::Common::UploadCancelledError.new
          next_state = state.with status: :cancelled, in_flight_length: 0, last_error: err
          [next_state, [Instruction::TerminateFailure.new(error: err)]]
        end

        def self.cancel_session state
          next_state = state.with status: :cancelling
          [next_state, [Instruction::SendCancel.new(url: state.upload_url)]]
        end

        def self.fail_with_deadline_exceeded state
          err = Gapic::Common::DeadlineExceededError.new
          next_state = state.with(
            status:           :error,
            in_flight_length: 0,
            last_error:       err
          )
          [next_state, [Instruction::TerminateFailure.new(error: err)]]
        end

        def self.fail_with_rejected state, event
          err = Gapic::Common::UploadRejectedError.new event.body
          next_state = state.with(
            status:           :rejected,
            in_flight_length: 0,
            last_error:       err
          )
          [next_state, [Instruction::TerminateFailure.new(error: err)]]
        end

        def self.fail_with_bad_response state, event
          err = Gapic::Common::BadResponseError.new event.status
          next_state = state.with(
            status:           :error,
            in_flight_length: 0,
            last_error:       err
          )
          [next_state, [Instruction::TerminateFailure.new(error: err)]]
        end

        def self.fail_with_request_error state, event
          err = event.source_error || Gapic::Common::Error.new(event.message || "Request failed")
          next_state = state.with(
            status:           :error,
            in_flight_length: 0,
            last_error:       err
          )
          [next_state, [Instruction::TerminateFailure.new(error: err)]]
        end

        def self.fail_with_unmatched_transition state, event
          shape = shape_of event
          action = STATE_DESCRIPTIONS[state.status] || "processing #{state.status}"
          happened = describe_event event, shape
          message = "Resumable upload failed while #{action}: #{happened}."
          response = event.is_a?(Event::HttpResponse) ? event : nil
          raise InvalidTransitionError.new(message, state: state.status, event: event, response: response)
        end

        def self.describe_event event, shape
          case event
          when Event::HttpResponse
            upload_status = event.headers["x-goog-upload-status"] || event.headers["X-Goog-Upload-Status"]
            status_desc = upload_status ? "'#{upload_status}'" : "missing"
            "received an unexpected HTTP #{event.status} response (X-Goog-Upload-Status: #{status_desc})"
          when Event::ChunkRead
            "received unexpected stream chunk read (#{event.bytes_buffered} bytes, eof: #{event.eof})"
          when Event::RequestFailed
            "encountered unexpected request failure (#{event.kind}: #{event.message})"
          else
            "received unexpected event #{shape} (#{event.class.name})"
          end
        end

        ##
        # Resolves effective chunk size given user specification and backend granularity.
        #
        # @param user_chunk_size [Integer, nil]
        # @param chunk_granularity [Integer, nil]
        # @return [Integer] Effective chunk size in bytes
        def self.resolve_chunk_size user_chunk_size, chunk_granularity
          base_size = user_chunk_size || DEFAULT_CHUNK_SIZE
          return base_size if chunk_granularity.nil? || chunk_granularity <= 0
          return chunk_granularity if base_size <= chunk_granularity

          base_size - (base_size % chunk_granularity)
        end

        ##
        # Classifies an HTTP response into a canonical response shape.
        #
        # @param response [Event::HttpResponse]
        # @return [Symbol]
        def self.classify_http_response response
          status_header = header_value(response.headers, "x-goog-upload-status")&.downcase

          case status_header
          when "active"
            response.status == 200 ? :response_active : :response_cat2
          when "final"
            response.status == 200 ? :response_final : :response_rejected
          when "cancelled"
            response.status == 200 ? :response_cancelled : :response_fatal_bad_response
          when nil, ""
            if FATAL_STATUS_CODES.include? response.status
              :response_fatal_bad_response
            else
              :response_cat2
            end
          else
            :response_fatal_bad_response
          end
        end

        ##
        # Case-insensitive header lookup helper.
        #
        # @param headers [Hash, Object]
        # @param key [String]
        # @return [String, nil]
        def self.header_value headers, key
          return nil unless headers.is_a? Hash
          return headers[key] if headers.key? key

          target = key.downcase
          _, val = headers.find { |k, _| k.to_s.downcase == target }
          val
        end

        def self.classify_chunk_read event
          if !event.eof
            :chunk_read_full
          elsif event.bytes_buffered.positive?
            :chunk_read_eof_with_data
          else
            :chunk_read_eof_empty
          end
        end

        def self.classify_request_failed event
          case event.kind
          when :retries_exhausted then :request_retries_exhausted
          when :connection_failed then :request_connection_failed
          else :request_failed_unknown
          end
        end

        def self.classify_event_class event_class
          if event_class == Event::StartUpload
            :start_upload
          elsif event_class == Event::Cancel
            :user_cancel
          elsif event_class == Event::GlobalDeadlineExceeded
            :global_deadline_exceeded
          else
            :unknown
          end
        end
      end
      # rubocop:enable Metrics/ModuleLength
    end
  end
end
