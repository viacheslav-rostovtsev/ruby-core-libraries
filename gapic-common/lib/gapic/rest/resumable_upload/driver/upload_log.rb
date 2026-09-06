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

require "google/logging/message"
require "gapic/rest/resumable_upload/driver/abridge"

module Gapic
  module Rest
    module ResumableUpload
      class Driver
        ##
        # @private
        # Structured logging helper for a single Resumable Upload run.
        #
        class UploadLog
          attr_reader :upload_id

          def initialize stub_logger, upload_id:
            @stub_logger = stub_logger
            @upload_id = upload_id
          end

          def decision decision
            msg = "Rules: #{decision.from_status} + #{decision.shape} -> " \
                  "#{decision.recipe} -> #{decision.next_state.status}"
            entry(
              :debug,
              msg,
              fromStatus:     decision.from_status,
              shape:          decision.shape,
              recipe:         decision.recipe,
              toStatus:       decision.next_state.status,
              offset:         decision.next_state.offset,
              inFlightLength: decision.next_state.in_flight_length,
              instructions:   Abridge.instructions(decision.instructions)
            )
          end

          # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
          def lifecycle decision, config
            case decision.recipe
            when :start_session
              entry(
                :info,
                "Initiating resumable upload",
                recipe:             decision.recipe,
                uploadSize:         config.upload_size,
                requestedChunkSize: config.chunk_size
              )
            when :begin_transmission
              entry(
                :info,
                "Upload session established",
                recipe:             decision.recipe,
                effectiveChunkSize: decision.next_state.chunk_size,
                granularity:        decision.next_state.chunk_granularity,
                uploadUrl:          Abridge.url(decision.next_state.upload_url)
              )
            when :send_chunk
              entry(
                :debug,
                "Sending upload chunk",
                recipe:         decision.recipe,
                offset:         decision.next_state.offset,
                inFlightLength: decision.next_state.in_flight_length
              )
            when :ack_chunk
              entry(
                :info,
                "Upload chunk acknowledged",
                recipe: decision.recipe,
                offset: decision.next_state.offset
              )
            when :complete_upload
              entry(
                :info,
                "Resumable upload completed",
                recipe: decision.recipe,
                offset: decision.next_state.offset
              )
            when :enter_recovery
              entry(
                :info,
                "Entering upload recovery",
                recipe: decision.recipe,
                offset: decision.next_state.offset
              )
            when :resume_from_query
              entry(
                :info,
                "Resuming upload from server offset",
                recipe: decision.recipe,
                offset: decision.next_state.offset
              )
            when :send_cancel
              entry(
                :info,
                "Canceling resumable upload",
                recipe:    decision.recipe,
                uploadUrl: Abridge.url(decision.next_state.upload_url)
              )
            when :complete_cancellation
              entry(
                :info,
                "Resumable upload canceled",
                recipe: decision.recipe
              )
            when :fail_with_bad_session,
                 :fail_with_range_drift,
                 :fail_with_protocol_error,
                 :fail_with_terminal_error,
                 :fail_with_unmatched_transition
              error_msg = decision.next_state.last_error&.message || decision.next_state.last_error.to_s
              entry(
                :warn,
                "Resumable upload failed",
                recipe: decision.recipe,
                error:  error_msg
              )
            end
          end
          # rubocop:enable Metrics/AbcSize, Metrics/MethodLength

          def wire_send method:, url:, headers:, start_attempt:, body_size: nil, body: nil, body_is_error: false
            fields = {
              method:       method,
              url:          Abridge.url(url),
              headers:      Abridge.headers(headers),
              startAttempt: start_attempt
            }
            fields[:bodySize] = body_size if body_size
            fields[:body] = body_is_error ? Abridge.error_body(body) : Abridge.bytes(body) if body

            entry :debug, "Sending #{method} request", **fields
          end

          def wire_receive event
            entry(
              :debug,
              "Received HTTP #{event.status}",
              status:  event.status,
              headers: Abridge.headers(event.headers),
              body:    event.status >= 400 ? Abridge.error_body(event.body) : Abridge.bytes(event.body)
            )
          end

          def wire_failure event
            entry(
              :debug,
              "Request failed: #{event.kind}",
              kind:  event.kind,
              error: event.message
            )
          end

          def buffer_realign action, server_offset:, current_offset:, unseekable: false
            if unseekable
              entry(
                :warn,
                "Server offset rewind on unseekable stream",
                action:        action,
                serverOffset:  server_offset,
                currentOffset: current_offset
              )
            end

            entry(
              :debug,
              "Buffer realignment: #{action}",
              action:        action,
              serverOffset:  server_offset,
              currentOffset: current_offset
            )
          end

          def unmatched_transition state, event, error
            entry(
              :warn,
              "Unmatched transition",
              status: state.status,
              shape:  Rules.shape_of(event),
              error:  error.message
            )
          end

          private

          def entry severity, log_msg, **fields
            @stub_logger.public_send severity do |builder|
              builder.set_system_name
              builder.set_service
              builder.set "uploadId", @upload_id
              fields.each do |k, v|
                builder.set k.to_s, v
              end
              builder.message = log_msg
            end
          end
        end
      end
    end
  end
end
