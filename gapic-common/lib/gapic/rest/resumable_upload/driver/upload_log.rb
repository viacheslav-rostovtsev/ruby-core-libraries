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
          SILENT_RECIPES = [
            :ignore_duplicate_cancel
          ].freeze

          LIFECYCLE = {
            start_session:                  [:info, "Initiating resumable upload"],
            begin_transmission:             [:info, "Upload session established"],
            send_chunk:                     [:debug, "Sending upload chunk"],
            send_upload_finalize:           [:info, "Sending final upload chunk"],
            send_finalize:                  [:info, "Sending finalize command"],
            ack_chunk:                      [:info, "Upload chunk acknowledged"],
            enter_recovery:                 [:info, "Entering upload recovery"],
            retry_recovery:                 [:info, "Retrying upload recovery query"],
            realign_from_recovery:          [:info, "Resuming upload from server offset"],
            complete_upload_with_data:      [:info, "Resumable upload completed"],
            complete_upload_finalized:      [:info, "Resumable upload completed"],
            cancel_session:                 [:info, "Canceling resumable upload"],
            complete_cancellation:          [:info, "Resumable upload canceled"],
            fail_with_deadline_exceeded:    [:warn, "Resumable upload failed"],
            fail_with_rejected:             [:warn, "Resumable upload failed"],
            fail_with_bad_response:         [:warn, "Resumable upload failed"],
            fail_with_request_error:        [:warn, "Resumable upload failed"],
            fail_with_unmatched_transition: [:warn, "Resumable upload failed"]
          }.freeze

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

          def lifecycle decision, config
            return if SILENT_RECIPES.include? decision.recipe

            severity, message = LIFECYCLE[decision.recipe]
            return unless severity

            extra_fields = lifecycle_fields decision, config
            entry severity, message, recipe: decision.recipe, **extra_fields
          end

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

          def lifecycle_fields decision, config
            state = decision.next_state
            case decision.recipe
            when :start_session
              { uploadSize: config.upload_size, requestedChunkSize: config.chunk_size }
            when :begin_transmission
              {
                effectiveChunkSize: state.chunk_size,
                granularity:        state.chunk_granularity,
                uploadUrl:          Abridge.url(state.upload_url)
              }
            when :send_chunk, :send_upload_finalize
              { offset: state.offset, inFlightLength: state.in_flight_length }
            when :send_finalize, :ack_chunk, :enter_recovery, :retry_recovery,
                 :realign_from_recovery, :complete_upload_with_data, :complete_upload_finalized
              { offset: state.offset }
            when :cancel_session
              { uploadUrl: Abridge.url(state.upload_url) }
            when :fail_with_deadline_exceeded, :fail_with_rejected, :fail_with_bad_response,
                 :fail_with_request_error, :fail_with_unmatched_transition
              { error: state.last_error&.message || state.last_error.to_s }
            else
              {}
            end
          end

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
