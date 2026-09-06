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

require "test_helper"
require "gapic/rest/resumable_upload"
require "stringio"

##
# Tests for ResumableUpload Rules normal progression and session lifecycle state transitions.
#
class RulesTest < Minitest::Test
  include Gapic::Rest::ResumableUpload

  def setup
    @config = CompleteUploadConfig.new(
      initial_url:     "https://example.com/upload",
      initial_headers: { "X-Custom" => "value" },
      initial_body:    '{"name":"obj"}',
      stream:          StringIO.new("data"),
      upload_size:     1024,
      chunk_size:      512
    )
  end

  def test_transition_initializing_to_starting
    state = State.new status: :initializing
    next_state, instructions = Rules.step state, Event::StartUpload.new, @config

    assert_equal :starting, next_state.status
    assert_equal 1, instructions.size
    assert_instance_of Instruction::SendStart, instructions.first
    assert_equal "https://example.com/upload", instructions.first.url
    assert_equal({ "X-Custom" => "value" }, instructions.first.headers)
    assert_equal '{"name":"obj"}', instructions.first.body
  end

  def test_transition_starting_to_transmission_reading
    state = State.new status: :starting
    headers = {
      "x-goog-upload-status"            => "active",
      "x-goog-upload-url"               => "https://example.com/session",
      "x-goog-upload-chunk-granularity" => "256"
    }
    resp = Event::HttpResponse.new status: 200, headers: headers
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :transmission_reading, next_state.status
    assert_equal "https://example.com/session", next_state.upload_url
    assert_equal 256, next_state.chunk_granularity
    assert_equal 512, next_state.chunk_size
    assert_equal 0, next_state.offset
    assert_equal 0, next_state.in_flight_length
    assert_equal 1, instructions.size
    assert_instance_of Instruction::FillBuffer, instructions.first
    assert_equal 512, instructions.first.target_bytesize
  end

  def test_transition_transmission_reading_full_chunk
    state = State.new status: :transmission_reading, upload_url: "https://example.com/session", offset: 0,
                      chunk_size: 512
    event = Event::ChunkRead.new bytes_buffered: 512, eof: false
    next_state, instructions = Rules.step state, event, @config

    assert_equal :transmission_sending, next_state.status
    assert_equal 512, next_state.in_flight_length
    assert_equal 1, instructions.size
    assert_instance_of Instruction::SendChunk, instructions.first
    assert_equal 0, instructions.first.offset
    assert_equal 512, instructions.first.length
    refute instructions.first.finalize
  end

  def test_transition_transmission_reading_eof_with_data
    state = State.new status: :transmission_reading, upload_url: "https://example.com/session", offset: 512,
                      chunk_size: 512
    event = Event::ChunkRead.new bytes_buffered: 200, eof: true
    next_state, instructions = Rules.step state, event, @config

    assert_equal :finalizing_sending_upload, next_state.status
    assert_equal 200, next_state.in_flight_length
    assert_equal 1, instructions.size
    assert_instance_of Instruction::SendChunk, instructions.first
    assert_equal 512, instructions.first.offset
    assert_equal 200, instructions.first.length
    assert instructions.first.finalize
  end

  def test_transition_transmission_reading_eof_empty
    state = State.new status: :transmission_reading, upload_url: "https://example.com/session", offset: 1024,
                      chunk_size: 512
    event = Event::ChunkRead.new bytes_buffered: 0, eof: true
    next_state, instructions = Rules.step state, event, @config

    assert_equal :finalizing_sending_finalize, next_state.status
    assert_equal 0, next_state.in_flight_length
    assert_equal 1, instructions.size
    assert_instance_of Instruction::SendFinalize, instructions.first
    assert_equal "https://example.com/session", instructions.first.url
  end

  def test_transition_transmission_sending_ack_chunk
    state = State.new status: :transmission_sending, upload_url: "https://example.com/session", offset: 0,
                      in_flight_length: 512, chunk_size: 512
    resp = Event::HttpResponse.new status: 200, headers: { "x-goog-upload-status" => "active" }
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :transmission_reading, next_state.status
    assert_equal 512, next_state.offset
    assert_equal 0, next_state.in_flight_length
    assert_equal 3, instructions.size
    assert_instance_of Instruction::NotifyProgress, instructions[0]
    assert_equal Progress.new(bytes_uploaded: 512, total_bytes: 1024), instructions[0].progress
    assert_instance_of Instruction::RealignBuffer, instructions[1]
    assert_equal 512, instructions[1].server_offset
    assert_instance_of Instruction::FillBuffer, instructions[2]
    assert_equal 512, instructions[2].target_bytesize
  end

  def test_transition_finalizing_sending_upload_success
    state = State.new status: :finalizing_sending_upload, offset: 512, in_flight_length: 512
    resp = Event::HttpResponse.new status: 200, headers: { "x-goog-upload-status" => "final" }, body: '{"done":true}'
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :success, next_state.status
    assert_equal 1024, next_state.offset
    assert_equal 0, next_state.in_flight_length
    assert_equal 2, instructions.size
    assert_instance_of Instruction::NotifyProgress, instructions[0]
    assert_equal Progress.new(bytes_uploaded: 1024, total_bytes: 1024), instructions[0].progress
    assert_instance_of Instruction::TerminateSuccess, instructions[1]
  end

  def test_transition_finalizing_sending_finalize_success
    state = State.new status: :finalizing_sending_finalize, offset: 1024, in_flight_length: 0
    resp = Event::HttpResponse.new status: 200, headers: { "x-goog-upload-status" => "final" }, body: '{"done":true}'
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :success, next_state.status
    assert_equal 1, instructions.size
    assert_instance_of Instruction::TerminateSuccess, instructions.first
  end

  def test_transition_cancellation_flow
    state = State.new status: :transmission_sending, upload_url: "https://example.com/session"
    next_state, instructions = Rules.step state, Event::Cancel.new, @config

    assert_equal :cancelling, next_state.status
    assert_equal 1, instructions.size
    assert_instance_of Instruction::SendCancel, instructions.first

    # Duplicate cancel in cancelling state does nothing
    dup_state, dup_instructions = Rules.step next_state, Event::Cancel.new, @config
    assert_equal :cancelling, dup_state.status
    assert_empty dup_instructions

    # Cancellation confirmed
    resp = Event::HttpResponse.new status: 200, headers: { "x-goog-upload-status" => "cancelled" }
    final_state, final_instructions = Rules.step next_state, resp, @config
    assert_equal :cancelled, final_state.status
    assert_instance_of Gapic::Common::UploadCancelledError, final_state.last_error
    assert_equal 1, final_instructions.size
    assert_instance_of Instruction::TerminateFailure, final_instructions.first
  end

  def test_decide_assertions_per_row
    # Row 1: [:initializing, :start_upload] -> :start_session
    decision = Rules.decide State.new(status: :initializing), Event::StartUpload.new, @config
    assert_equal :initializing, decision.from_status
    assert_equal :start_upload, decision.shape
    assert_equal :starting, decision.next_state.status
    assert_instance_of Instruction::SendStart, decision.instructions.first

    # Row 2: [:starting, :response_active] -> :begin_transmission
    active_resp = Event::HttpResponse.new(
      status:  200,
      headers: { "x-goog-upload-status" => "active", "x-goog-upload-url" => "https://example.com/session" }
    )
    decision = Rules.decide State.new(status: :starting), active_resp, @config
    assert_equal :starting, decision.from_status
    assert_equal :response_active, decision.shape
    assert_equal :transmission_reading, decision.next_state.status
    assert_instance_of Instruction::FillBuffer, decision.instructions.first

    # Row 3: [:transmission_reading, :chunk_read_full] -> :send_chunk
    decision = Rules.decide(
      State.new(status: :transmission_reading, upload_url: "https://example.com/session"),
      Event::ChunkRead.new(bytes_buffered: 512, eof: false),
      @config
    )
    assert_equal :transmission_reading, decision.from_status
    assert_equal :chunk_read_full, decision.shape
    assert_equal :transmission_sending, decision.next_state.status
    assert_instance_of Instruction::SendChunk, decision.instructions.first
    refute decision.instructions.first.finalize

    # Row 4: [:transmission_reading, :chunk_read_eof_with_data] -> :send_upload_finalize
    decision = Rules.decide(
      State.new(status: :transmission_reading, upload_url: "https://example.com/session"),
      Event::ChunkRead.new(bytes_buffered: 256, eof: true),
      @config
    )
    assert_equal :transmission_reading, decision.from_status
    assert_equal :chunk_read_eof_with_data, decision.shape
    assert_equal :finalizing_sending_upload, decision.next_state.status
    assert_instance_of Instruction::SendChunk, decision.instructions.first
    assert decision.instructions.first.finalize

    # Row 5: [:transmission_reading, :chunk_read_eof_empty] -> :send_finalize
    decision = Rules.decide(
      State.new(status: :transmission_reading, upload_url: "https://example.com/session"),
      Event::ChunkRead.new(bytes_buffered: 0, eof: true),
      @config
    )
    assert_equal :transmission_reading, decision.from_status
    assert_equal :chunk_read_eof_empty, decision.shape
    assert_equal :finalizing_sending_finalize, decision.next_state.status
    assert_instance_of Instruction::SendFinalize, decision.instructions.first

    # Row 6: [:transmission_sending, :response_active] -> :ack_chunk
    decision = Rules.decide(
      State.new(status: :transmission_sending, upload_url: "https://example.com/session", offset: 0, in_flight_length: 512),
      active_resp,
      @config
    )
    assert_equal :transmission_sending, decision.from_status
    assert_equal :response_active, decision.shape
    assert_equal :transmission_reading, decision.next_state.status
    assert_equal 3, decision.instructions.size

    # Row 7: [:transmission_sending, :response_cat2] -> :enter_recovery
    cat2_resp = Event::HttpResponse.new status: 503, headers: {}
    decision = Rules.decide(
      State.new(status: :transmission_sending, upload_url: "https://example.com/session"),
      cat2_resp,
      @config
    )
    assert_equal :transmission_sending, decision.from_status
    assert_equal :response_cat2, decision.shape
    assert_equal :recovery, decision.next_state.status
    assert_instance_of Instruction::SendQuery, decision.instructions.first

    # Row 8: [:finalizing_sending_upload, :response_final] -> :complete_upload_with_data
    final_resp = Event::HttpResponse.new status: 200, headers: { "x-goog-upload-status" => "final" }
    decision = Rules.decide(
      State.new(status: :finalizing_sending_upload, offset: 512, in_flight_length: 512),
      final_resp,
      @config
    )
    assert_equal :finalizing_sending_upload, decision.from_status
    assert_equal :response_final, decision.shape
    assert_equal :success, decision.next_state.status
    assert_equal 2, decision.instructions.size

    # Row 9: [:finalizing_sending_finalize, :response_final] -> :complete_upload_finalized
    decision = Rules.decide State.new(status: :finalizing_sending_finalize), final_resp, @config
    assert_equal :finalizing_sending_finalize, decision.from_status
    assert_equal :response_final, decision.shape
    assert_equal :success, decision.next_state.status
    assert_instance_of Instruction::TerminateSuccess, decision.instructions.first

    # Row 10: [:recovery, :response_active] -> :realign_from_recovery
    recovery_active_resp = Event::HttpResponse.new(
      status:  200,
      headers: { "x-goog-upload-status" => "active", "x-goog-upload-size-received" => "256" }
    )
    decision = Rules.decide State.new(status: :recovery), recovery_active_resp, @config
    assert_equal :recovery, decision.from_status
    assert_equal :response_active, decision.shape
    assert_equal :transmission_reading, decision.next_state.status
    assert_instance_of Instruction::RealignBuffer, decision.instructions.first

    # Row 11: [:recovery, :response_cat2] -> :retry_recovery
    decision = Rules.decide State.new(status: :recovery), cat2_resp, @config
    assert_equal :recovery, decision.from_status
    assert_equal :response_cat2, decision.shape
    assert_equal :recovery, decision.next_state.status
    assert_instance_of Instruction::SendQuery, decision.instructions.first

    # Row 12: [:cancelling, :response_cancelled] -> :complete_cancellation
    cancelled_resp = Event::HttpResponse.new status: 200, headers: { "x-goog-upload-status" => "cancelled" }
    decision = Rules.decide State.new(status: :cancelling), cancelled_resp, @config
    assert_equal :cancelling, decision.from_status
    assert_equal :response_cancelled, decision.shape
    assert_equal :cancelled, decision.next_state.status
    assert_instance_of Instruction::TerminateFailure, decision.instructions.first

    # Row 13: [:cancelling, :user_cancel] -> :ignore_duplicate_cancel
    decision = Rules.decide State.new(status: :cancelling), Event::Cancel.new, @config
    assert_equal :cancelling, decision.from_status
    assert_equal :user_cancel, decision.shape
    assert_equal :cancelling, decision.next_state.status
    assert_empty decision.instructions

    # Row 14: [_, :global_deadline_exceeded] -> :fail_with_deadline_exceeded
    decision = Rules.decide State.new(status: :transmission_sending), Event::GlobalDeadlineExceeded.new, @config
    assert_equal :transmission_sending, decision.from_status
    assert_equal :global_deadline_exceeded, decision.shape
    assert_equal :error, decision.next_state.status
    assert_instance_of Gapic::Common::DeadlineExceededError, decision.next_state.last_error

    # Row 15: [_, :user_cancel] -> :cancel_session
    decision = Rules.decide State.new(status: :transmission_sending), Event::Cancel.new, @config
    assert_equal :transmission_sending, decision.from_status
    assert_equal :user_cancel, decision.shape
    assert_equal :cancelling, decision.next_state.status
    assert_instance_of Instruction::SendCancel, decision.instructions.first

    # Row 16: [:starting, :response_rejected] -> :fail_with_rejected
    rejected_resp = Event::HttpResponse.new status: 403, headers: { "x-goog-upload-status" => "final" }, body: "Rejected"
    decision = Rules.decide State.new(status: :starting), rejected_resp, @config
    assert_equal :starting, decision.from_status
    assert_equal :response_rejected, decision.shape
    assert_equal :rejected, decision.next_state.status
    assert_instance_of Gapic::Common::UploadRejectedError, decision.next_state.last_error

    # Row 17: [:starting, :response_cat2] -> :fail_with_bad_response
    decision = Rules.decide State.new(status: :starting), cat2_resp, @config
    assert_equal :starting, decision.from_status
    assert_equal :response_cat2, decision.shape
    assert_equal :error, decision.next_state.status
    assert_instance_of Gapic::Common::BadResponseError, decision.next_state.last_error

    # Row 18: [:starting, :request_retries_exhausted] -> :fail_with_request_error
    req_failed = Event::RequestFailed.new kind: :retries_exhausted, message: "Exhausted"
    decision = Rules.decide State.new(status: :starting), req_failed, @config
    assert_equal :starting, decision.from_status
    assert_equal :request_retries_exhausted, decision.shape
    assert_equal :error, decision.next_state.status
    assert_instance_of Instruction::TerminateFailure, decision.instructions.first
  end
end
