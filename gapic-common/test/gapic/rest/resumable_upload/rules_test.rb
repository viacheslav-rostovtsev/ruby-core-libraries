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

class RulesTest < Minitest::Test
  include Gapic::Rest::ResumableUpload

  def setup
    @config = CompleteUploadConfig.new(
      initial_url: "https://example.com/upload",
      stream:      StringIO.new("test content"),
      upload_size: 1024,
      chunk_size:  512
    )
  end

  def test_shape_of_start_upload
    assert_equal :start_upload, Rules.shape_of(Event::StartUpload.new)
    assert_equal :start_upload, Rules.shape_of(Event::StartUpload)
  end

  def test_shape_of_chunk_read
    full = Event::ChunkRead.new bytes_buffered: 512, eof: false
    assert_equal :chunk_read_full, Rules.shape_of(full)

    eof_data = Event::ChunkRead.new bytes_buffered: 256, eof: true
    assert_equal :chunk_read_eof_with_data, Rules.shape_of(eof_data)

    eof_empty = Event::ChunkRead.new bytes_buffered: 0, eof: true
    assert_equal :chunk_read_eof_empty, Rules.shape_of(eof_empty)
  end

  def test_shape_of_cancel
    assert_equal :user_cancel, Rules.shape_of(Event::Cancel.new)
    assert_equal :user_cancel, Rules.shape_of(Event::Cancel)
  end

  def test_shape_of_global_deadline_exceeded
    assert_equal :global_deadline_exceeded, Rules.shape_of(Event::GlobalDeadlineExceeded.new)
    assert_equal :global_deadline_exceeded, Rules.shape_of(Event::GlobalDeadlineExceeded)
  end

  def test_shape_of_request_failed
    retries = Event::RequestFailed.new kind: :retries_exhausted
    assert_equal :request_retries_exhausted, Rules.shape_of(retries)

    conn = Event::RequestFailed.new kind: :connection_failed
    assert_equal :request_connection_failed, Rules.shape_of(conn)

    unknown = Event::RequestFailed.new kind: :other
    assert_equal :request_failed_unknown, Rules.shape_of(unknown)
  end

  def test_shape_of_http_response_active
    resp_200 = Event::HttpResponse.new status: 200, headers: { "X-Goog-Upload-Status" => "active" }
    assert_equal :response_active, Rules.shape_of(resp_200)

    resp_500 = Event::HttpResponse.new status: 500, headers: { "x-goog-upload-status" => "active" }
    assert_equal :response_cat2, Rules.shape_of(resp_500)
  end

  def test_shape_of_http_response_final
    resp_200 = Event::HttpResponse.new status: 200, headers: { "X-Goog-Upload-Status" => "final" }
    assert_equal :response_final, Rules.shape_of(resp_200)

    resp_400 = Event::HttpResponse.new status: 400, headers: { "x-goog-upload-status" => "final" }
    assert_equal :response_rejected, Rules.shape_of(resp_400)
  end

  def test_shape_of_http_response_cancelled
    resp_200 = Event::HttpResponse.new status: 200, headers: { "X-Goog-Upload-Status" => "cancelled" }
    assert_equal :response_cancelled, Rules.shape_of(resp_200)

    resp_500 = Event::HttpResponse.new status: 500, headers: { "x-goog-upload-status" => "cancelled" }
    assert_equal :response_fatal_bad_response, Rules.shape_of(resp_500)
  end

  def test_shape_of_http_response_missing_header
    [200, 400, 408, 409, 412, 416, 429, 499, 500, 502, 503, 504].each do |code|
      resp = Event::HttpResponse.new status: code, headers: {}
      assert_equal :response_cat2, Rules.shape_of(resp), "Status #{code} with missing header should be :response_cat2"
    end

    [401, 403, 404, 405, 410, 413, 415].each do |code|
      resp = Event::HttpResponse.new status: code, headers: {}
      assert_equal :response_fatal_bad_response, Rules.shape_of(resp),
                   "Status #{code} with missing header should be :response_fatal_bad_response"
    end
  end

  # Section 5: Chunk Size Adjustment Rules
  def test_resolve_chunk_size_no_granularity
    assert_equal 8_388_608, Rules.resolve_chunk_size(nil, nil)
    assert_equal 8_388_608, Rules.resolve_chunk_size(nil, 0)
    assert_equal 4_000_000, Rules.resolve_chunk_size(4_000_000, nil)
    assert_equal 4_000_000, Rules.resolve_chunk_size(4_000_000, 0)
  end

  def test_resolve_chunk_size_default_with_granularity
    # 8_388_608 % 256_000 = 196_608 -> 8_388_608 - 196_608 = 8_192_000
    assert_equal 8_192_000, Rules.resolve_chunk_size(nil, 256_000)
    # If DEFAULT_CHUNK_SIZE < granularity, promote to granularity
    assert_equal 16_000_000, Rules.resolve_chunk_size(nil, 16_000_000)
  end

  def test_resolve_chunk_size_user_specified_with_granularity
    # Case 3A: user_chunk_size >= granularity
    assert_equal 512_000, Rules.resolve_chunk_size(512_000, 256_000)
    assert_equal 768_000, Rules.resolve_chunk_size(1_000_000, 256_000)

    # Case 3B: user_chunk_size < granularity (promoted to granularity)
    assert_equal 256_000, Rules.resolve_chunk_size(100_000, 256_000)
  end

  # Section 4: State Machine Transitions
  def test_transition_initializing_to_starting
    state = State.new status: :initializing
    next_state, instructions = Rules.step state, Event::StartUpload.new, @config

    assert_equal :starting, next_state.status
    assert_equal 1, instructions.size
    assert_instance_of Instruction::SendStart, instructions.first
    assert_equal "https://example.com/upload", instructions.first.url
  end

  def test_transition_starting_to_transmission_reading
    state = State.new status: :starting
    headers = {
      "X-Goog-Upload-URL"               => "https://example.com/session123",
      "X-Goog-Upload-Chunk-Granularity" => "256000",
      "X-Goog-Upload-Status"            => "active"
    }
    resp = Event::HttpResponse.new status: 200, headers: headers
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :transmission_reading, next_state.status
    assert_equal "https://example.com/session123", next_state.upload_url
    assert_equal 256_000, next_state.chunk_granularity
    assert_equal 0, next_state.offset
    assert_equal 1, instructions.size
    assert_instance_of Instruction::FillBuffer, instructions.first
    assert_equal next_state.chunk_size, instructions.first.target_bytesize
  end

  def test_transition_starting_rejected
    state = State.new status: :starting
    resp = Event::HttpResponse.new status: 400, headers: { "x-goog-upload-status" => "final" }, body: "Invalid metadata"
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :rejected, next_state.status
    assert_instance_of Gapic::Common::UploadRejectedError, next_state.last_error
    assert_equal 1, instructions.size
    assert_instance_of Instruction::TerminateFailure, instructions.first
  end

  def test_transition_starting_fatal_error
    state = State.new status: :starting
    resp = Event::HttpResponse.new status: 401, headers: {}
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :error, next_state.status
    assert_instance_of Gapic::Common::BadResponseError, next_state.last_error
    assert_equal 401, next_state.last_error.status_code
    assert_equal 1, instructions.size
    assert_instance_of Instruction::TerminateFailure, instructions.first
  end

  def test_transition_starting_request_failed
    state = State.new status: :starting
    failed = Event::RequestFailed.new kind: :connection_failed, message: "DNS resolution failed"
    next_state, instructions = Rules.step state, failed, @config

    assert_equal :error, next_state.status
    assert_equal 1, instructions.size
    assert_instance_of Instruction::TerminateFailure, instructions.first
  end

  def test_transition_transmission_reading_full_chunk
    state = State.new status: :transmission_reading, upload_url: "https://example.com/session", offset: 0
    event = Event::ChunkRead.new bytes_buffered: 512, eof: false
    next_state, instructions = Rules.step state, event, @config

    assert_equal :transmission_sending, next_state.status
    assert_equal 512, next_state.in_flight_length
    assert_equal 1, instructions.size
    assert_instance_of Instruction::SendChunk, instructions.first
    refute instructions.first.finalize
    assert_equal 512, instructions.first.length
    assert_equal 0, instructions.first.offset
  end

  def test_transition_transmission_reading_eof_with_data
    state = State.new status: :transmission_reading, upload_url: "https://example.com/session", offset: 512
    event = Event::ChunkRead.new bytes_buffered: 256, eof: true
    next_state, instructions = Rules.step state, event, @config

    assert_equal :finalizing_sending_upload, next_state.status
    assert_equal 256, next_state.in_flight_length
    assert_equal 1, instructions.size
    assert_instance_of Instruction::SendChunk, instructions.first
    assert instructions.first.finalize
    assert_equal 256, instructions.first.length
    assert_equal 512, instructions.first.offset
  end

  def test_transition_transmission_reading_eof_empty
    state = State.new status: :transmission_reading, upload_url: "https://example.com/session", offset: 1024
    event = Event::ChunkRead.new bytes_buffered: 0, eof: true
    next_state, instructions = Rules.step state, event, @config

    assert_equal :finalizing_sending_finalize, next_state.status
    assert_equal 0, next_state.in_flight_length
    assert_equal 1, instructions.size
    assert_instance_of Instruction::SendFinalize, instructions.first
    assert_equal "https://example.com/session", instructions.first.url
  end

  def test_transition_transmission_sending_ack_chunk
    state = State.new status: :transmission_sending, offset: 0, in_flight_length: 512, chunk_size: 512
    resp = Event::HttpResponse.new status: 200, headers: { "x-goog-upload-status" => "active" }
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :transmission_reading, next_state.status
    assert_equal 512, next_state.offset
    assert_equal 0, next_state.in_flight_length
    assert_equal 3, instructions.size
    assert_instance_of Instruction::NotifyProgress, instructions[0]
    assert_equal 512, instructions[0].bytes_uploaded
    assert_equal 1024, instructions[0].total_bytes
    assert_instance_of Instruction::RealignBuffer, instructions[1]
    assert_equal 512, instructions[1].server_offset
    assert_instance_of Instruction::FillBuffer, instructions[2]
    assert_equal 512, instructions[2].target_bytesize
  end

  def test_transition_transmission_sending_cat2_triggers_recovery
    state = State.new status: :transmission_sending, upload_url: "https://example.com/session", offset: 0,
                      in_flight_length: 512
    resp = Event::HttpResponse.new status: 409, headers: {}
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :recovery, next_state.status
    assert_equal 0, next_state.in_flight_length
    assert_equal 1, instructions.size
    assert_instance_of Instruction::SendQuery, instructions.first
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
    assert_equal 1024, instructions[0].bytes_uploaded
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

  def test_transition_recovery_active_realigns_buffer
    state = State.new status: :recovery, upload_url: "https://example.com/session", offset: 0, chunk_size: 512
    headers = {
      "x-goog-upload-status"        => "active",
      "x-goog-upload-size-received" => "768"
    }
    resp = Event::HttpResponse.new status: 200, headers: headers
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :transmission_reading, next_state.status
    assert_equal 768, next_state.offset
    assert_equal 2, instructions.size
    assert_instance_of Instruction::RealignBuffer, instructions[0]
    assert_equal 768, instructions[0].server_offset
    assert_instance_of Instruction::FillBuffer, instructions[1]
  end

  def test_transition_recovery_final_completes_upload
    state = State.new status: :recovery, upload_url: "https://example.com/session", offset: 512
    resp = Event::HttpResponse.new status: 200, headers: { "x-goog-upload-status" => "final" }
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :success, next_state.status
    assert_equal 1, instructions.size
    assert_instance_of Instruction::TerminateSuccess, instructions.first
  end

  def test_transition_recovery_cat2_retries_query
    state = State.new status: :recovery, upload_url: "https://example.com/session"
    resp = Event::HttpResponse.new status: 416, headers: {}
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :recovery, next_state.status
    assert_equal 1, instructions.size
    assert_instance_of Instruction::SendQuery, instructions.first
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

  def test_transition_global_deadline_exceeded
    state = State.new status: :transmission_sending
    next_state, instructions = Rules.step state, Event::GlobalDeadlineExceeded.new, @config

    assert_equal :error, next_state.status
    assert_instance_of Gapic::Common::DeadlineExceededError, next_state.last_error
    assert_equal 1, instructions.size
    assert_instance_of Instruction::TerminateFailure, instructions.first
  end

  def test_invalid_transition_raises_error
    state = State.new status: :initializing
    assert_raises InvalidTransitionError do
      Rules.step state, Event::ChunkRead.new(bytes_buffered: 512, eof: false), @config
    end
  end
end
