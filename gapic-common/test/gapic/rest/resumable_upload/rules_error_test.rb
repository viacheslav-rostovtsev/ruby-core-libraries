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
# Tests for ResumableUpload Rules terminal error transitions and actionable error formatting.
#
class RulesErrorTest < Minitest::Test
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

  def test_transition_starting_rejected
    state = State.new status: :starting
    resp = Event::HttpResponse.new status: 403, headers: { "x-goog-upload-status" => "final" }, body: "Forbidden"
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :rejected, next_state.status
    assert_instance_of UploadRejectedError, next_state.last_error
    assert_equal "Forbidden", next_state.last_error.response_body
    assert_equal 1, instructions.size
    assert_instance_of Instruction::TerminateFailure, instructions.first
    assert_equal next_state.last_error, instructions.first.error
  end

  def test_transition_starting_fatal_error
    state = State.new status: :starting
    resp = Event::HttpResponse.new status: 400, headers: {}, body: "Bad Request"
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :error, next_state.status
    assert_instance_of BadResponseError, next_state.last_error
    assert_equal 400, next_state.last_error.status_code
    assert_equal 1, instructions.size
    assert_instance_of Instruction::TerminateFailure, instructions.first
  end

  def test_transition_starting_request_failed
    state = State.new status: :starting
    err = StandardError.new "DNS resolution failed"
    failed = Event::RequestFailed.new kind: :connection_failed, message: "DNS resolution failed", source_error: err
    next_state, instructions = Rules.step state, failed, @config

    assert_equal :error, next_state.status
    assert_equal err, next_state.last_error
    assert_equal 1, instructions.size
    assert_instance_of Instruction::TerminateFailure, instructions.first
  end

  def test_transition_starting_timeout_terminates_failure
    state = State.new status: :starting
    err = StandardError.new "Read timeout"
    req_failed = Event::RequestFailed.new kind: :timeout, message: "Read timeout", source_error: err
    next_state, instructions = Rules.step state, req_failed, @config

    assert_equal :error, next_state.status
    assert_equal 0, next_state.in_flight_length
    assert_equal err, next_state.last_error
    assert_equal 1, instructions.size
    assert_instance_of Instruction::TerminateFailure, instructions.first
    assert_equal err, instructions.first.error
  end

  def test_transition_transmission_sending_retries_exhausted_terminates_failure
    state = State.new status: :transmission_sending, upload_url: "https://example.com/session", offset: 0,
                      in_flight_length: 512
    err = StandardError.new "Retries exhausted"
    req_failed = Event::RequestFailed.new kind: :retries_exhausted, message: "Retries exhausted", source_error: err
    next_state, instructions = Rules.step state, req_failed, @config

    assert_equal :error, next_state.status
    assert_equal 0, next_state.in_flight_length
    assert_equal err, next_state.last_error
    assert_equal 1, instructions.size
    assert_instance_of Instruction::TerminateFailure, instructions.first
    assert_equal err, instructions.first.error
  end

  def test_transition_recovery_timeout_terminates_failure
    state = State.new status: :recovery, upload_url: "https://example.com/session"
    err = StandardError.new "Query read timeout"
    req_failed = Event::RequestFailed.new kind: :timeout, message: "Query read timeout", source_error: err
    next_state, instructions = Rules.step state, req_failed, @config

    assert_equal :error, next_state.status
    assert_equal 0, next_state.in_flight_length
    assert_equal err, next_state.last_error
    assert_equal 1, instructions.size
    assert_instance_of Instruction::TerminateFailure, instructions.first
    assert_equal err, instructions.first.error
  end

  def test_transition_global_deadline_exceeded
    state = State.new status: :transmission_sending, upload_url: "https://example.com/session"
    next_state, instructions = Rules.step state, Event::GlobalDeadlineExceeded.new, @config

    assert_equal :error, next_state.status
    assert_instance_of DeadlineExceededError, next_state.last_error
    assert_equal 1, instructions.size
    assert_instance_of Instruction::TerminateFailure, instructions.first
  end

  def test_invalid_transition_raises_actionable_error_with_response_details_and_header
    state = State.new status: :transmission_sending
    resp = Event::HttpResponse.new status: 200, headers: { "X-Goog-Upload-Status" => "final" }, body: '{"done":true}'

    err = assert_raises InvalidTransitionError do
      Rules.step state, resp, @config
    end

    expected_msg = "Resumable upload failed while sending a chunk of data: " \
                   "received an unexpected HTTP 200 response (X-Goog-Upload-Status: 'final')."
    assert_equal expected_msg, err.message
    assert_equal :transmission_sending, err.state
    assert_equal resp, err.event
    assert_equal resp, err.response
  end

  def test_invalid_transition_shows_missing_when_upload_status_header_absent
    state = State.new status: :transmission_reading
    resp = Event::HttpResponse.new status: 200, headers: {}, body: ""

    err = assert_raises InvalidTransitionError do
      Rules.step state, resp, @config
    end

    expected_msg = "Resumable upload failed while reading chunk from stream: " \
                   "received an unexpected HTTP 200 response (X-Goog-Upload-Status: missing)."
    assert_equal expected_msg, err.message
    assert_equal :transmission_reading, err.state
    assert_equal resp, err.response
  end

  def test_invalid_transition_raises_error_for_non_http_event
    state = State.new status: :starting
    event = Event::ChunkRead.new bytes_buffered: 512, eof: false

    err = assert_raises InvalidTransitionError do
      Rules.step state, event, @config
    end

    expected_msg = "Resumable upload failed while initiating upload session: " \
                   "received unexpected stream chunk read (512 bytes, eof: false)."
    assert_equal expected_msg, err.message
    assert_equal :starting, err.state
    assert_equal event, err.event
    assert_nil err.response
  end

  def test_rejected_with_wrapped_error_deprefixes_message_and_preserves_metadata
    details = [{ "reason" => "ACCESS_DENIED" }]
    headers = { "x-goog-upload-status" => "final", "content-type" => "application/json" }
    wrapped_err = Gapic::Rest::Error.new(
      "#{Gapic::Rest::Error::REST_ERROR_PREFIX}: The caller does not have permission",
      403,
      status:  "PERMISSION_DENIED",
      details: details,
      headers: headers
    )
    resp = Event::HttpResponse.new(
      status:  403,
      headers: headers,
      body:    '{"error":{"message":"The caller does not have permission"}}',
      error:   wrapped_err
    )

    state = State.new status: :starting
    next_state, instructions = Rules.step state, resp, @config

    assert_equal :rejected, next_state.status
    err = next_state.last_error
    assert_instance_of UploadRejectedError, err
    assert_equal "Upload rejected by server with HTTP 403 Permission Denied: The caller does not have permission",
                 err.message
    assert_equal 403, err.status_code
    assert_equal "PERMISSION_DENIED", err.status
    assert_equal details, err.details
    assert_equal details, err.status_details
    assert_equal headers, err.headers
    assert_equal headers, err.header
    assert_equal '{"error":{"message":"The caller does not have permission"}}', err.response_body
    assert_equal err, instructions.first.error
  end

  def test_rejected_fallback_without_wrapped_error
    resp = Event::HttpResponse.new status: 403, headers: { "x-goog-upload-status" => "final" }, body: "Forbidden"
    state = State.new status: :starting
    next_state, _instructions = Rules.step state, resp, @config

    assert_equal :rejected, next_state.status
    err = next_state.last_error
    assert_instance_of UploadRejectedError, err
    assert_equal "Upload rejected by server with HTTP 403 Forbidden (X-Goog-Upload-Status: 'final')", err.message
    assert_equal 403, err.status_code
    assert_equal "Forbidden", err.response_body
  end

  def test_bad_response_with_wrapped_error_deprefixes_message_and_preserves_metadata
    details = ["Quota limit details"]
    headers = { "x-goog-upload-status" => "active" }
    wrapped_err = Gapic::Rest::Error.new(
      "#{Gapic::Rest::Error::REST_ERROR_PREFIX}: Quota limit reached",
      429,
      status:  "RESOURCE_EXHAUSTED",
      details: details,
      headers: headers
    )
    resp = Event::HttpResponse.new status: 429, headers: headers, body: "Too many requests", error: wrapped_err

    state = State.new status: :starting
    next_state, _instructions = Rules.step state, resp, @config

    assert_equal :error, next_state.status
    err = next_state.last_error
    assert_instance_of BadResponseError, err
    assert_equal "Resumable upload failed with HTTP 429 Resource Exhausted: Quota limit reached", err.message
    assert_equal 429, err.status_code
    assert_equal "RESOURCE_EXHAUSTED", err.status
    assert_equal details, err.status_details
    assert_equal headers, err.headers
    assert_equal "Too many requests", err.response_body
  end

  def test_bad_response_fallback_without_wrapped_error
    resp = Event::HttpResponse.new status: 503, headers: {}, body: "Service unavailable"
    state = State.new status: :starting
    next_state, _instructions = Rules.step state, resp, @config

    assert_equal :error, next_state.status
    err = next_state.last_error
    assert_instance_of BadResponseError, err
    assert_equal "Resumable upload failed with HTTP 503 Service Unavailable (X-Goog-Upload-Status: missing)", err.message
    assert_equal 503, err.status_code
    assert_equal "Service unavailable", err.response_body
  end

  def test_error_class_inheritance_hierarchy
    assert_operator UploadCancelledError, :<, Gapic::Common::Error
    refute_operator UploadCancelledError, :<, Gapic::Rest::Error

    assert_operator DeadlineExceededError, :<, Gapic::Common::Error
    refute_operator DeadlineExceededError, :<, Gapic::Rest::Error

    assert_operator UploadRejectedError, :<, Gapic::Rest::Error
    assert_operator BadResponseError, :<, Gapic::Rest::Error
  end
end
