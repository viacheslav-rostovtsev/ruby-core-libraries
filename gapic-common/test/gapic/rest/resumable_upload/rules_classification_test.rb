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

##
# Tests for classification and header extraction rules in the Resumable Upload protocol.
#
class RulesClassificationTest < Minitest::Test
  include Gapic::Rest::ResumableUpload

  def test_header_value_exact_match
    headers = { "X-Goog-Upload-Status" => "active" }
    assert_equal "active", Rules.header_value(headers, "X-Goog-Upload-Status")
  end

  def test_header_value_case_insensitivity
    assert_equal "active", Rules.header_value({ "x-goog-upload-status" => "active" }, "X-Goog-Upload-Status")
    assert_equal "active", Rules.header_value({ "X-GOOG-UPLOAD-STATUS" => "active" }, "X-Goog-Upload-Status")
    assert_equal "active", Rules.header_value({ "x-Goog-UpLoad-Status" => "active" }, "X-Goog-Upload-Status")
  end

  def test_header_value_with_symbol_keys
    headers = { :"x-goog-upload-status" => "active" }
    assert_equal "active", Rules.header_value(headers, "X-Goog-Upload-Status")
  end

  def test_header_value_missing_or_non_hash
    assert_nil Rules.header_value({ "Content-Type" => "text/plain" }, "X-Goog-Upload-Status")
    assert_nil Rules.header_value(nil, "X-Goog-Upload-Status")
    assert_nil Rules.header_value([], "X-Goog-Upload-Status")
    assert_nil Rules.header_value("string", "X-Goog-Upload-Status")
  end

  def test_classify_http_response_active
    resp_200 = Event::HttpResponse.new status: 200, headers: { "X-Goog-Upload-Status" => "active" }, body: ""
    assert_equal :response_active, Rules.classify_http_response(resp_200)

    # Value case variations
    ["Active", "ACTIVE", "aCtIvE"].each do |val|
      resp = Event::HttpResponse.new status: 200, headers: { "X-Goog-Upload-Status" => val }, body: ""
      assert_equal :response_active, Rules.classify_http_response(resp)
    end

    # Non-200 with active maps to Category 2
    [503, 500, 400, 408].each do |code|
      resp_non_200 = Event::HttpResponse.new status: code, headers: { "X-Goog-Upload-Status" => "active" }, body: ""
      assert_equal :response_cat2, Rules.classify_http_response(resp_non_200)
    end
  end

  def test_classify_http_response_final
    resp_200 = Event::HttpResponse.new status: 200, headers: { "X-Goog-Upload-Status" => "final" }, body: ""
    assert_equal :response_final, Rules.classify_http_response(resp_200)

    # Value case variations
    ["Final", "FINAL"].each do |val|
      resp = Event::HttpResponse.new status: 200, headers: { "X-Goog-Upload-Status" => val }, body: ""
      assert_equal :response_final, Rules.classify_http_response(resp)
    end

    # Non-200 with final maps to response_rejected
    [400, 404, 500].each do |code|
      resp_non_200 = Event::HttpResponse.new status: code, headers: { "X-Goog-Upload-Status" => "final" }, body: ""
      assert_equal :response_rejected, Rules.classify_http_response(resp_non_200)
    end
  end

  def test_classify_http_response_cancelled
    resp_200 = Event::HttpResponse.new status: 200, headers: { "X-Goog-Upload-Status" => "cancelled" }, body: ""
    assert_equal :response_cancelled, Rules.classify_http_response(resp_200)

    # Value case variations
    ["Cancelled", "CANCELLED"].each do |val|
      resp = Event::HttpResponse.new status: 200, headers: { "X-Goog-Upload-Status" => val }, body: ""
      assert_equal :response_cancelled, Rules.classify_http_response(resp)
    end

    # Non-200 with cancelled maps to fatal bad response
    [400, 500].each do |code|
      resp_non_200 = Event::HttpResponse.new status: code, headers: { "X-Goog-Upload-Status" => "cancelled" }, body: ""
      assert_equal :response_fatal_bad_response, Rules.classify_http_response(resp_non_200)
    end
  end

  def test_classify_http_response_missing_header_non_fatal
    # HTTP 200 missing header
    resp_200 = Event::HttpResponse.new status: 200, headers: {}, body: ""
    assert_equal :response_cat2, Rules.classify_http_response(resp_200)

    # Recoverable 4xx missing header
    Rules::CAT2_STATUS_CODES.each do |code|
      resp = Event::HttpResponse.new status: code, headers: {}, body: ""
      assert_equal :response_cat2, Rules.classify_http_response(resp), "Expected #{code} to classify as :response_cat2"
    end

    # 5xx server/gateway errors missing header
    [500, 502, 503, 504].each do |code|
      resp = Event::HttpResponse.new status: code, headers: {}, body: ""
      assert_equal :response_cat2, Rules.classify_http_response(resp), "Expected #{code} to classify as :response_cat2"
    end

    # Empty string header
    resp_empty = Event::HttpResponse.new status: 200, headers: { "X-Goog-Upload-Status" => "" }, body: ""
    assert_equal :response_cat2, Rules.classify_http_response(resp_empty)
  end

  def test_classify_http_response_missing_header_fatal_status_codes
    Rules::FATAL_STATUS_CODES.each do |code|
      resp = Event::HttpResponse.new status: code, headers: {}, body: ""
      assert_equal :response_fatal_bad_response, Rules.classify_http_response(resp),
                   "Expected fatal code #{code} to classify as :response_fatal_bad_response"

      resp_empty = Event::HttpResponse.new status: code, headers: { "X-Goog-Upload-Status" => "" }, body: ""
      assert_equal :response_fatal_bad_response, Rules.classify_http_response(resp_empty),
                   "Expected fatal code #{code} with empty header to classify as :response_fatal_bad_response"
    end
  end

  def test_classify_http_response_unknown_header_values
    ["absconded", "pending", "in_progress", "error", "unknown"].each do |unknown_val|
      resp_200 = Event::HttpResponse.new status: 200, headers: { "X-Goog-Upload-Status" => unknown_val }, body: ""
      assert_equal :response_fatal_bad_response, Rules.classify_http_response(resp_200)

      resp_400 = Event::HttpResponse.new status: 400, headers: { "X-Goog-Upload-Status" => unknown_val }, body: ""
      assert_equal :response_fatal_bad_response, Rules.classify_http_response(resp_400)
    end
  end

  def test_classify_http_response_header_key_casing
    keys = ["x-goog-upload-status", "X-GOOG-UPLOAD-STATUS", "X-Goog-Upload-Status", :"x-goog-upload-status"]
    keys.each do |key|
      resp = Event::HttpResponse.new status: 200, headers: { key => "active" }, body: ""
      assert_equal :response_active, Rules.classify_http_response(resp)
    end
  end

  def test_shape_of_control_events
    assert_equal :start_upload, Rules.shape_of(Event::StartUpload.new)
    assert_equal :start_upload, Rules.shape_of(Event::StartUpload)
    assert_equal :user_cancel, Rules.shape_of(Event::Cancel.new)
    assert_equal :user_cancel, Rules.shape_of(Event::Cancel)
    assert_equal :global_deadline_exceeded, Rules.shape_of(Event::GlobalDeadlineExceeded.new)
    assert_equal :global_deadline_exceeded, Rules.shape_of(Event::GlobalDeadlineExceeded)
  end

  def test_shape_of_chunk_read
    full_chunk = Event::ChunkRead.new bytes_buffered: 4096, eof: false
    assert_equal :chunk_read_full, Rules.shape_of(full_chunk)

    eof_data = Event::ChunkRead.new bytes_buffered: 1024, eof: true
    assert_equal :chunk_read_eof_with_data, Rules.shape_of(eof_data)

    eof_empty = Event::ChunkRead.new bytes_buffered: 0, eof: true
    assert_equal :chunk_read_eof_empty, Rules.shape_of(eof_empty)
  end

  def test_shape_of_request_failed
    exhausted = Event::RequestFailed.new kind: :retries_exhausted, message: "timeout"
    assert_equal :request_retries_exhausted, Rules.shape_of(exhausted)

    conn_failed = Event::RequestFailed.new kind: :connection_failed, message: "dropped"
    assert_equal :request_connection_failed, Rules.shape_of(conn_failed)

    other = Event::RequestFailed.new kind: :other, message: "unknown error"
    assert_equal :request_failed_unknown, Rules.shape_of(other)
  end

  def test_shape_of_http_response_delegates_to_classify
    resp = Event::HttpResponse.new status: 200, headers: { "X-Goog-Upload-Status" => "active" }, body: ""
    assert_equal :response_active, Rules.shape_of(resp)
  end

  def test_shape_of_unknown_event
    assert_equal :unknown, Rules.shape_of(Object.new)
    assert_equal :unknown, Rules.shape_of(nil)
    assert_equal :unknown, Rules.shape_of("unrecognized_event")
  end
end
