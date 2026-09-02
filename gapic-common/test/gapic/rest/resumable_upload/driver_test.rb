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
# Tests for ResumableUpload Driver synchronous execution engine.
#
class DriverTest < Minitest::Test
  include Gapic::Rest::ResumableUpload

  FakeResponse = Struct.new :status, :headers, :body, keyword_init: true

  # Fake client stub recording calls and yielding scripted responses.
  class FakeClientStub
    attr_reader :requests

    def initialize responses
      @responses = responses
      @requests = []
    end

    def make_post_request uri:, body:, params:, options:
      @requests << { uri: uri, body: body, params: params, options: options }
      raise "Unexpected request: no scripted response left" if @responses.empty?

      @responses.shift
    end
  end

  def test_multi_chunk_upload_with_active_responses
    stub = FakeClientStub.new build_scripted_responses
    config = CompleteUploadConfig.new(
      initial_url: "https://example.com/upload",
      stream:      StringIO.new("0123456789"),
      upload_size: 10,
      chunk_size:  4
    )

    driver = Driver.new client_stub: stub, config: config
    result = driver.run

    assert_equal '{"done":true}', result
    assert_equal 4, stub.requests.size
    assert_start_request stub.requests[0]
    assert_chunk_request stub.requests[1], offset: "0", length: "4", body: "0123", finalize: false
    assert_chunk_request stub.requests[2], offset: "4", length: "4", body: "4567", finalize: false
    assert_chunk_request stub.requests[3], offset: "8", length: "2", body: "89", finalize: true
  end

  def test_upload_recovers_when_chunk_response_lacks_status_header
    responses = build_recovery_responses
    stub = FakeClientStub.new responses
    config = CompleteUploadConfig.new(
      initial_url: "https://example.com/upload",
      stream:      StringIO.new("0123456789"),
      upload_size: 10,
      chunk_size:  4
    )

    driver = Driver.new client_stub: stub, config: config
    result = driver.run

    assert_equal '{"done":true}', result
    assert_equal 5, stub.requests.size
    assert_start_request stub.requests[0]
    assert_chunk_request stub.requests[1], offset: "0", length: "4", body: "0123", finalize: false
    assert_query_request stub.requests[2]
    assert_chunk_request stub.requests[3], offset: "4", length: "4", body: "4567", finalize: false
    assert_chunk_request stub.requests[4], offset: "8", length: "2", body: "89", finalize: true
  end

  def test_start_retries_when_response_lacks_status_header_even_on_200
    fast_policy = Gapic::Common::RetryPolicy.new(
      initial_delay: 0.001,
      max_delay:     0.002,
      timeout:       1.0,
      retry_predicate: lambda do |error_or_response|
        headers = RetryPolicies.extract_headers error_or_response
        if headers
          status_hdr = headers["x-goog-upload-status"] || headers["X-Goog-Upload-Status"]
          return true if status_hdr.nil? || status_hdr.empty?
        end
        nil
      end
    )
    responses = [
      # First start response: 200 OK but NO X-Goog-Upload-Status header
      FakeResponse.new(status: 200, headers: {}, body: ""),
      # Second start response: valid 200 OK with active header
      FakeResponse.new(
        status:  200,
        headers: {
          "X-Goog-Upload-URL"    => "https://example.com/session/1",
          "X-Goog-Upload-Status" => "active"
        },
        body:    ""
      ),
      FakeResponse.new(status: 200, headers: { "X-Goog-Upload-Status" => "final" }, body: '{"done":true}')
    ]
    stub = FakeClientStub.new responses
    config = CompleteUploadConfig.new(
      initial_url:        "https://example.com/upload",
      stream:             StringIO.new("0123"),
      upload_size:        4,
      chunk_size:         10,
      start_retry_policy: fast_policy
    )

    driver = Driver.new client_stub: stub, config: config
    result = driver.run

    assert_equal '{"done":true}', result
    # 2 start requests + 1 chunk request = 3 requests
    assert_equal 3, stub.requests.size
    assert_start_request stub.requests[0]
    assert_start_request stub.requests[1]
    assert_chunk_request stub.requests[2], offset: "0", length: "4", body: "0123", finalize: true
  end

  def test_start_exhausts_retries_when_responses_continually_lack_status_header
    exhausting_policy = Gapic::Common::RetryPolicy.new(
      initial_delay: 0.001,
      max_delay:     0.002,
      timeout:       0.01,
      retry_predicate: lambda do |error_or_response|
        headers = RetryPolicies.extract_headers error_or_response
        if headers
          status_hdr = headers["x-goog-upload-status"] || headers["X-Goog-Upload-Status"]
          return true if status_hdr.nil? || status_hdr.empty?
        end
        nil
      end
    )
    # Return 200 OK without status headers repeatedly
    responses = Array.new(10) { FakeResponse.new status: 200, headers: {}, body: "" }
    stub = FakeClientStub.new responses
    config = CompleteUploadConfig.new(
      initial_url:        "https://example.com/upload",
      stream:             StringIO.new("0123"),
      upload_size:        4,
      chunk_size:         10,
      start_retry_policy: exhausting_policy
    )

    driver = Driver.new client_stub: stub, config: config
    err = assert_raises Gapic::Common::BadResponseError do
      driver.run
    end

    assert_match(/Missing X-Goog-Upload-Status/, err.message)
    assert stub.requests.size > 1
  end

  def test_query_does_not_retry_on_missing_status_header_in_driver
    responses = [
      FakeResponse.new(
        status:  200,
        headers: {
          "X-Goog-Upload-URL"    => "https://example.com/session/1",
          "X-Goog-Upload-Status" => "active"
        },
        body:    ""
      ),
      # Chunk returns 503 without status header -> triggers Category 2 recovery
      FakeResponse.new(status: 503, headers: {}, body: "Service Unavailable"),
      # Query returns 200 with missing status header -> Core handles recovery retry,
      # Driver does not retry query internally
      FakeResponse.new(status: 200, headers: {}, body: ""),
      # Next query succeeds
      FakeResponse.new(
        status:  200,
        headers: {
          "X-Goog-Upload-Status"        => "active",
          "X-Goog-Upload-Size-Received" => "0"
        },
        body:    ""
      ),
      FakeResponse.new(status: 200, headers: { "X-Goog-Upload-Status" => "final" }, body: '{"done":true}')
    ]
    stub = FakeClientStub.new responses
    config = CompleteUploadConfig.new(
      initial_url: "https://example.com/upload",
      stream:      StringIO.new("0123"),
      upload_size: 4,
      chunk_size:  10
    )

    driver = Driver.new client_stub: stub, config: config
    result = driver.run

    assert_equal '{"done":true}', result
    # Verify exact sequence: start, chunk, query1, query2, chunk
    assert_equal 5, stub.requests.size
    assert_start_request stub.requests[0]
    assert_chunk_request stub.requests[1], offset: "0", length: "4", body: "0123", finalize: true
    assert_query_request stub.requests[2]
    assert_query_request stub.requests[3]
    assert_chunk_request stub.requests[4], offset: "0", length: "4", body: "0123", finalize: true
  end

  private

  def build_scripted_responses
    [
      FakeResponse.new(
        status:  200,
        headers: {
          "X-Goog-Upload-URL"    => "https://example.com/session/1",
          "X-Goog-Upload-Status" => "active"
        },
        body:    ""
      ),
      FakeResponse.new(status: 200, headers: { "X-Goog-Upload-Status" => "active" }, body: ""),
      FakeResponse.new(status: 200, headers: { "X-Goog-Upload-Status" => "active" }, body: ""),
      FakeResponse.new(status: 200, headers: { "X-Goog-Upload-Status" => "final" }, body: '{"done":true}')
    ]
  end

  def build_recovery_responses
    [
      FakeResponse.new(
        status:  200,
        headers: {
          "X-Goog-Upload-URL"    => "https://example.com/session/1",
          "X-Goog-Upload-Status" => "active"
        },
        body:    ""
      ),
      FakeResponse.new(status: 503, headers: {}, body: "Service Unavailable"),
      FakeResponse.new(
        status:  200,
        headers: {
          "X-Goog-Upload-Status"        => "active",
          "X-Goog-Upload-Size-Received" => "4"
        },
        body:    ""
      ),
      FakeResponse.new(status: 200, headers: { "X-Goog-Upload-Status" => "active" }, body: ""),
      FakeResponse.new(status: 200, headers: { "X-Goog-Upload-Status" => "final" }, body: '{"done":true}')
    ]
  end

  def assert_start_request req
    assert_equal "https://example.com/upload", req[:uri]
    assert_equal "start", req[:options][:metadata]["X-Goog-Upload-Command"]
  end

  def assert_query_request req
    assert_equal "https://example.com/session/1", req[:uri]
    assert_equal "query", req[:options][:metadata]["X-Goog-Upload-Command"]
  end

  def assert_chunk_request req, offset:, length:, body:, finalize:
    expected_cmd = finalize ? "upload, finalize" : "upload"
    metadata = req[:options][:metadata]
    assert_equal "https://example.com/session/1", req[:uri]
    assert_equal expected_cmd, metadata["X-Goog-Upload-Command"]
    assert_equal offset, metadata["X-Goog-Upload-Offset"]
    assert_equal length, metadata["Content-Length"]
    assert_equal body, req[:body]
  end
end
