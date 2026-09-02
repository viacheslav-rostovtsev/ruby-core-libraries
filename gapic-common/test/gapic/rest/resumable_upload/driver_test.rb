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

    assert_equal '{"done":true}', result.body
    assert_equal 4, stub.requests.size
    assert_start_request stub.requests[0]
    assert_chunk_request stub.requests[1], offset: "0", length: "4", body: "0123", finalize: false
    assert_chunk_request stub.requests[2], offset: "4", length: "4", body: "4567", finalize: false
    assert_chunk_request stub.requests[3], offset: "8", length: "2", body: "89", finalize: true
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

  def assert_start_request req
    assert_equal "https://example.com/upload", req[:uri]
    assert_equal "start", req[:options][:metadata]["X-Goog-Upload-Command"]
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
