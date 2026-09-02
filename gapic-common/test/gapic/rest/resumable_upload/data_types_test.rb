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
# Tests for data types in resumable upload.
#
class DataTypesTest < Minitest::Test
  include Gapic::Rest::ResumableUpload

  def test_complete_upload_config_defaults
    stream = StringIO.new "content"
    config = CompleteUploadConfig.new(
      initial_url: "https://example.com",
      stream:      stream
    )

    assert_equal "https://example.com", config.initial_url
    assert_same stream, config.stream
    assert_nil config.initial_body
    assert_equal({}, config.initial_headers)
    assert_nil config.upload_size
    assert_nil config.chunk_size
    assert_nil config.content_type
    assert_nil config.deadline
    assert_nil config.start_retry_policy
    assert_nil config.control_plane_retry_policy
    assert_nil config.data_plane_retry_policy
    assert_nil config.user_override_start_retry_policy
    assert_nil config.on_progress
  end

  def test_state_defaults_and_with
    state = State.new

    assert_equal :initializing, state.status
    assert_nil state.upload_url
    assert_equal 0, state.offset
    assert_equal 8_388_608, state.chunk_size
    assert_nil state.chunk_granularity
    assert_equal 0, state.in_flight_length
    assert_nil state.last_error

    modified = state.with status: :starting, upload_url: "https://example.com/upload"
    assert_equal :starting, modified.status
    assert_equal "https://example.com/upload", modified.upload_url
    assert_equal :initializing, state.status
  end

  def test_event_instantiation
    start_event = Event::StartUpload.new
    assert_instance_of Event::StartUpload, start_event

    chunk = Event::ChunkRead.new bytes_buffered: 100, eof: true
    assert_equal 100, chunk.bytes_buffered
    assert chunk.eof

    http = Event::HttpResponse.new status: 200, headers: { "a" => "b" }, body: "body"
    assert_equal 200, http.status
    assert_equal({ "a" => "b" }, http.headers)
    assert_equal "body", http.body

    req_fail = Event::RequestFailed.new kind: :connection_failed, message: "err"
    assert_equal :connection_failed, req_fail.kind
    assert_equal "err", req_fail.message
  end

  def test_instruction_instantiation
    start = Instruction::SendStart.new url: "https://example.com"
    assert_equal "https://example.com", start.url
    assert_equal({}, start.headers)
    assert_nil start.body

    chunk = Instruction::SendChunk.new url: "https://example.com", offset: 0, length: 100
    assert_equal 0, chunk.offset
    assert_equal 100, chunk.length
    refute chunk.finalize

    realign = Instruction::RealignBuffer.new server_offset: 500
    assert_equal 500, realign.server_offset
  end
end
