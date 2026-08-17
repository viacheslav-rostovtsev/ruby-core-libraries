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

class CoreTest < Minitest::Test
  include Gapic::Rest::ResumableUpload

  def setup
    @config = CompleteUploadConfig.new(
      initial_url: "https://example.com/upload",
      stream:      StringIO.new("test content"),
      upload_size: 2048,
      chunk_size:  1024
    )
    @core = Core.new @config
  end

  def test_initial_state
    state = @core.state
    assert_equal :initializing, state.status
    assert_nil state.upload_url
    assert_equal 0, state.offset
    assert_equal 1024, state.chunk_size
    assert_nil state.chunk_granularity
    assert_equal 0, state.in_flight_length
    assert_nil state.last_error
  end

  def test_dispatch_updates_state_and_returns_instructions
    instructions = @core.dispatch Event::StartUpload.new
    assert_equal :starting, @core.state.status
    assert_equal 1, instructions.size
    assert_instance_of Instruction::SendStart, instructions.first

    resp = Event::HttpResponse.new(
      status:  200,
      headers: {
        "X-Goog-Upload-URL"               => "https://example.com/session/1",
        "X-Goog-Upload-Chunk-Granularity" => "512",
        "X-Goog-Upload-Status"            => "active"
      }
    )
    instructions = @core.dispatch resp
    assert_equal :transmission_reading, @core.state.status
    assert_equal "https://example.com/session/1", @core.state.upload_url
    assert_equal 512, @core.state.chunk_granularity
    assert_equal 1024, @core.state.chunk_size
    assert_equal 1, instructions.size
    assert_instance_of Instruction::FillBuffer, instructions.first
    assert_equal 1024, instructions.first.target_bytesize
  end
end
