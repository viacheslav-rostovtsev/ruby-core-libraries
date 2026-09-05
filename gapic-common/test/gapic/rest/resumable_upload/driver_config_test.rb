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
# Tests for ResumableUpload Driver configuration and deadline resolution.
#
class DriverConfigTest < Minitest::Test
  include Gapic::Rest::ResumableUpload

  # Fake client stub recording calls and yielding scripted responses.
  class FakeClientStub
    attr_reader :requests

    def initialize responses = []
      @responses = responses
      @requests = []
    end

    def make_post_request uri:, body:, params:, options:
      @requests << { uri: uri, body: body, params: params, options: options }
      raise "Unexpected request: no scripted response left" if @responses.empty?

      @responses.shift
    end
  end

  def test_resolve_timeout_prefers_positive_config_timeout
    stub = FakeClientStub.new
    config = CompleteUploadConfig.new(
      initial_url: "https://example.com/upload",
      stream:      StringIO.new("0123"),
      upload_size: 10 * 1_048_576,
      timeout:     42
    )
    driver = Driver.new client_stub: stub, config: config

    assert_equal 42, driver.send(:resolve_timeout)
  end

  def test_resolve_timeout_treats_zero_timeout_same_as_nil
    stub = FakeClientStub.new
    config = CompleteUploadConfig.new(
      initial_url: "https://example.com/upload",
      stream:      StringIO.new("0123"),
      timeout:     0
    )
    driver = Driver.new client_stub: stub, config: config

    assert_equal Driver::BASE_TIMEOUT, driver.send(:resolve_timeout)
  end

  def test_resolve_timeout_treats_negative_timeout_same_as_nil
    stub = FakeClientStub.new
    config = CompleteUploadConfig.new(
      initial_url: "https://example.com/upload",
      stream:      StringIO.new("0123"),
      timeout:     -10
    )
    driver = Driver.new client_stub: stub, config: config

    assert_equal Driver::BASE_TIMEOUT, driver.send(:resolve_timeout)
  end

  def test_resolve_timeout_calculates_from_upload_size_above_base_timeout
    stub = FakeClientStub.new
    large_size = 7_200 * Driver::MIN_ASSUMED_THROUGHPUT # 7200 seconds at 1MB/s
    config = CompleteUploadConfig.new(
      initial_url: "https://example.com/upload",
      stream:      StringIO.new("0123"),
      upload_size: large_size
    )
    driver = Driver.new client_stub: stub, config: config

    assert_in_delta 7_200.0, driver.send(:resolve_timeout), 0.001
  end

  def test_resolve_timeout_uses_base_timeout_floor_for_small_upload_size
    stub = FakeClientStub.new
    config = CompleteUploadConfig.new(
      initial_url: "https://example.com/upload",
      stream:      StringIO.new("0123"),
      upload_size: 1_048_576 # 1 second at 1MB/s < 3600
    )
    driver = Driver.new client_stub: stub, config: config

    assert_equal Driver::BASE_TIMEOUT, driver.send(:resolve_timeout)
  end

  def test_resolve_timeout_defaults_to_base_timeout_when_upload_size_nil
    stub = FakeClientStub.new
    config = CompleteUploadConfig.new(
      initial_url: "https://example.com/upload",
      stream:      StringIO.new("0123")
    )
    driver = Driver.new client_stub: stub, config: config

    assert_equal Driver::BASE_TIMEOUT, driver.send(:resolve_timeout)
  end

  def test_run_raises_deadline_exceeded_when_timeout_expires
    stub = FakeClientStub.new
    config = CompleteUploadConfig.new(
      initial_url: "https://example.com/upload",
      stream:      StringIO.new("0123"),
      upload_size: 4,
      chunk_size:  10,
      timeout:     5
    )
    driver = Driver.new client_stub: stub, config: config

    # Stub monotonic clock so that initial check sets deadline at t=105, and subsequent checks read t=110
    clock_ticks = [100.0, 110.0, 110.0]
    Process.stub :clock_gettime, ->(_clock_id) { clock_ticks.shift || 110.0 } do
      assert_raises Gapic::Common::DeadlineExceededError do
        driver.run
      end
    end
    assert_empty stub.requests
  end
end
