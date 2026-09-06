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
# Integration and unit tests for Driver logging concerns.
#
class DriverLoggingTest < Minitest::Test
  include Gapic::Rest::ResumableUpload

  FakeResponse = Struct.new :status, :headers, :body

  class FakeStub
    attr_reader :method_names

    def initialize responses
      @responses = responses
      @method_names = []
    end

    def endpoint
      "https://storage.googleapis.com"
    end

    def make_post_request uri:, body:, params:, options:, method_name: nil
      _ = uri
      _ = body
      _ = params
      _ = options
      @method_names << method_name
      @responses.shift
    end
  end

  def test_all_entries_share_upload_id_and_pass_method_names
    recording = RecordingLogger.new
    responses = [
      FakeResponse.new(
        200,
        {
          "X-Goog-Upload-Status" => "active",
          "X-Goog-Upload-URL"    => "https://storage.googleapis.com/session?id=123"
        },
        ""
      ),
      FakeResponse.new(
        200,
        { "X-Goog-Upload-Status" => "final" },
        "done"
      )
    ]

    stub = FakeStub.new responses
    config = CompleteUploadConfig.new(
      initial_url: "https://storage.googleapis.com/upload",
      stream:      StringIO.new("hello world"),
      upload_size: 11,
      chunk_size:  256
    )

    driver = Driver.new client_stub: stub, config: config, logger: recording
    driver.run

    refute_empty recording.entries
    upload_ids = recording.entries.map { |e| e.message.fields["uploadId"] }.uniq
    assert_equal 1, upload_ids.size
    refute_nil upload_ids.first

    assert_equal ["ResumableUpload.start", "ResumableUpload.upload"], stub.method_names
  end

  def test_unmatched_transition_logs_warn_and_reraises
    recording = RecordingLogger.new
    stub = FakeStub.new []
    config = CompleteUploadConfig.new(
      initial_url: "https://storage.googleapis.com/upload",
      stream:      StringIO.new("hello"),
      upload_size: 5,
      chunk_size:  256
    )

    failing_core = Minitest::Mock.new
    failing_core.expect :dispatch, nil do |_event|
      raise InvalidTransitionError, "unmatched transition in state"
    end
    failing_core.expect :state, State.new(status: :initializing)

    driver = Driver.new client_stub: stub, config: config, core: failing_core, logger: recording

    assert_raises InvalidTransitionError do
      driver.run
    end

    warn_entries = recording.entries.select { |e| e.severity == Logger::WARN }
    assert_equal 1, warn_entries.size
    fields = warn_entries.first.message.fields
    assert_equal "initializing", fields["status"]
    assert_equal "unmatched transition in state", fields["error"]
  end

  def test_redaction_of_payload_session_url_and_authorization_header
    recording = RecordingLogger.new
    sentinel_payload = "SECRET123_PAYLOAD_DATA"
    responses = [
      FakeResponse.new(
        200,
        {
          "X-Goog-Upload-Status" => "active",
          "X-Goog-Upload-URL"    => "https://storage.googleapis.com/session?sid=SECRET123"
        },
        ""
      ),
      FakeResponse.new(
        200,
        { "X-Goog-Upload-Status" => "final" },
        "done"
      )
    ]

    stub = FakeStub.new responses
    config = CompleteUploadConfig.new(
      initial_url:     "https://storage.googleapis.com/upload?key=SECRET123",
      initial_headers: { "Authorization" => "Bearer SECRET123" },
      stream:          StringIO.new(sentinel_payload),
      upload_size:     sentinel_payload.bytesize,
      chunk_size:      256
    )

    driver = Driver.new client_stub: stub, config: config, logger: recording
    driver.run

    recording.entries.each do |entry|
      full_dump = entry.message.to_s
      refute_includes full_dump, "SECRET123"
      refute_includes full_dump, sentinel_payload
    end
  end

  def test_total_logged_bytes_for_run_under_64kb
    recording = RecordingLogger.new
    chunk_data = "X" * 32_768
    responses = [
      FakeResponse.new(
        200,
        {
          "X-Goog-Upload-Status" => "active",
          "X-Goog-Upload-URL"    => "https://storage.googleapis.com/session?id=1"
        },
        ""
      ),
      FakeResponse.new(
        200,
        { "X-Goog-Upload-Status" => "final" },
        "done"
      )
    ]

    stub = FakeStub.new responses
    config = CompleteUploadConfig.new(
      initial_url: "https://storage.googleapis.com/upload",
      stream:      StringIO.new(chunk_data),
      upload_size: chunk_data.bytesize,
      chunk_size:  65_536
    )

    driver = Driver.new client_stub: stub, config: config, logger: recording
    driver.run

    total_bytes = recording.entries.sum { |e| e.message.to_s.bytesize }
    assert_operator total_bytes, :<, 65_536
  end
end
