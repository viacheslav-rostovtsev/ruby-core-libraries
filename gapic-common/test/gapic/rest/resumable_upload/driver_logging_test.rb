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

    info_recipes = recording.entries.select { |e| e.severity == Logger::INFO }.map { |e| e.message.fields["recipe"] }
    assert_includes ["complete_upload_with_data", "complete_upload_finalized"], info_recipes.last
  end

  def test_multi_chunk_upload_logs_ack_chunk_and_completion
    recording = RecordingLogger.new
    run_two_chunk_upload_with_secret recording

    info_recipes = recording.entries.select { |e| e.severity == Logger::INFO }.map { |e| e.message.fields["recipe"] }
    assert_includes info_recipes, "ack_chunk"
    assert(info_recipes.any? { |r| ["complete_upload_with_data", "complete_upload_finalized"].include? r })
  end

  def test_recovery_scenario_logs_enter_recovery_and_realign
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
      FakeResponse.new(503, {}, "Service Unavailable"),
      FakeResponse.new(
        200,
        {
          "X-Goog-Upload-Status"        => "active",
          "X-Goog-Upload-Size-Received" => "0"
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

    info_recipes = recording.entries.select { |e| e.severity == Logger::INFO }.map { |e| e.message.fields["recipe"] }
    assert_includes info_recipes, "enter_recovery"
    assert_includes info_recipes, "realign_from_recovery"
  end

  def test_fatal_failure_logs_warn_with_fail_with_recipe
    recording = RecordingLogger.new
    responses = [
      FakeResponse.new(
        403,
        { "X-Goog-Upload-Status" => "final" },
        "Forbidden"
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
    assert_raises Gapic::Common::UploadRejectedError do
      driver.run
    end

    warn_entries = recording.entries.select { |e| e.severity == Logger::WARN }
    refute_empty warn_entries
    assert(warn_entries.any? { |e| e.message.fields["recipe"]&.start_with? "fail_with_" })
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

  def test_full_log_corpus_redacts_secrets
    recording = RecordingLogger.new
    run_two_chunk_upload_with_secret recording

    corpus = log_corpus recording
    refute_includes corpus, "SECRET-123456"
  end

  def test_full_log_corpus_size_under_64kib
    recording = RecordingLogger.new
    run_two_chunk_upload_with_secret recording

    corpus = log_corpus recording
    assert_operator corpus.bytesize, :<, 65_536
  end

  private

  def run_two_chunk_upload_with_secret recording
    chunk_size = 8 * 1024 * 1024
    secret = "SECRET-123456"
    binary_prefix = "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09".b

    half = (chunk_size / 2) - 10
    chunk1 = binary_prefix + ("A" * half) + secret + ("A" * (chunk_size - 10 - half - secret.bytesize))
    chunk2 = binary_prefix + ("A" * (chunk_size - 10))
    stream_data = chunk1 + chunk2

    responses = [
      FakeResponse.new(
        200,
        {
          "X-Goog-Upload-Status" => "active",
          "X-Goog-Upload-URL"    => "https://storage.googleapis.com/session?sid=#{secret}"
        },
        ""
      ),
      FakeResponse.new(
        200,
        {
          "X-Goog-Upload-Status"        => "active",
          "X-Goog-Upload-Size-Received" => chunk_size.to_s
        },
        ""
      ),
      FakeResponse.new(
        200,
        {
          "X-Goog-Upload-Status"        => "active",
          "X-Goog-Upload-Size-Received" => (chunk_size * 2).to_s
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
      initial_url:     "https://storage.googleapis.com/upload?token=#{secret}",
      initial_headers: { "Authorization" => "Bearer #{secret}" },
      stream:          StringIO.new(stream_data),
      upload_size:     stream_data.bytesize,
      chunk_size:      chunk_size
    )

    driver = Driver.new client_stub: stub, config: config, logger: recording
    driver.run
  end
end
