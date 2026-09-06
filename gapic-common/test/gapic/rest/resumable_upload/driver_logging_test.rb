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
# Tests for ResumableUpload Driver structured logging across lifecycle, decisions,
# wire exchanges, buffer realignments, and data abridging.
#
class DriverLoggingTest < Minitest::Test
  include Gapic::Rest::ResumableUpload

  FakeHttpResponse = Struct.new :status, :headers, :body

  class RecordingLogger < Logger
    attr_reader :entries

    def initialize
      super(StringIO.new)
      @entries = []
    end

    def add severity, message = nil, progname = nil
      severity ||= UNKNOWN
      if message.nil?
        if block_given?
          message = yield
        else
          message = progname
          progname = @progname
        end
      end
      @entries << { severity: severity, message: message, progname: progname }
      true
    end
  end

  class ScriptedClientStub
    attr_reader :requests, :logger, :endpoint

    def initialize responses, logger: nil, endpoint: "https://storage.googleapis.com"
      @responses = responses.dup
      @requests = []
      @logger = logger
      @endpoint = endpoint
    end

    def make_post_request uri:, body: nil, params: {}, options: {}, method_name: nil
      @requests << { uri: uri, body: body, params: params, options: options, method_name: method_name }
      raise "No scripted response left" if @responses.empty?

      resp = @responses.shift
      raise resp if resp.is_a? Exception

      resp
    end
  end

  class UnseekableStream
    def initialize data
      @io = StringIO.new data
    end

    def read length = nil
      @io.read length
    end
  end

  def test_logs_decision_lifecycle_and_wire_entries_with_upload_id_and_method_name
    logger = RecordingLogger.new
    # 100 bytes data so body > 64 bytes triggers abridging
    payload = "A" * 100
    stream = StringIO.new payload

    responses = [
      FakeHttpResponse.new(
        200,
        {
          "X-Goog-Upload-Status"            => "active",
          "X-Goog-Upload-URL"               => "https://storage.googleapis.com/upload/session?upload_id=secret123&part=1",
          "X-Goog-Upload-Chunk-Granularity" => "256",
          "X-Secret-Header"                 => "super-secret-value"
        },
        ""
      ),
      FakeHttpResponse.new(
        200,
        { "X-Goog-Upload-Status" => "final" },
        '{"id":"obj-1"}'
      )
    ]

    stub = ScriptedClientStub.new responses, logger: logger
    config = CompleteUploadConfig.new(
      initial_url:     "https://storage.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=resumable&key=secretKey",
      initial_headers: { "Authorization" => "Bearer secret-token", "X-Goog-Upload-Header-Content-Length" => "100" },
      initial_body:    '{"name":"test.bin"}',
      stream:          stream,
      upload_size:     100,
      chunk_size:      256
    )

    driver = Driver.new client_stub: stub, config: config
    result = driver.run

    assert_equal '{"id":"obj-1"}', result
    refute_empty logger.entries

    # Verify method_name passed to make_post_request
    assert_equal "ResumableUpload.start", stub.requests[0][:method_name]
    assert_equal "ResumableUpload.upload", stub.requests[1][:method_name]

    # Every log entry must have uploadId matching driver.upload_id and be a Google::Logging::Message
    logger.entries.each do |entry|
      msg = entry[:message]
      assert_instance_of Google::Logging::Message, msg
      assert_equal driver.upload_id, msg.fields["uploadId"]
      refute_nil driver.upload_id
    end

    # Verify Decision logs (DEBUG)
    decision_entries = logger.entries.select { |e| e[:message].message.start_with?("Rules:") }
    assert_equal 4, decision_entries.size

    first_decision = decision_entries.first[:message]
    assert_equal Logger::DEBUG, decision_entries.first[:severity]
    assert_equal "Rules: initializing + start_upload -> start_session -> starting", first_decision.message
    assert_equal "initializing", first_decision.fields["fromStatus"]
    assert_equal "start_upload", first_decision.fields["shape"]
    assert_equal "start_session", first_decision.fields["recipe"]
    assert_equal "starting", first_decision.fields["toStatus"]
    # Instructions in decision logs must be summaries without raw bodies
    inst_summary = first_decision.fields["instructions"].first
    assert_equal "SendStart", inst_summary["type"]
    assert_equal "https://storage.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=<...>&key=<...>",
                 inst_summary["url"]
    refute inst_summary.key?("body")

    # Verify Lifecycle logs: start_session (INFO), begin_transmission (INFO), send_upload_finalize (INFO), complete (INFO)
    info_entries = logger.entries.select { |e| e[:severity] == Logger::INFO }
    info_messages = info_entries.map { |e| e[:message].message }
    assert_includes info_messages, "Initiating resumable upload"
    assert_includes info_messages, "Upload session established"
    assert_includes info_messages, "Sending final upload chunk and finalizing"
    assert_includes info_messages, "Resumable upload completed"

    # Verify Wire logs (DEBUG): URL query values elided, non-X-Goog-Upload headers elided, >64B chunk body abridged
    send_entries = logger.entries.select { |e| e[:message].message.start_with?("Sending ") }
    upload_send = send_entries.find { |e| e[:message].fields["command"] == "upload, finalize" }[:message]
    assert_equal "https://storage.googleapis.com/upload/session?upload_id=<...>&part=<...>", upload_send.fields["url"]
    assert_equal 1, upload_send.fields["retryAttempt"]
    assert_match(/^<100 bytes; first 32: A{32}>$/, upload_send.fields["body"])

    # Verify Authorization header is elided in wire logs
    start_send = send_entries.find { |e| e[:message].fields["command"] == "start" }[:message]
    assert_equal "<...>", start_send.fields["headers"]["Authorization"]
    assert_equal "100", start_send.fields["headers"]["X-Goog-Upload-Header-Content-Length"]

    # Verify Received wire logs
    recv_entries = logger.entries.select { |e| e[:message].message.start_with?("Received ") }
    first_recv = recv_entries.first[:message]
    assert_equal 200, first_recv.fields["httpStatus"]
    assert_equal "active", first_recv.fields["uploadStatus"]
    assert_equal 256, first_recv.fields["granularity"]
    assert_equal "<...>", first_recv.fields["headers"]["X-Secret-Header"]
  end

  def test_send_chunk_lifecycle_is_logged_at_debug_level_only
    logger = RecordingLogger.new
    stream = StringIO.new("A" * 512)

    responses = [
      FakeHttpResponse.new(
        200,
        {
          "X-Goog-Upload-Status" => "active",
          "X-Goog-Upload-URL"    => "https://storage.googleapis.com/session"
        },
        ""
      ),
      FakeHttpResponse.new(
        200,
        { "X-Goog-Upload-Status" => "active" },
        ""
      ),
      FakeHttpResponse.new(
        200,
        { "X-Goog-Upload-Status" => "active" },
        ""
      ),
      FakeHttpResponse.new(
        200,
        { "X-Goog-Upload-Status" => "final" },
        '{"done":true}'
      )
    ]

    stub = ScriptedClientStub.new responses, logger: logger
    config = CompleteUploadConfig.new(
      initial_url: "https://storage.googleapis.com/upload",
      stream:      stream,
      upload_size: 512,
      chunk_size:  256
    )

    driver = Driver.new client_stub: stub, config: config
    driver.run

    send_chunk_entries = logger.entries.select do |e|
      e[:message].fields["recipe"] == "send_chunk" && !e[:message].message.start_with?("Rules:")
    end
    refute_empty send_chunk_entries
    send_chunk_entries.each do |entry|
      assert_equal Logger::DEBUG, entry[:severity]
    end
  end

  def test_realign_within_buffer_logs_debug_and_unseekable_rewind_logs_warn
    logger = RecordingLogger.new
    stream = UnseekableStream.new("A" * 512)

    responses = [
      FakeHttpResponse.new(
        200,
        {
          "X-Goog-Upload-Status" => "active",
          "X-Goog-Upload-URL"    => "https://storage.googleapis.com/session"
        },
        ""
      ),
      # First chunk send fails with 503 triggering recovery
      FakeHttpResponse.new(503, {}, "Backend error"),
      # Query response asks to rewind before buffer start (0) when buffer_start is 0 -> test within_buffer first
      FakeHttpResponse.new(
        200,
        {
          "X-Goog-Upload-Status"        => "active",
          "X-Goog-Upload-Size-Received" => "128"
        },
        ""
      ),
      # Next chunk fails with 503 triggering recovery
      FakeHttpResponse.new(503, {}, "Backend error"),
      # Query response asks to rewind to offset 0 (which is behind buffer_start_offset 128 on unseekable stream)
      FakeHttpResponse.new(
        200,
        {
          "X-Goog-Upload-Status"        => "active",
          "X-Goog-Upload-Size-Received" => "0"
        },
        ""
      )
    ]

    stub = ScriptedClientStub.new responses, logger: logger
    config = CompleteUploadConfig.new(
      initial_url: "https://storage.googleapis.com/upload",
      stream:      stream,
      upload_size: 512,
      chunk_size:  256
    )

    driver = Driver.new client_stub: stub, config: config
    assert_raises UnseekableStreamError do
      driver.run
    end

    realign_entries = logger.entries.select { |e| e[:message].fields.key?("realignCase") }
    assert_equal 2, realign_entries.size
    assert_equal "within_buffer", realign_entries[0][:message].fields["realignCase"]
    assert_equal 128, realign_entries[0][:message].fields["offset"]

    assert_equal "rewind", realign_entries[1][:message].fields["realignCase"]
    assert_equal 0, realign_entries[1][:message].fields["offset"]

    warn_entries = logger.entries.select { |e| e[:severity] == Logger::WARN }
    assert_equal 1, warn_entries.size
    assert_match(/Cannot rewind unseekable stream to offset 0/, warn_entries.first[:message].message)
  end
end
