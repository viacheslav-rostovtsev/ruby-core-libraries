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

class SessionTest < Minitest::Test
  include Gapic::Rest::ResumableUpload

  FakeResponse = Struct.new :status, :headers, :body, keyword_init: true

  class ScriptedClientStub
    attr_reader :requests

    def initialize responses = []
      @responses = responses.dup
      @requests = []
    end

    def make_post_request uri:, body:, params:, options:, method_name: nil
      @requests << { uri: uri, body: body, params: params, options: options, method_name: method_name }
      raise "Unexpected request: no scripted response left" if @responses.empty?

      res = @responses.shift
      if res.is_a? Proc
        res.call
      elsif res.is_a? Exception
        raise res
      else
        res
      end
    end
  end

  class UnseekableStream
    attr_reader :pos

    def initialize string
      @io = StringIO.new string
      @pos = 0
    end

    def read length = nil
      chunk = @io.read length
      @pos += chunk.bytesize if chunk
      chunk
    end
  end

  def build_session stub: nil, stream: nil, upload_size: 10, chunk_size: 4, **kwargs
    stream ||= StringIO.new "0123456789"
    stub ||= ScriptedClientStub.new
    Session.new(
      client_stub:  stub,
      stream:       stream,
      initial_url:  "https://example.com/initiate",
      initial_body: '{"name":"test.txt"}',
      upload_size:  upload_size,
      chunk_size:   chunk_size,
      **kwargs
    )
  end

  # ============================================================================
  # 1. Initialization and argument validation
  # ============================================================================

  def test_initialize_mandatory_arguments
    assert_raises ArgumentError do
      Session.new stream: StringIO.new, initial_url: "http://x", initial_body: ""
    end

    assert_raises ArgumentError do
      Session.new client_stub: ScriptedClientStub.new, initial_url: "http://x", initial_body: ""
    end

    assert_raises ArgumentError do
      Session.new client_stub: ScriptedClientStub.new, stream: StringIO.new, initial_body: ""
    end

    assert_raises ArgumentError do
      Session.new client_stub: ScriptedClientStub.new, stream: StringIO.new, initial_url: "http://x"
    end
  end

  def test_initialize_defaults
    session = Session.new(
      client_stub:  ScriptedClientStub.new,
      stream:       StringIO.new("abc"),
      initial_url:  "https://example.com/initiate",
      initial_body: "",
      upload_size:  300
    )

    assert_equal 300, session.upload_size
    assert_equal({}, session.initial_headers)
    assert_nil session.chunk_size
    assert_nil session.content_type
    assert_nil session.timeout
    assert_nil session.start_retry_policy
    assert_nil session.control_plane_retry_policy
    assert_nil session.data_plane_retry_policy
    assert_nil session.on_progress
    assert_nil session.logger
  end

  # ============================================================================
  # 2. Observable States: Unbound
  # ============================================================================

  def test_initial_unbound_state
    session = build_session
    refute session.bound?
    assert_nil session.upload_url
    assert_nil session.resume_handle
    refute session.resumable?
    refute session.running?
  end

  def test_bare_resume_on_unbound_session_raises_session_state_error
    session = build_session
    err = assert_raises SessionStateError do
      session.resume
    end
    assert_includes err.message, "Cannot resume unbound session without resume_handle or upload_url"
  end

  # ============================================================================
  # 3. Start Lifecycle & Bound Transitions
  # ============================================================================

  def test_start_successful_upload_transitions_to_bound_dead
    responses = [
      # Initiation response
      FakeResponse.new(
        status:  200,
        headers: {
          "x-goog-upload-status"            => "active",
          "x-goog-upload-url"               => "https://upload.example.com/session_1",
          "x-goog-upload-chunk-granularity" => "4"
        },
        body:    ""
      ),
      # Chunk 1 (0-3)
      FakeResponse.new(
        status:  200,
        headers: { "x-goog-upload-status" => "active" },
        body:    ""
      ),
      # Chunk 2 (4-7)
      FakeResponse.new(
        status:  200,
        headers: { "x-goog-upload-status" => "active" },
        body:    ""
      ),
      # Final Chunk (8-9)
      FakeResponse.new(
        status:  200,
        headers: { "x-goog-upload-status" => "final" },
        body:    '{"status":"completed"}'
      )
    ]

    stub = ScriptedClientStub.new responses
    session = build_session stub: stub, upload_size: 10, chunk_size: 4

    result = session.start

    assert_equal '{"status":"completed"}', result
    assert session.bound?
    assert_equal "https://upload.example.com/session_1", session.upload_url
    refute session.running?
    refute session.resumable?
    assert_nil session.resume_handle
  end

  def test_start_when_already_bound_raises_session_state_error
    responses = [
      FakeResponse.new(
        status:  200,
        headers: {
          "x-goog-upload-status"            => "active",
          "x-goog-upload-url"               => "https://upload.example.com/session_1",
          "x-goog-upload-chunk-granularity" => "4"
        },
        body:    ""
      ),
      FakeResponse.new(
        status:  200,
        headers: { "x-goog-upload-status" => "final" },
        body:    '{"done":true}'
      )
    ]
    session = build_session(
      stub:        ScriptedClientStub.new(responses),
      stream:      StringIO.new("01"),
      upload_size: 2,
      chunk_size:  4
    )
    session.start

    assert session.bound?

    err = assert_raises SessionStateError do
      session.start
    end
    assert_includes err.message, "Session is already bound to an upload"
  end

  # ============================================================================
  # 4. Resume Forms: Three Mutually Exclusive Forms
  # ============================================================================

  def test_resume_form1_bare_resume_on_bound_alive_session
    # First run fails during recovery query with connection failure
    stub = ScriptedClientStub.new [
      # Initiation succeeds -> binds session to upload URL
      FakeResponse.new(
        status:  200,
        headers: {
          "x-goog-upload-status"            => "active",
          "x-goog-upload-url"               => "https://upload.example.com/session_1",
          "x-goog-upload-chunk-granularity" => "4"
        },
        body:    ""
      ),
      # Chunk 1 returns 503 -> triggers Category 2 recovery
      FakeResponse.new(status: 503, headers: {}, body: "Service Unavailable"),
      # Recovery query fails with connection error -> raises RequestFailedError
      Faraday::ConnectionFailed.new("network connection failed")
    ]

    session = build_session stub: stub, upload_size: 10, chunk_size: 4

    raised = assert_raises RequestFailedError do
      session.start
    end

    assert_includes raised.message, "(upload session is resumable: see #resume_handle)"
    refute_nil raised.resume_handle
    # State: Bound and Alive
    assert session.bound?
    assert session.resumable?
    refute session.running?
    assert_equal "https://upload.example.com/session_1", session.upload_url
    assert_equal "https://upload.example.com/session_1", session.resume_handle.upload_url

    # Second run: bare resume continues the bound upload
    recovery_responses = [
      # Query response
      FakeResponse.new(
        status:  200,
        headers: {
          "x-goog-upload-status"        => "active",
          "x-goog-upload-size-received" => "0"
        },
        body:    ""
      ),
      # Chunk 1
      FakeResponse.new(
        status:  200,
        headers: { "x-goog-upload-status" => "active" },
        body:    ""
      ),
      # Chunk 2
      FakeResponse.new(
        status:  200,
        headers: { "x-goog-upload-status" => "active" },
        body:    ""
      ),
      # Chunk 3 (final)
      FakeResponse.new(
        status:  200,
        headers: { "x-goog-upload-status" => "final" },
        body:    '{"resumed":true}'
      )
    ]
    stub.instance_variable_set :@responses, recovery_responses

    result = session.resume
    assert_equal '{"resumed":true}', result
    assert session.bound?
    refute session.resumable?
  end

  def test_resume_form1_bare_resume_on_seekable_stream_without_manual_rewind
    stub = ScriptedClientStub.new [
      FakeResponse.new(
        status:  200,
        headers: {
          "x-goog-upload-status"            => "active",
          "x-goog-upload-url"               => "https://upload.example.com/session_bare",
          "x-goog-upload-chunk-granularity" => "4"
        },
        body:    ""
      ),
      FakeResponse.new(status: 503, headers: {}, body: "Service Unavailable"),
      Faraday::ConnectionFailed.new("network connection failed")
    ]
    session = build_session stub: stub, stream: StringIO.new("01"), upload_size: 2, chunk_size: 4
    assert_raises RequestFailedError do
      session.start
    end

    assert session.bound?
    assert session.resumable?

    stub.instance_variable_set :@responses, [
      FakeResponse.new(
        status:  200,
        headers: {
          "x-goog-upload-status"        => "active",
          "x-goog-upload-size-received" => "0"
        },
        body:    ""
      ),
      FakeResponse.new(status: 200, headers: { "x-goog-upload-status" => "final" }, body: '{"resumed_bare":true}')
    ]

    result = session.resume
    assert_equal '{"resumed_bare":true}', result
    assert session.bound?
    refute session.resumable?
  end

  def test_resume_form1_bare_resume_on_unseekable_stream_derives_offset
    stream = UnseekableStream.new "0123456789"
    stub = ScriptedClientStub.new [
      # Initiation
      FakeResponse.new(
        status:  200,
        headers: {
          "x-goog-upload-status"            => "active",
          "x-goog-upload-url"               => "https://upload.example.com/session_unseekable",
          "x-goog-upload-chunk-granularity" => "4"
        },
        body:    ""
      ),
      # Chunk 1 (0-3) returns 503 -> recovery
      FakeResponse.new(status: 503, headers: {}, body: "Service Unavailable"),
      # Recovery query fails -> RequestFailedError
      Faraday::ConnectionFailed.new("network connection failed")
    ]

    session = build_session stub: stub, stream: stream, upload_size: 10, chunk_size: 4
    assert_raises RequestFailedError do
      session.start
    end

    assert session.bound?
    assert session.resumable?
    assert_equal 4, stream.pos

    # Script responses for bare resume
    stub.instance_variable_set :@responses, [
      # Recovery query on resume: server acknowledges 4 bytes received
      FakeResponse.new(
        status:  200,
        headers: {
          "x-goog-upload-status"        => "active",
          "x-goog-upload-size-received" => "4"
        },
        body:    ""
      ),
      # Chunk 2 (4-7)
      FakeResponse.new(status: 200, headers: { "x-goog-upload-status" => "active" }, body: ""),
      # Final chunk (8-9)
      FakeResponse.new(status: 200, headers: { "x-goog-upload-status" => "final" }, body: '{"unseekable_resumed":true}')
    ]

    result = session.resume
    assert_equal '{"unseekable_resumed":true}', result
    assert_equal 10, stream.pos
    assert session.bound?
    refute session.resumable?
    # Verify derived stream_offset was 4 in the resume driver config
    assert_equal 4, session.instance_variable_get(:@last_driver).instance_variable_get(:@config).stream_offset
  end

  def test_resume_form2_explicit_url_and_chunk_size_binds_unbound_session
    responses = [
      FakeResponse.new(
        status:  200,
        headers: {
          "x-goog-upload-status"        => "active",
          "x-goog-upload-size-received" => "0"
        },
        body:    ""
      ),
      FakeResponse.new(
        status:  200,
        headers: { "x-goog-upload-status" => "final" },
        body:    '{"from_url":true}'
      )
    ]
    stub = ScriptedClientStub.new responses
    session = build_session stub: stub, stream: StringIO.new("01"), upload_size: 2, chunk_size: 4

    refute session.bound?
    result = session.resume upload_url: "https://upload.example.com/direct", chunk_size: 4

    assert_equal '{"from_url":true}', result
    assert session.bound?
    assert_equal "https://upload.example.com/direct", session.upload_url
  end

  def test_resume_form3_resume_handle_binds_unbound_session
    responses = [
      FakeResponse.new(
        status:  200,
        headers: {
          "x-goog-upload-status"        => "active",
          "x-goog-upload-size-received" => "0"
        },
        body:    ""
      ),
      FakeResponse.new(
        status:  200,
        headers: { "x-goog-upload-status" => "final" },
        body:    '{"from_handle":true}'
      )
    ]
    stub = ScriptedClientStub.new responses
    session = build_session stub: stub, stream: StringIO.new("01"), upload_size: 2, chunk_size: 4

    handle = ResumeHandle.new upload_url: "https://upload.example.com/from_handle", chunk_size: 4

    refute session.bound?
    result = session.resume resume_handle: handle

    assert_equal '{"from_handle":true}', result
    assert session.bound?
    assert_equal "https://upload.example.com/from_handle", session.upload_url
  end

  # ============================================================================
  # 5. Resume Argument Shape & Lifecycle Violations
  # ============================================================================

  def test_resume_mixing_arguments_raises_argument_error
    session = build_session
    handle = ResumeHandle.new upload_url: "https://example.com", chunk_size: 4

    # Mixing resume_handle with upload_url
    assert_raises ArgumentError do
      session.resume resume_handle: handle, upload_url: "https://example.com"
    end

    # Mixing resume_handle with chunk_size
    assert_raises ArgumentError do
      session.resume resume_handle: handle, chunk_size: 4
    end

    # upload_url without chunk_size
    assert_raises ArgumentError do
      session.resume upload_url: "https://example.com"
    end

    # chunk_size without upload_url
    assert_raises ArgumentError do
      session.resume chunk_size: 4
    end

    # Invalid positional argument type
    assert_raises ArgumentError do
      session.resume handle
    end
  end

  def test_resume_rebinding_different_upload_url_raises_session_state_error
    responses = [
      FakeResponse.new(
        status:  200,
        headers: {
          "x-goog-upload-status"        => "active",
          "x-goog-upload-size-received" => "0"
        },
        body:    ""
      ),
      FakeResponse.new(
        status:  200,
        headers: { "x-goog-upload-status" => "final" },
        body:    '{"done":true}'
      )
    ]
    session = build_session(
      stub:        ScriptedClientStub.new(responses),
      stream:      StringIO.new("01"),
      upload_size: 2,
      chunk_size:  4
    )
    session.resume upload_url: "https://upload.example.com/session_a", chunk_size: 4

    assert session.bound?
    assert_equal "https://upload.example.com/session_a", session.upload_url

    # Attempting to resume with a different upload_url
    err = assert_raises SessionStateError do
      session.resume upload_url: "https://upload.example.com/session_b", chunk_size: 4
    end
    assert_includes err.message, "Session is already bound to a different upload"

    handle_b = ResumeHandle.new upload_url: "https://upload.example.com/session_b", chunk_size: 4
    err2 = assert_raises SessionStateError do
      session.resume resume_handle: handle_b
    end
    assert_includes err2.message, "Session is already bound to a different upload"
  end

  def test_resume_on_bound_dead_session_raises_session_state_error
    responses = [
      FakeResponse.new(
        status:  200,
        headers: {
          "x-goog-upload-status"            => "active",
          "x-goog-upload-url"               => "https://upload.example.com/session_dead",
          "x-goog-upload-chunk-granularity" => "4"
        },
        body:    ""
      ),
      FakeResponse.new(
        status:  200,
        headers: { "x-goog-upload-status" => "final" },
        body:    '{"completed":true}'
      )
    ]
    session = build_session(
      stub:        ScriptedClientStub.new(responses),
      stream:      StringIO.new("01"),
      upload_size: 2,
      chunk_size:  4
    )
    session.start

    assert session.bound?
    refute session.resumable?

    err = assert_raises SessionStateError do
      session.resume
    end
    assert_includes err.message, "Session is dead and cannot be resumed"
  end

  # ============================================================================
  # 6. Concurrency & Running Guard
  # ============================================================================

  def test_running_guard_prevents_concurrent_runs
    started_q = Queue.new
    unblock_q = Queue.new

    blocking_proc = proc do
      started_q.push :started
      unblock_q.pop # wait until test signals to proceed
      FakeResponse.new(
        status:  200,
        headers: {
          "x-goog-upload-status"            => "active",
          "x-goog-upload-url"               => "https://upload.example.com/session_block",
          "x-goog-upload-chunk-granularity" => "4"
        },
        body:    ""
      )
    end

    stub = ScriptedClientStub.new [
      blocking_proc,
      FakeResponse.new(
        status:  200,
        headers: { "x-goog-upload-status" => "final" },
        body:    '{"done":true}'
      )
    ]
    session = build_session(
      stub:        stub,
      stream:      StringIO.new("01"),
      upload_size: 2,
      chunk_size:  4
    )

    worker = Thread.new do
      session.start
    end

    started_q.pop # wait for worker thread to enter driver.run
    assert session.running?

    # Concurrent call from another thread raises SessionStateError
    err = assert_raises SessionStateError do
      session.resume
    end
    assert_includes err.message, "A run is already in progress for this session"

    err_start = assert_raises SessionStateError do
      session.start
    end
    assert_includes err_start.message, "A run is already in progress for this session"

    # Unblock worker thread
    unblock_q.push :continue
    result = worker.value

    assert_equal '{"done":true}', result
    refute session.running?
  end

  # ============================================================================
  # 7. Driver#upload_url Direct Verification
  # ============================================================================

  def test_driver_upload_url_across_statuses
    dummy_client = ScriptedClientStub.new
    config = CompleteUploadConfig.new(
      initial_url: "https://example.com/upload",
      stream:      StringIO.new("data"),
      upload_size: 4,
      chunk_size:  4
    )
    driver = Driver.new client_stub: dummy_client, config: config

    assert_nil driver.upload_url

    # Active
    driver.core.instance_variable_set(
      :@state,
      driver.core.state.with(status: :transmission_sending, upload_url: "https://upload.example.com/sess1")
    )
    assert_equal "https://upload.example.com/sess1", driver.upload_url
    assert_equal "https://upload.example.com/sess1", driver.resume_handle.upload_url

    # Rejected (resume_handle is nil, but upload_url remains readable)
    driver.core.instance_variable_set(
      :@state,
      driver.core.state.with(status: :rejected, upload_url: "https://upload.example.com/sess1")
    )
    assert_equal "https://upload.example.com/sess1", driver.upload_url
    assert_nil driver.resume_handle

    # Cancelled (resume_handle is nil, but upload_url remains readable)
    driver.core.instance_variable_set(
      :@state,
      driver.core.state.with(status: :cancelled, upload_url: "https://upload.example.com/sess1")
    )
    assert_equal "https://upload.example.com/sess1", driver.upload_url
    assert_nil driver.resume_handle

    # Success (resume_handle is nil, but upload_url remains readable)
    driver.core.instance_variable_set(
      :@state,
      driver.core.state.with(status: :success, upload_url: "https://upload.example.com/sess1")
    )
    assert_equal "https://upload.example.com/sess1", driver.upload_url
    assert_nil driver.resume_handle
  end
end
