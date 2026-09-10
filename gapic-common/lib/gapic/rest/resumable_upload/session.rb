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

require "gapic/rest/resumable_upload/data_types"
require "gapic/rest/resumable_upload/driver"
require "gapic/rest/resumable_upload/errors"

module Gapic
  module Rest
    module ResumableUpload
      ##
      # Coordinates a resumable upload across its entire lifecycle.
      #
      # A Session is 1 per logical transfer. It owns the input stream and configuration options,
      # performing runs against exactly one upload.
      #
      # ### Observable States
      # 1. **Unbound** (`!bound?`): Fresh session. `start` and explicit `resume` are allowed.
      #    Bare `resume` raises {SessionStateError}.
      # 2. **Bound and Alive** (`bound? && resumable?`): Active or paused with valid {resume_handle}.
      #    Bare `resume` and matching explicit `resume` are allowed; `start` raises {SessionStateError}.
      # 3. **Bound Dead** (`bound? && !resumable?`): Finalized upload (completed, rejected, or cancelled).
      #    Completed uploads are not resumable. The session is permanently unusable for further uploads;
      #    both `start` and `resume` raise {SessionStateError}. An end user wishing to upload must create
      #    a new session.
      #
      class Session
        # @return [Gapic::Rest::ClientStub] Underlying REST client stub
        attr_reader :client_stub

        # @return [IO] Binary input stream to upload
        attr_reader :stream

        # @return [String] Initial endpoint URI for session initiation
        attr_reader :initial_url

        # @return [String, nil] Request payload for session initiation
        attr_reader :initial_body

        # @return [Hash<String, String>] Additional headers for initiation
        attr_reader :initial_headers

        # @return [Integer, nil] Total upload bytes if known upfront
        attr_reader :upload_size

        # @return [Integer, nil] Explicit chunk size in bytes
        attr_reader :chunk_size

        # @return [String, nil] MIME type of uploaded media
        attr_reader :content_type

        # @return [Numeric, nil] Total upload timeout in seconds
        attr_reader :timeout

        # @return [Gapic::Common::RetryPolicy, Hash, nil] Retry policy for session initiation
        attr_reader :start_retry_policy

        # @return [Gapic::Common::RetryPolicy, Hash, nil] Retry policy for control commands
        attr_reader :control_plane_retry_policy

        # @return [Gapic::Common::RetryPolicy, Hash, nil] Retry policy for data commands
        attr_reader :data_plane_retry_policy

        # @return [Proc, nil] Callback invoked with Progress snapshots
        attr_reader :on_progress

        # @return [Logger, nil] Logger instance
        attr_reader :logger

        ##
        # Initializes a new Resumable Upload Session.
        #
        # @param client_stub [Gapic::Rest::ClientStub] Underlying REST client stub
        # @param stream [IO] Binary input stream to upload
        # @param initial_url [String] Initial endpoint URI for session initiation
        # @param initial_body [String, nil] Request payload for session initiation
        # @param initial_headers [Hash<String, String>] Additional headers for initiation
        # @param upload_size [Integer, nil] Total upload bytes if known upfront
        # @param chunk_size [Integer, nil] Explicit chunk size in bytes
        # @param content_type [String, nil] MIME type of uploaded media
        # @param timeout [Numeric, nil] Total upload timeout in seconds
        # @param start_retry_policy [Gapic::Common::RetryPolicy, Hash, nil] Initiation retry policy
        # @param control_plane_retry_policy [Gapic::Common::RetryPolicy, Hash, nil] Control retry policy
        # @param data_plane_retry_policy [Gapic::Common::RetryPolicy, Hash, nil] Data retry policy
        # @param on_progress [Proc, nil] Progress callback
        # @param logger [Logger, nil] Logger instance
        #
        def initialize client_stub:,
                       stream:,
                       initial_url:,
                       initial_body:,
                       initial_headers: {},
                       upload_size: nil,
                       chunk_size: nil,
                       content_type: nil,
                       timeout: nil,
                       start_retry_policy: nil,
                       control_plane_retry_policy: nil,
                       data_plane_retry_policy: nil,
                       on_progress: nil,
                       logger: nil
          @client_stub = client_stub
          @stream = stream
          @initial_url = initial_url
          @initial_body = initial_body
          @initial_headers = initial_headers || {}
          @upload_size = upload_size
          @chunk_size = chunk_size
          @content_type = content_type
          @timeout = timeout
          @start_retry_policy = start_retry_policy
          @control_plane_retry_policy = control_plane_retry_policy
          @data_plane_retry_policy = data_plane_retry_policy
          @on_progress = on_progress
          @logger = logger

          @mutex = Mutex.new
          @running = false
          @upload_url = nil
          @last_driver = nil
        end

        ##
        # Returns the raw upload session URL if established.
        #
        # @return [String, nil]
        def upload_url
          @mutex.synchronize { upload_url_internal }
        end

        ##
        # Returns whether the session is bound to a server-side upload.
        #
        # @return [Boolean]
        def bound?
          @mutex.synchronize { !upload_url_internal.nil? }
        end

        ##
        # Returns the current {ResumeHandle} if the session is alive and resumable.
        # Completed uploads are not resumable (returns nil). Rejected uploads and
        # cancelled uploads are also finalized and not resumable, returning nil.
        #
        # @return [ResumeHandle, nil]
        def resume_handle
          @mutex.synchronize { resume_handle_internal }
        end

        ##
        # Returns whether the session can be resumed.
        # Completed uploads are not resumable (returns false). Rejected uploads and
        # cancelled uploads are also finalized and not resumable (returns false).
        #
        # @return [Boolean]
        def resumable?
          @mutex.synchronize { !resume_handle_internal.nil? }
        end

        ##
        # Returns whether a run is currently executing.
        #
        # @return [Boolean]
        def running?
          @mutex.synchronize { @running }
        end

        ##
        # Starts a new upload session on the server.
        #
        # @return [String, Object] Final response body upon completion
        # @raise [SessionStateError] If already bound or if a run is currently in progress
        def start
          driver = nil
          @mutex.synchronize do
            raise SessionStateError, "A run is already in progress for this session" if @running
            raise SessionStateError, "Session is already bound to an upload" if bound_internal?

            @running = true
            config = build_start_config
            driver = Driver.new client_stub: @client_stub, config: config, logger: @logger
          end

          execute_run driver
        end

        ##
        # Resumes an upload session using one of three mutually exclusive forms:
        # 1. Bare `resume(stream_offset: nil)`: Continues the bound upload. Derives stream_offset as 0
        #    for seekable streams or the prior run stream_position for unseekable streams.
        # 2. `resume(upload_url:, chunk_size:, stream_offset: nil)`: Binds and resumes explicit URL and chunk size.
        # 3. `resume(resume_handle:, stream_offset: nil)`: Binds and resumes via {ResumeHandle}.
        #
        # Completed uploads are not resumable; attempting to resume a completed session raises {SessionStateError}.
        #
        # @param upload_url [String, nil] Explicit upload URL
        # @param chunk_size [Integer, nil] Explicit chunk size
        # @param resume_handle [ResumeHandle, nil] Explicit resume handle
        # @param stream_offset [Integer, nil] Current absolute byte offset of the stream
        # @return [String, Object] Final response body upon completion
        # @raise [ArgumentError] If argument shape is invalid or forms are mixed
        # @raise [SessionStateError] If lifecycle rules are violated
        def resume upload_url: nil,
                   chunk_size: nil,
                   resume_handle: nil,
                   stream_offset: nil
          target_url, target_chunk_size = resolve_resume_args(
            upload_url:    upload_url,
            chunk_size:    chunk_size,
            resume_handle: resume_handle
          )

          driver = nil
          @mutex.synchronize do
            target_url, target_chunk_size, resolved_offset = validate_and_bind_resume(
              target_url, target_chunk_size, stream_offset
            )
            @running = true
            config = build_resume_config target_url, target_chunk_size, resolved_offset
            driver = Driver.new client_stub: @client_stub, config: config, logger: @logger
          end

          execute_run driver
        end

        private

        def upload_url_internal
          @upload_url || @last_driver&.upload_url
        end

        def bound_internal?
          !upload_url_internal.nil?
        end

        def resume_handle_internal
          @last_driver&.resume_handle
        end

        def dead_internal?
          bound_internal? && resume_handle_internal.nil?
        end

        def build_start_config
          CompleteUploadConfig.new(
            initial_url:                @initial_url,
            initial_body:               @initial_body,
            initial_headers:            @initial_headers,
            stream:                     @stream,
            upload_size:                @upload_size,
            chunk_size:                 @chunk_size,
            content_type:               @content_type,
            timeout:                    @timeout,
            start_retry_policy:         @start_retry_policy,
            control_plane_retry_policy: @control_plane_retry_policy,
            data_plane_retry_policy:    @data_plane_retry_policy,
            on_progress:                @on_progress
          )
        end

        def build_resume_config target_url, target_chunk_size, stream_offset
          ResumeUploadConfig.new(
            upload_url:                 target_url,
            chunk_size:                 target_chunk_size,
            stream:                     @stream,
            stream_offset:              stream_offset || 0,
            upload_size:                @upload_size,
            content_type:               @content_type,
            timeout:                    @timeout,
            start_retry_policy:         @start_retry_policy,
            control_plane_retry_policy: @control_plane_retry_policy,
            data_plane_retry_policy:    @data_plane_retry_policy,
            on_progress:                @on_progress
          )
        end

        def resolve_resume_args upload_url:, chunk_size:, resume_handle:
          if resume_handle
            raise ArgumentError, "Cannot pass both resume_handle and upload_url/chunk_size" if upload_url || chunk_size
            [resume_handle.upload_url, resume_handle.chunk_size]
          elsif upload_url
            raise ArgumentError, "Must provide chunk_size with upload_url" if chunk_size.nil?
            [upload_url, chunk_size]
          elsif chunk_size
            raise ArgumentError, "Cannot pass chunk_size without upload_url"
          else
            [nil, nil]
          end
        end

        def validate_and_bind_resume target_url, target_chunk_size, stream_offset
          raise SessionStateError, "A run is already in progress for this session" if @running

          if target_url.nil?
            validate_bare_resume stream_offset
          else
            validate_explicit_resume target_url, target_chunk_size, stream_offset
          end
        end

        def validate_bare_resume stream_offset
          unless bound_internal?
            raise SessionStateError, "Cannot resume unbound session without resume_handle or upload_url"
          end
          raise SessionStateError, "Session is dead and cannot be resumed" if dead_internal?

          resolved_url = upload_url_internal
          resolved_chunk = resume_handle_internal&.chunk_size || @chunk_size
          raise SessionStateError, "No chunk_size available to resume session" if resolved_chunk.nil?

          resolved_offset = stream_offset || (
            @stream.respond_to?(:seek) ? 0 : (@last_driver&.stream_position || 0)
          )
          @stream.seek resolved_offset if @stream.respond_to? :seek

          [resolved_url, resolved_chunk, resolved_offset]
        end

        def validate_explicit_resume target_url, target_chunk_size, stream_offset
          if bound_internal? && target_url != upload_url_internal
            raise SessionStateError, "Session is already bound to a different upload: #{upload_url_internal}"
          end
          raise SessionStateError, "Session is dead and cannot be resumed" if dead_internal?

          @upload_url = target_url
          resolved_offset = stream_offset || 0
          @stream.seek resolved_offset if @stream.respond_to? :seek
          [target_url, target_chunk_size, resolved_offset]
        end

        def execute_run driver
          @mutex.synchronize { @last_driver = driver }
          result = driver.run
          @mutex.synchronize do
            @upload_url ||= driver.upload_url
            @running = false
          end
          result
        rescue StandardError
          @mutex.synchronize do
            @upload_url ||= driver.upload_url
            @running = false
          end
          raise
        end
      end
    end
  end
end
