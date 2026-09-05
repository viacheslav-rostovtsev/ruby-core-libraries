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

module Gapic
  module Rest
    module ResumableUpload
      ##
      # Immutable configuration for initiating and executing a resumable upload session.
      #
      # @!attribute [r] initial_url
      #   @return [String] Initial endpoint URI for session initiation
      # @!attribute [r] initial_body
      #   @return [String, nil] Request payload for session initiation
      # @!attribute [r] initial_headers
      #   @return [Hash<String, String>] Additional headers for initiation
      # @!attribute [r] stream
      #   @return [IO] Binary input stream to upload
      # @!attribute [r] upload_size
      #   @return [Integer, nil] Total upload bytes if known upfront
      # @!attribute [r] chunk_size
      #   @return [Integer, nil] Explicit chunk size in bytes
      # @!attribute [r] content_type
      #   @return [String, nil] MIME type of uploaded media
      # @!attribute [r] timeout
      #   @return [Numeric, nil] Total upload timeout in seconds (zero/negative values treated as nil)
      # @!attribute [r] start_retry_policy
      #   @return [Gapic::Common::RetryPolicy, nil] Default policy for start command
      # @!attribute [r] control_plane_retry_policy
      #   @return [Gapic::Common::RetryPolicy, nil] Policy for query/cancel commands
      # @!attribute [r] data_plane_retry_policy
      #   @return [Gapic::Common::RetryPolicy, nil] Policy for upload/finalize
      # @!attribute [r] on_progress
      #   @return [Proc, nil] Callback invoked as `->(bytes_uploaded, total_bytes)`
      #
      CompleteUploadConfig = Data.define(
        :initial_url,
        :initial_body,
        :initial_headers,
        :stream,
        :upload_size,
        :chunk_size,
        :content_type,
        :timeout,
        :start_retry_policy,
        :control_plane_retry_policy,
        :data_plane_retry_policy,
        :on_progress
      ) do
        def initialize initial_url:,
                       stream:,
                       initial_body: nil,
                       initial_headers: {},
                       upload_size: nil,
                       chunk_size: nil,
                       content_type: nil,
                       timeout: nil,
                       start_retry_policy: nil,
                       control_plane_retry_policy: nil,
                       data_plane_retry_policy: nil,
                       on_progress: nil
          super(
            initial_url:                initial_url,
            initial_body:               initial_body,
            initial_headers:            initial_headers || {},
            stream:                     stream,
            upload_size:                upload_size,
            chunk_size:                 chunk_size,
            content_type:               content_type,
            timeout:                    timeout,
            start_retry_policy:         start_retry_policy,
            control_plane_retry_policy: control_plane_retry_policy,
            data_plane_retry_policy:    data_plane_retry_policy,
            on_progress:                on_progress
          )
        end
      end

      ##
      # Immutable state snapshot representing the current protocol progression.
      #
      State = Data.define(
        :status,
        :upload_url,
        :offset,
        :chunk_size,
        :chunk_granularity,
        :in_flight_length,
        :last_error
      ) do
        def initialize status: :initializing,
                       upload_url: nil,
                       offset: 0,
                       chunk_size: 8_388_608,
                       chunk_granularity: nil,
                       in_flight_length: 0,
                       last_error: nil
          super(
            status:            status,
            upload_url:        upload_url,
            offset:            offset,
            chunk_size:        chunk_size,
            chunk_granularity: chunk_granularity,
            in_flight_length:  in_flight_length,
            last_error:        last_error
          )
        end
      end
    end
  end
end
