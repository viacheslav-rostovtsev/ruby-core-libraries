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

require "json"

module Gapic
  module Common
    # Gapic Common exception class
    class Error < StandardError
    end

    ##
    # Raised when Scotty backend explicitly rejects the upload session
    # (returns non-2xx with X-Goog-Upload-Status: final).
    #
    class UploadRejectedError < Error
      # @return [String, nil] Response body from backend
      attr_reader :response_body

      # @param response_body [String, nil]
      def initialize response_body = nil
        @response_body = response_body
        super "Upload was rejected by server: #{response_body}"
      end
    end

    ##
    # Raised when the upload session is cancelled.
    #
    class UploadCancelledError < Error
      def initialize message = "Upload session was cancelled"
        super message
      end
    end

    ##
    # Raised when an unrecoverable HTTP response is received.
    #
    class BadResponseError < Error
      # @return [Integer, nil] HTTP status code
      attr_reader :status_code

      # @param status_code [Integer, nil]
      # @param message [String, nil]
      def initialize status_code = nil, message = nil
        @status_code = status_code
        super(message || "Received unexpected response with status code: #{status_code}")
      end
    end

    ##
    # Raised when an upload exceeds its global monotonic deadline.
    #
    class DeadlineExceededError < Error
      def initialize message = "Upload deadline exceeded"
        super message
      end
    end
  end
end
