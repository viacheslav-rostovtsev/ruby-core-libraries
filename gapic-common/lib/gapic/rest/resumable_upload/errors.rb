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

require "gapic/common/error"
require "gapic/rest/error"

module Gapic
  module Rest
    module ResumableUpload
      ##
      # @private
      # HTTP status code to reason phrase mapping.
      # @return [Hash<Integer, String>]
      HTTP_STATUS_PHRASES = {
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        410 => "Gone",
        411 => "Length Required",
        412 => "Precondition Failed",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        416 => "Range Not Satisfiable",
        429 => "Too Many Requests",
        499 => "Client Closed Request",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout"
      }.freeze

      ##
      # @private
      # Internal formatting helper for terminal error message and attribute extraction.
      #
      module ErrorBuilder
        class << self
          ##
          # @private
          # Formats status representation.
          #
          # @param status [Object, nil] Status value
          # @return [String, nil]
          def format_status status
            return nil if status.nil? || status.to_s.empty?

            status.to_s
          end

          ##
          # @private
          # Strips REST error prefix from message string.
          #
          # @param raw_message [String, nil] Raw error message
          # @return [String, nil]
          def clean_message raw_message
            return nil if raw_message.nil? || raw_message.empty?

            prefix = Gapic::Rest::Error::REST_ERROR_PREFIX
            msg = raw_message.to_s
            msg = msg.sub(/\A#{Regexp.escape prefix}:\s*/, "") if msg.start_with? prefix
            msg = msg.sub(/\A:\s*/, "").strip
            msg.empty? ? nil : msg
          end

          ##
          # @private
          # Builds error attributes tuple from an HTTP event or wrapped error.
          #
          # @param event [Object] HTTP response event or failure event
          # @param prefix [String] Error message prefix
          # @return [Array] Tuple of [message, status_code, status, details, headers]
          def build_attributes event, prefix: "Resumable upload failed"
            if event.respond_to?(:error) && event.error
              build_from_wrapped_error event, prefix: prefix
            else
              build_from_http_event event, prefix: prefix
            end
          end

          private

          ##
          # @private
          # Builds error attributes when a wrapped REST error is available.
          #
          # @param event [Object] HTTP response event containing wrapped error
          # @param prefix [String] Error message prefix
          # @return [Array] Tuple of [message, status_code, status, details, headers]
          def build_from_wrapped_error event, prefix:
            err = event.error
            status_code = err.status_code || (event.respond_to?(:status) ? event.status : nil)
            status = err.status
            status_name = format_status(status) || HTTP_STATUS_PHRASES[status_code]
            status_part = status_name ? " #{status_name}" : ""
            inner_msg = clean_message err.message
            msg = if inner_msg
                    "#{prefix} with HTTP #{status_code}#{status_part}: #{inner_msg}"
                  else
                    "#{prefix} with HTTP #{status_code}#{status_part}"
                  end
            headers = err.headers || (event.respond_to?(:headers) ? event.headers : nil)
            [msg, status_code, status, err.details, headers]
          end

          ##
          # @private
          # Builds error attributes directly from raw HTTP response event.
          #
          # @param event [Object] HTTP response event
          # @param prefix [String] Error message prefix
          # @return [Array] Tuple of [message, status_code, status, details, headers]
          def build_from_http_event event, prefix:
            status_code = event.status
            headers = event.respond_to?(:headers) && event.headers ? event.headers : {}
            upload_status = headers["x-goog-upload-status"] || headers["X-Goog-Upload-Status"]
            status_desc = upload_status ? "'#{upload_status}'" : "missing"
            status_name = HTTP_STATUS_PHRASES[status_code]
            status_part = status_name ? " #{status_name}" : ""
            msg = "#{prefix} with HTTP #{status_code}#{status_part} " \
                  "(X-Goog-Upload-Status: #{status_desc})"
            [msg, status_code, nil, nil, headers]
          end
        end
      end

      ##
      # Raised when an invalid or unmatched event is dispatched for the current protocol state.
      #
      # @!attribute [r] response
      #   @return [Gapic::Rest::ResumableUpload::Event::HttpResponse, Object, nil] Associated HTTP response
      # @!attribute [r] state
      #   @return [Symbol, nil] Current protocol state
      # @!attribute [r] event
      #   @return [Object, nil] Received event
      #
      class InvalidTransitionError < Gapic::Common::Error
        # @return [Gapic::Rest::ResumableUpload::Event::HttpResponse, Object, nil]
        attr_reader :response

        # @return [Symbol, nil] Current protocol state
        attr_reader :state

        # @return [Object, nil] Received event
        attr_reader :event

        ##
        # Initializes a new InvalidTransitionError.
        #
        # @param message [String] Descriptive error message
        # @param state [Symbol, nil] Current protocol state
        # @param event [Object, nil] Received event
        # @param response [Gapic::Rest::ResumableUpload::Event::HttpResponse, Object, nil] Associated HTTP response
        def initialize message, state: nil, event: nil, response: nil
          @state = state
          @event = event
          @response = response || (event if defined?(Event::HttpResponse) && event.is_a?(Event::HttpResponse))
          super message
        end
      end

      ##
      # Raised when stream rewinding is required but the stream does not support seeking.
      #
      class UnseekableStreamError < Gapic::Common::Error
      end

      ##
      # Raised when an unrecoverable HTTP response is received.
      #
      # @!attribute [r] response_body
      #   @return [String, nil] Response body from backend
      #
      class BadResponseError < Gapic::Rest::Error
        # @return [String, nil] Response body from backend
        attr_reader :response_body

        ##
        # Initializes a new BadResponseError.
        #
        # @param message [String, nil] Error message
        # @param status_code [Integer, nil] HTTP status code
        # @param status [String, nil] Status description
        # @param details [Object, nil] Error details
        # @param headers [Object, nil] Response headers
        # @param response_body [String, nil] Response body
        def initialize message = nil, status_code = nil, status: nil, details: nil, headers: nil, response_body: nil
          @response_body = response_body
          super message, status_code, status: status, details: details, headers: headers
        end

        ##
        # Creates a BadResponseError from an HTTP response event.
        #
        # @param event [Object] HTTP response event
        # @param response_body [String, nil] Optional response body override
        # @return [BadResponseError]
        def self.from event, response_body: nil
          body = response_body || (event.respond_to?(:body) ? event.body : nil)
          message, status_code, status, details, headers = ErrorBuilder.build_attributes event
          new message, status_code, status: status, details: details, headers: headers, response_body: body
        end
      end

      ##
      # Raised when Scotty backend explicitly rejects the upload session
      # (returns non-2xx with X-Goog-Upload-Status: final).
      #
      # @!attribute [r] response_body
      #   @return [String, nil] Response body from backend
      #
      class UploadRejectedError < Gapic::Rest::Error
        # @return [String, nil] Response body from backend
        attr_reader :response_body

        ##
        # Initializes a new UploadRejectedError.
        #
        # @param message [String, nil] Error message
        # @param status_code [Integer, nil] HTTP status code
        # @param status [String, nil] Status description
        # @param details [Object, nil] Error details
        # @param headers [Object, nil] Response headers
        # @param response_body [String, nil] Response body
        def initialize message = nil, status_code = nil, status: nil, details: nil, headers: nil, response_body: nil
          @response_body = response_body
          super message, status_code, status: status, details: details, headers: headers
        end

        ##
        # Creates an UploadRejectedError from an HTTP response event.
        #
        # @param event [Object] HTTP response event
        # @param response_body [String, nil] Optional response body override
        # @return [UploadRejectedError]
        def self.from event, response_body: nil
          body = response_body || (event.respond_to?(:body) ? event.body : nil)
          message, status_code, status, details, headers =
            ErrorBuilder.build_attributes event, prefix: "Upload rejected by server"
          new message, status_code, status: status, details: details, headers: headers, response_body: body
        end
      end

      ##
      # Raised when the upload session is cancelled.
      #
      class UploadCancelledError < Gapic::Common::Error
        ##
        # Initializes a new UploadCancelledError.
        #
        # @param message [String] Cancellation message
        def initialize message = "Upload session was cancelled"
          super message
        end

        ##
        # Creates an UploadCancelledError from a source event or message string.
        #
        # @param source [Object, String, nil] Source event or message
        # @return [UploadCancelledError]
        def self.from source = nil
          if source.is_a?(String) && !source.empty?
            new source
          else
            new
          end
        end
      end

      ##
      # Raised when an upload exceeds its global monotonic deadline.
      #
      # @!attribute [r] root_cause
      #   @return [Object, nil] Root cause exception if deadline exceeded during a retry loop
      #
      class DeadlineExceededError < Gapic::Common::Error
        # @return [Object, nil] Root cause exception if deadline exceeded during a retry loop
        attr_reader :root_cause

        ##
        # Initializes a new DeadlineExceededError.
        #
        # @param message [String] Deadline exceeded message
        # @param root_cause [Object, nil] Root cause exception
        def initialize message = "Upload deadline exceeded", root_cause: nil
          super message
          @root_cause = root_cause
        end
      end
    end
  end
end
