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
          def format_status status
            return nil if status.nil? || status.to_s.empty?

            status.to_s.split("_").map(&:capitalize).join(" ")
          end

          def clean_message raw_message
            return nil if raw_message.nil? || raw_message.empty?

            prefix = Gapic::Rest::Error::REST_ERROR_PREFIX
            msg = raw_message.to_s
            msg = msg.sub(/\A#{Regexp.escape prefix}:\s*/, "") if msg.start_with? prefix
            msg = msg.sub(/\A:\s*/, "").strip
            msg.empty? ? nil : msg
          end

          def build_attributes source, prefix: "Resumable upload failed"
            if source.respond_to?(:error) && source.error
              build_from_wrapped_error source, prefix: prefix
            elsif source.is_a? Gapic::Rest::Error
              build_from_rest_error source, prefix: prefix
            elsif source.respond_to? :status
              build_from_http_event source, prefix: prefix
            elsif source.is_a? Integer
              status_name = HTTP_STATUS_PHRASES[source]
              status_part = status_name ? " #{status_name}" : ""
              ["#{prefix} with HTTP #{source}#{status_part}".strip, source, nil, nil, nil]
            else
              [source.to_s, nil, nil, nil, nil]
            end
          end

          private

          def build_from_wrapped_error source, prefix:
            err = source.error
            status_code = err.status_code || (source.respond_to?(:status) ? source.status : nil)
            status = err.status
            status_name = format_status(status) || HTTP_STATUS_PHRASES[status_code]
            status_part = status_name ? " #{status_name}" : ""
            inner_msg = clean_message err.message
            msg = if inner_msg
                    "#{prefix} with HTTP #{status_code}#{status_part}: #{inner_msg}"
                  else
                    "#{prefix} with HTTP #{status_code}#{status_part}"
                  end
            headers = err.headers || (source.respond_to?(:headers) ? source.headers : nil)
            [msg, status_code, status, err.details, headers]
          end

          def build_from_rest_error source, prefix:
            status_code = source.status_code
            status = source.status
            status_name = format_status(status) || HTTP_STATUS_PHRASES[status_code]
            status_part = status_name ? " #{status_name}" : ""
            inner_msg = clean_message source.message
            msg = if inner_msg
                    "#{prefix} with HTTP #{status_code}#{status_part}: #{inner_msg}"
                  else
                    "#{prefix} with HTTP #{status_code}#{status_part}"
                  end
            [msg, status_code, status, source.details, source.headers]
          end

          def build_from_http_event source, prefix:
            status_code = source.status
            headers = source.respond_to?(:headers) && source.headers ? source.headers : {}
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
      class InvalidTransitionError < Gapic::Common::Error
        # @return [Gapic::Rest::ResumableUpload::Event::HttpResponse, Object, nil]
        attr_reader :response

        # @return [Symbol, nil] Current protocol state
        attr_reader :state

        # @return [Object, nil] Received event
        attr_reader :event

        # @param message [String]
        # @param state [Symbol, nil]
        # @param event [Object, nil]
        # @param response [Gapic::Rest::ResumableUpload::Event::HttpResponse, Object, nil]
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
      class BadResponseError < Gapic::Rest::Error
        # @return [String, nil] Response body from backend
        attr_reader :response_body

        # @param message [String, nil]
        # @param status_code [Integer, nil]
        # @param status [String, nil]
        # @param details [Object, nil]
        # @param headers [Object, nil]
        # @param response_body [String, nil]
        def initialize message = nil, status_code = nil, status: nil, details: nil, headers: nil, response_body: nil
          @response_body = response_body
          if message.is_a?(Integer) && status_code.is_a?(String)
            message, status_code = status_code, message
          elsif message.is_a?(Integer) && status_code.nil?
            status_code = message
            message = nil
          end
          message ||= "Received unexpected response with status code: #{status_code}" if status_code
          super message, status_code, status: status, details: details, headers: headers
        end

        def self.from source, response_body: nil
          body = response_body || (source.respond_to?(:body) ? source.body : nil)
          message, status_code, status, details, headers = ErrorBuilder.build_attributes source
          new message, status_code, status: status, details: details, headers: headers, response_body: body
        end
      end

      ##
      # Raised when Scotty backend explicitly rejects the upload session
      # (returns non-2xx with X-Goog-Upload-Status: final).
      #
      class UploadRejectedError < Gapic::Rest::Error
        # @return [String, nil] Response body from backend
        attr_reader :response_body

        # @param message [String, nil]
        # @param status_code [Integer, nil]
        # @param status [String, nil]
        # @param details [Object, nil]
        # @param headers [Object, nil]
        # @param response_body [String, nil]
        def initialize message = nil, status_code = nil, status: nil, details: nil, headers: nil, response_body: nil
          @response_body = response_body
          if status_code.nil? && response_body.nil? && message &&
             !message.start_with?("Upload rejected by server")
            @response_body = message
            message = "Upload rejected by server: #{message}"
          end
          super message, status_code, status: status, details: details, headers: headers
        end

        def self.from source, response_body: nil
          body = response_body || (source.respond_to?(:body) ? source.body : nil)
          message, status_code, status, details, headers =
            ErrorBuilder.build_attributes source, prefix: "Upload rejected by server"
          new message, status_code, status: status, details: details, headers: headers, response_body: body
        end
      end

      ##
      # Raised when the upload session is cancelled.
      #
      class UploadCancelledError < Gapic::Common::Error
        def initialize message = "Upload session was cancelled"
          super message
        end

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
      class DeadlineExceededError < Gapic::Common::Error
        # @return [Object, nil] Root cause exception if deadline exceeded during a retry loop
        attr_reader :root_cause

        def initialize message = "Upload deadline exceeded", root_cause: nil
          super message
          @root_cause = root_cause
        end
      end
    end
  end
end
