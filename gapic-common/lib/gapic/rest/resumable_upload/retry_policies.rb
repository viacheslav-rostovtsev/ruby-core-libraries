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

require "gapic/common/retry_policy"

module Gapic
  module Rest
    module ResumableUpload
      ##
      # Default retry policy generators for control plane and data plane requests.
      #
      module RetryPolicies
        ##
        # Default retry policy for control plane requests (start, query, cancel).
        # Missing X-Goog-Upload-Status header is retriable (predicate returns true).
        #
        # @return [Gapic::Common::RetryPolicy]
        def self.default_control_plane
          Gapic::Common::RetryPolicy.new(
            retry_codes:     ["UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED", "INTERNAL"],
            initial_delay:   1.0,
            max_delay:       15.0,
            multiplier:      1.3,
            retry_predicate: lambda do |error_or_response|
              headers = extract_headers error_or_response
              if headers
                status_hdr = headers["x-goog-upload-status"] || headers["X-Goog-Upload-Status"]
                return true if status_hdr.nil? || status_hdr.empty?
              end
              nil
            end
          )
        end

        ##
        # Default retry policy for data plane requests (upload, finalize).
        # Missing X-Goog-Upload-Status header is unretriable (predicate returns false).
        #
        # @return [Gapic::Common::RetryPolicy]
        def self.default_data_plane
          Gapic::Common::RetryPolicy.new(
            retry_codes:     ["UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED", "INTERNAL"],
            initial_delay:   1.0,
            max_delay:       15.0,
            multiplier:      1.3,
            retry_predicate: lambda do |error_or_response|
              headers = extract_headers error_or_response
              if headers
                status_hdr = headers["x-goog-upload-status"] || headers["X-Goog-Upload-Status"]
                return false if status_hdr.nil? || status_hdr.empty?
              end
              nil
            end
          )
        end

        ##
        # Extracts headers hash from Faraday response or error object.
        #
        # @param error_or_response [Object]
        # @return [Hash, nil]
        def self.extract_headers error_or_response
          if error_or_response.respond_to? :headers
            error_or_response.headers
          elsif error_or_response.respond_to? :response_headers
            error_or_response.response_headers
          elsif error_or_response.respond_to?(:response) && error_or_response.response.is_a?(Hash)
            error_or_response.response[:headers]
          end
        end
      end
    end
  end
end
