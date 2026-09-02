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

module Gapic
  module Rest
    module ResumableUpload
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
    end
  end
end
