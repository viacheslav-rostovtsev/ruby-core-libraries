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
      # Event vocabulary emitted by the Driver and dispatched to Core/Rules.
      #
      module Event
        ##
        # Signals the start of the upload session.
        #
        StartUpload = Data.define

        ##
        # Signals that binary data was read from the stream into the Driver's buffer.
        #
        ChunkRead = Data.define :bytes_buffered, :eof do
          def initialize bytes_buffered: 0, eof: false
            super bytes_buffered: bytes_buffered, eof: eof
          end
        end

        ##
        # Signals a completed HTTP exchange over the wire (status, headers, body).
        #
        HttpResponse = Data.define :status, :headers, :body do
          def initialize status:, headers: {}, body: nil
            super status: status, headers: headers || {}, body: body
          end
        end

        ##
        # Signals an HTTP request failure (e.g. transport connection failure or retries exhausted).
        #
        RequestFailed = Data.define :kind, :message, :source_error do
          def initialize kind:, message: nil, source_error: nil
            super kind: kind, message: message, source_error: source_error
          end
        end

        ##
        # Signals a caller-requested session cancellation.
        #
        Cancel = Data.define

        ##
        # Signals that the global monotonic clock exceeded the configured deadline.
        #
        GlobalDeadlineExceeded = Data.define
      end
    end
  end
end
