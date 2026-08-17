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
      # Instruction vocabulary emitted by Rules/Core to be executed by Driver.
      #
      module Instruction
        ##
        # Execute initiation request to establish upload session.
        #
        SendStart = Data.define :url, :headers, :body do
          def initialize url:, headers: {}, body: nil
            super url: url, headers: headers || {}, body: body
          end
        end

        ##
        # Transmit buffered chunk starting at offset for length bytes.
        #
        SendChunk = Data.define :url, :offset, :length, :finalize do
          def initialize url:, offset:, length:, finalize: false
            super url: url, offset: offset, length: length, finalize: finalize
          end
        end

        ##
        # Send standalone finalize command when all data bytes were already uploaded.
        #
        SendFinalize = Data.define :url do
          def initialize url:
            super url: url
          end
        end

        ##
        # Query backend for current acknowledged offset.
        #
        SendQuery = Data.define :url do
          def initialize url:
            super url: url
          end
        end

        ##
        # Cancel upload session on backend.
        #
        SendCancel = Data.define :url do
          def initialize url:
            super url: url
          end
        end

        ##
        # Realign Driver in-memory buffer and stream position to match server_offset.
        #
        RealignBuffer = Data.define :server_offset do
          def initialize server_offset:
            super server_offset: server_offset
          end
        end

        ##
        # Read from stream until in-memory buffer reaches target_bytesize or stream hits EOF.
        #
        FillBuffer = Data.define :target_bytesize do
          def initialize target_bytesize:
            super target_bytesize: target_bytesize
          end
        end

        ##
        # Invoke user progress callback with bytes_uploaded and total_bytes.
        #
        NotifyProgress = Data.define :bytes_uploaded, :total_bytes do
          def initialize bytes_uploaded:, total_bytes: nil
            super bytes_uploaded: bytes_uploaded, total_bytes: total_bytes
          end
        end

        ##
        # Upload finalized cleanly; return response.
        #
        TerminateSuccess = Data.define :response do
          def initialize response:
            super response: response
          end
        end

        ##
        # Terminate upload with error.
        #
        TerminateFailure = Data.define :error do
          def initialize error:
            super error: error
          end
        end
      end
    end
  end
end
