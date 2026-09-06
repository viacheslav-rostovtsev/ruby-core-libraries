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

require "uri"

module Gapic
  module Rest
    module ResumableUpload
      class Driver
        ##
        # @private
        # Pure functions for redacting and abridging sensitive data and large payloads in logs.
        #
        module Abridge
          module_function

          def bytes data
            return nil if data.nil?

            str = data.to_s
            if str.bytesize >= 64
              "#{str.byteslice(0, 32).unpack1('H*')}... <#{str.bytesize} bytes>"
            else
              str.unpack1 "H*"
            end
          end

          def error_body data
            return nil if data.nil?

            data.to_s.dup.force_encoding(Encoding::UTF_8).scrub[0, 512]
          end

          def url url
            return nil if url.nil?

            uri = URI.parse url.to_s
            if uri.query && !uri.query.empty?
              elided = uri.query.split("&").map do |pair|
                key, _val = pair.split "=", 2
                "#{key}=<...>"
              end.join "&"
              uri.query = nil
              return "#{uri}?#{elided}"
            end
            uri.to_s
          rescue URI::InvalidURIError
            url.to_s
          end

          def headers headers
            return {} unless headers.is_a? Hash

            headers.each_with_object({}) do |(k, v), acc|
              key_str = k.to_s
              acc[key_str] = if key_str.downcase == "x-goog-upload-url"
                               url v
                             elsif key_str.downcase.start_with? "x-goog-upload-"
                               v
                             else
                               "<...>"
                             end
            end
          end

          def instructions instructions
            instructions.map { |i| instruction i }
          end

          # rubocop:disable Metrics/MethodLength
          def instruction instruction
            case instruction
            when Instruction::SendStart
              { "type" => "SendStart", "url" => url(instruction.url) }
            when Instruction::SendChunk
              {
                "type"     => "SendChunk",
                "url"      => url(instruction.url),
                "offset"   => instruction.offset,
                "length"   => instruction.length,
                "finalize" => instruction.finalize
              }
            when Instruction::SendFinalize
              { "type" => "SendFinalize", "url" => url(instruction.url) }
            when Instruction::SendQuery
              { "type" => "SendQuery", "url" => url(instruction.url) }
            when Instruction::SendCancel
              { "type" => "SendCancel", "url" => url(instruction.url) }
            when Instruction::RealignBuffer
              { "type" => "RealignBuffer", "serverOffset" => instruction.server_offset }
            when Instruction::FillBuffer
              { "type" => "FillBuffer", "targetBytesize" => instruction.target_bytesize }
            when Instruction::NotifyProgress
              {
                "type"          => "NotifyProgress",
                "bytesUploaded" => instruction.progress.bytes_uploaded,
                "totalBytes"    => instruction.progress.total_bytes
              }
            when Instruction::TerminateSuccess
              { "type" => "TerminateSuccess" }
            when Instruction::TerminateFailure
              { "type" => "TerminateFailure", "error" => instruction.error.to_s }
            else
              { "type" => instruction.class.name }
            end
          end
          # rubocop:enable Metrics/MethodLength
        end
      end
    end
  end
end
