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

require "integration_helper"
require "json"
require "stringio"

##
# Golden path integration tests for ResumableUpload Driver against Showcase.
#
class GoldenPathTest < ShowcaseIntegrationTest
  def test_multi_chunk_known_size
    size = 1_500_000
    chunk_size = 524_288
    stream = StringIO.new payload(size)

    config = build_config(
      stream: stream,
      upload_size: size,
      chunk_size: chunk_size
    )

    driver = Gapic::Rest::ResumableUpload::Driver.new(
      client_stub: showcase_client_stub,
      config: config
    )

    result = driver.run
    parsed = JSON.parse result

    assert_equal size, parsed["size"]
    assert_equal [
      Gapic::Rest::ResumableUpload::Progress.new(bytes_uploaded: 524_288, total_bytes: 1_500_000),
      Gapic::Rest::ResumableUpload::Progress.new(bytes_uploaded: 1_048_576, total_bytes: 1_500_000),
      Gapic::Rest::ResumableUpload::Progress.new(bytes_uploaded: 1_500_000, total_bytes: 1_500_000)
    ], progress_records
  end
end
