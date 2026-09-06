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

require "minitest/autorun"
require "minitest/focus"
require "minitest/mock"

require "gapic/common"
require "gapic/rest"
require "gapic/rest/resumable_upload"

##
# Helper methods for Showcase integration tests.
#
module ShowcaseIntegrationHelper
  UPLOAD_PATH = "resumable/upload/v1beta1/files:upload"

  attr_reader :progress_records

  def showcase_endpoint
    ENV["SHOWCASE_ENDPOINT"]
  end

  def setup
    skip "SHOWCASE_ENDPOINT is not set" if showcase_endpoint.to_s.empty?
    super
  end

  def payload size
    pattern = "0123456789".b
    (pattern * ((size / pattern.bytesize) + 1)).byteslice(0, size)
  end

  def showcase_client_stub
    Gapic::Rest::ClientStub.new(
      endpoint: showcase_endpoint,
      credentials: :dummy_credentials,
      raise_faraday_errors: false
    )
  end

  def build_config **overrides
    @progress_records = []
    defaults = {
      initial_url: UPLOAD_PATH,
      on_progress: ->(bytes_uploaded, total_bytes) { @progress_records << [bytes_uploaded, total_bytes] }
    }
    Gapic::Rest::ResumableUpload::CompleteUploadConfig.new(**defaults, **overrides)
  end
end

Minitest::Test.prepend ShowcaseIntegrationHelper