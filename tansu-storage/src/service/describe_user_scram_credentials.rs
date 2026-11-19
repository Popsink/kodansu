// Copyright ⓒ 2026 Popsink SAS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `DescribeUserScramCredentials`, answered with an empty result list.

storage_service! {
    /// Lists no credentials, whatever users the request names: the stored
    /// SCRAM credentials are never read back out over the protocol.
    DescribeUserScramCredentialsService:
        tansu_sans_io::DescribeUserScramCredentialsRequest
            => tansu_sans_io::DescribeUserScramCredentialsResponse,
    |_ctx, _req| Ok(Default::default())
}
