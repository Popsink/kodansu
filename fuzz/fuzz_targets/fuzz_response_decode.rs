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
//! A response frame. Unlike a request, a response does not say which API it
//! answers, so the first four input bytes pick the API key and version and the
//! rest is the frame.

#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    if let Some(([key_hi, key_lo, version_hi, version_lo], frame)) = input.split_first_chunk() {
        _ = tansu_sans_io::Frame::response_from_bytes(
            bytes::Bytes::from(frame.to_vec()),
            i16::from_be_bytes([*key_hi, *key_lo]),
            i16::from_be_bytes([*version_hi, *version_lo]),
        );
    }
});
