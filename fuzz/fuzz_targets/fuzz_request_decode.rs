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
//! A whole request frame, size prefix and header included: the first thing the
//! broker does with the bytes a client sends.

#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    _ = tansu_sans_io::Frame::request_from_bytes(bytes::Bytes::from(input.to_vec()));
});
