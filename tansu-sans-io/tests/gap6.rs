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

//! Malformed, truncated or out-of-range input is an `Err`, never a frame: in
//! requests, in responses, and in the enums decoded from wire integers.

use tansu_sans_io::{Compression, ErrorCode, FindCoordinatorRequest, Frame, Header};

/// A FindCoordinator v4 request from `console-consumer` for the group
/// `test-consumer-group`, to be cut short.
const FIND_COORDINATOR_V4: &[u8] =
    b"\0\0\0\x32\0\x0a\0\x04\0\0\0\0\0\x10console-consumer\0\0\x02\x14test-consumer-group\x01\0\0";

#[test]
fn malformed_requests_are_errors() {
    let cut = &FIND_COORDINATOR_V4[..FIND_COORDINATOR_V4.len() / 2];
    let inputs: [(&str, &[u8]); 14] = [
        ("empty input", b""),
        ("a single byte", b"\0"),
        ("half a size field", b"\0\0"),
        ("half an api_key", b"\0\0\0\x0a\0"),
        ("no api_version", b"\0\0\0\x0a\0\x0a"),
        ("half a correlation_id", b"\0\0\0\x14\0\x0a\0\x04\0\0"),
        ("a frame cut mid-body", cut),
        ("api_key 999", b"\0\0\0\x0c\x03\xe7\0\0\0\0\0\x01\xff\xff"),
        ("api_key -2", b"\0\0\0\x0c\xff\xfe\0\0\0\0\0\x01\xff\xff"),
        ("garbage", b"\xde\xad\xbe\xef\xca\xfe\xba\xbe"),
        ("twenty zero bytes", &[0; 20]),
        ("twenty 0xff bytes", &[0xff; 20]),
        (
            "a size of 1000 over 8 bytes",
            b"\0\0\x03\xe8\0\x0a\0\x04\0\0\0\0",
        ),
        (
            "a client_id of 32767 bytes",
            b"\0\0\0\x14\0\x0a\0\0\0\0\0\x01\x7f\xff\0\0\0\0\0\0",
        ),
    ];
    for (what, input) in inputs {
        assert!(Frame::request_from_bytes(input).is_err(), "{what} decoded");
    }
}

#[test]
fn malformed_responses_are_errors() {
    let inputs: [(&str, &[u8], i16, i16); 4] = [
        ("empty input", b"", 10, 4),
        ("a single byte", b"\0", 10, 4),
        ("three bytes of size", b"\0\0\0", 10, 4),
        ("api_key 999", b"\0\0\0\x04\0\0\0\x01", 999, 0),
    ];
    for (what, input, api_key, api_version) in inputs {
        let decoded = Frame::response_from_bytes(input, api_key, api_version);
        assert!(decoded.is_err(), "{what} decoded");
    }
}

#[test]
fn a_response_frame_has_no_request_header_fields() {
    let header = Header::Response { correlation_id: 0 };
    let body = FindCoordinatorRequest::default().into();
    let size = 0;
    let frame = Frame { size, header, body };
    assert!(frame.api_key().is_err(), "api_key");
    assert!(frame.api_version().is_err(), "api_version");
    assert!(frame.client_id().is_err(), "client_id");
}

#[test]
fn integers_outside_the_wire_enums_are_errors() {
    for code in [9999, -100, i16::MIN, i16::MAX] {
        assert!(ErrorCode::try_from(code).is_err(), "error code {code}");
    }
    for kind in 5..=7_i16 {
        assert!(Compression::try_from(kind).is_err(), "compression {kind}");
    }
}
