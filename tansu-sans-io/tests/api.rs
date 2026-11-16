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

//! The crate's small public types: a frame's header accessors, and each
//! enum and flag set carried on the wire as an integer, round-tripped.

use std::fmt::Debug;
use std::process::ExitCode;
use std::process::Termination as _;
use std::time::Duration;
use std::time::SystemTime;

use tansu_sans_io::{Ack, ApiKey as _, BatchAttribute, Body, Compression, ConfigResource};
use tansu_sans_io::{ConfigSource, ConfigType, ControlBatch, CoordinatorType};
use tansu_sans_io::{EndTransactionMarker, EndpointType, ErrorCode, FetchRequest, FetchResponse};
use tansu_sans_io::{Frame, Header, IsolationLevel, ListOffset, MaximumAllocationSize as _};
use tansu_sans_io::{OpType, TimestampType, to_system_time, to_timestamp};

type Octets = bytes::Bytes;

/// Asserts that each of `wire` converts to `T` and back unchanged.
fn round_trip<W, T>(wire: impl IntoIterator<Item = W>)
where
    W: Copy + Debug + PartialEq + From<T>,
    T: TryFrom<W, Error: Debug>,
{
    for value in wire {
        let typed = T::try_from(value).unwrap_or_else(|error| panic!("{value:?}: {error:?}"));
        assert_eq!(value, W::from(typed));
    }
}

/// `1.7e12` milliseconds after the epoch, as a `SystemTime`.
fn november_2023() -> (i64, SystemTime) {
    let millis = 1_700_000_000_000_i64;
    (
        millis,
        SystemTime::UNIX_EPOCH + Duration::from_millis(millis.unsigned_abs()),
    )
}

/// `body` under `header`, its size left for the encoder.
fn framed(header: Header, body: Body) -> Frame {
    let size = i32::default();
    Frame { size, header, body }
}

#[test]
fn a_request_frame_has_header_fields_a_response_frame_does_not() -> tansu_sans_io::Result<()> {
    let (key, version, correlation, client) = (FetchRequest::KEY, 6, 32123, "client");
    let header = Header::Request {
        api_key: key,
        api_version: version,
        correlation_id: correlation,
        client_id: Some(String::from(client)),
    };
    let request = framed(header, FetchRequest::default().into());
    let key_and_version = (request.api_key()?, request.api_version()?);
    assert_eq!((key, version), key_and_version);
    assert_eq!(correlation, request.correlation_id()?);
    let named = (request.api_name(), request.client_id()?);
    assert_eq!(("FetchRequest", Some(client)), named);

    let header = Header::Response {
        correlation_id: correlation,
    };
    let answer = framed(header, FetchResponse::default().into());
    assert_eq!("FetchResponse", answer.api_name());
    assert_eq!(correlation, answer.correlation_id()?);
    let missing = (answer.api_key().is_err(), answer.api_version().is_err());
    assert_eq!(
        (true, true, true),
        (missing.0, missing.1, answer.client_id().is_err())
    );
    Ok(())
}

#[test]
fn error_codes_round_trip_and_report_as_exit_codes() {
    round_trip::<i16, ErrorCode>(-1..120);
    let reported = (
        ErrorCode::None.report(),
        ErrorCode::UnknownMemberId.report(),
    );
    assert_eq!((ExitCode::SUCCESS, ExitCode::FAILURE), reported);
}

#[test]
fn small_enums_round_trip() {
    round_trip::<i8, IsolationLevel>([0, 1]);
    round_trip::<i16, Ack>([-1, 0, 1]);
    round_trip::<i16, Compression>(0..=4);
    round_trip::<i8, CoordinatorType>([0, 1, 2]);
    round_trip::<i8, OpType>(0..=3);
}

#[test]
fn small_enums_refuse_the_next_value() {
    assert!(Compression::try_from(5_i16).is_err());
    assert!(CoordinatorType::try_from(3_i8).is_err());
    assert!(OpType::try_from(4_i8).is_err());
}

#[test]
fn timestamp_types_are_create_time_and_log_append_time() {
    round_trip::<i16, TimestampType>([0, 8]);
    assert_eq!(TimestampType::CreateTime, TimestampType::from(0));
    assert_eq!(TimestampType::LogAppendTime, TimestampType::from(8));
}

/// Values outside each enum read as `Unknown`, which is 0 on the wire.
#[test]
fn open_enums_round_trip_and_read_anything_else_as_unknown() {
    round_trip::<i8, EndpointType>(0..=2);
    round_trip::<i8, ConfigResource>([2, 4, 8, 16, 32]);
    round_trip::<i8, ConfigSource>(1..=8);
    round_trip::<i8, ConfigType>(1..=9);

    assert_eq!(0, i8::from(EndpointType::from(99)));
    assert_eq!(0, i8::from(ConfigResource::from(99)));
    assert_eq!(0, i8::from(ConfigSource::from(99)));
    assert_eq!(
        (0, 0),
        (
            i8::from(ConfigType::from(99)),
            i8::from(ConfigType::from(0))
        )
    );

    let as_i32 = (
        i32::from(ConfigResource::Topic),
        i32::from(ConfigResource::Broker),
    );
    assert_eq!((2, 4), as_i32);
}

#[test]
fn batch_attributes_round_trip_bit_by_bit() -> tansu_sans_io::Result<()> {
    let unset = i16::from(BatchAttribute::default());
    let back = BatchAttribute::try_from(unset)?;
    assert_eq!(BatchAttribute::default(), back);
    round_trip::<i16, BatchAttribute>(0..=4);

    let timestamped = BatchAttribute::try_from(8_i16)?;
    assert_eq!(TimestampType::LogAppendTime, timestamped.timestamp);
    assert!(BatchAttribute::try_from(16_i16)?.transaction);
    assert!(BatchAttribute::try_from(32_i16)?.control);
    assert!(BatchAttribute::try_from(64_i16)?.delete_horizon);

    let gzip_log_append_txn_control_horizon = 1 | 8 | 16 | 32 | 64;
    round_trip::<i16, BatchAttribute>([gzip_log_append_txn_control_horizon]);
    Ok(())
}

#[test]
fn control_batches_and_end_markers_round_trip_through_bytes() -> tansu_sans_io::Result<()> {
    let committed = ControlBatch::default().commit();
    assert_eq!((true, false), (committed.is_commit(), committed.is_abort()));
    let aborted = ControlBatch::default().abort();
    assert_eq!((false, true), (aborted.is_commit(), aborted.is_abort()));

    for control in [
        ControlBatch::default().version(1).commit(),
        ControlBatch::default().version(1).abort(),
    ] {
        let encoded = Octets::try_from(control.clone())?;
        assert_eq!(control, ControlBatch::try_from(encoded)?);
    }

    let (version, coordinator_epoch) = (1, 42);
    let ended = EndTransactionMarker {
        version,
        coordinator_epoch,
    };
    let encoded = Octets::try_from(ended.clone())?;
    assert_eq!(ended, EndTransactionMarker::try_from(encoded)?);
    Ok(())
}

#[test]
fn milliseconds_convert_to_system_time_and_back() -> tansu_sans_io::Result<()> {
    let epoch = to_system_time(0)?;
    assert_eq!((SystemTime::UNIX_EPOCH, 0), (epoch, to_timestamp(&epoch)?));

    let (millis, expected) = november_2023();
    let converted = to_system_time(millis)?;
    assert_eq!((expected, millis), (converted, to_timestamp(&converted)?));

    assert!(
        to_system_time(-1).is_err(),
        "a millisecond before the epoch"
    );
    Ok(())
}

/// -2 and -1 are the earliest and latest offsets; anything else below zero
/// is not a time.
#[test]
fn list_offsets_round_trip_through_their_wire_timestamps() -> tansu_sans_io::Result<()> {
    for sentinel in [-2_i64, -1] {
        assert_eq!(sentinel, i64::try_from(ListOffset::try_from(sentinel)?)?);
    }

    let (millis, at) = november_2023();
    let listed = ListOffset::try_from(millis)?;
    assert_eq!(ListOffset::Timestamp(at), listed);
    assert_eq!(millis, i64::try_from(listed)?);

    assert!(ListOffset::try_from(-3_i64).is_err());
    Ok(())
}

/// A string's prefix, a `u16` length, plus its bytes; a missing string, the
/// `u32` the absent case reserves.
#[test]
fn an_optional_string_reports_its_maximum_allocation() -> tansu_sans_io::Result<()> {
    let present = Some(String::from("hey"));
    assert_eq!(size_of::<u16>() + 3, present.maximum_allocation_size()?);
    let absent: Option<String> = None;
    assert_eq!(size_of::<u32>(), absent.maximum_allocation_size()?);
    Ok(())
}
