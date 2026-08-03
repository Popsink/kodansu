// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! The record layer, not just the batch header (#271).
//!
//! `fuzz_deflated_batch` already fuzzes `deflated::Batch::try_from`, which is
//! the header decode. It cannot reach the site this target covers: sizing a
//! `Vec` from the header's `record_count` happens during **inflation**, one call
//! later. A green header fuzzer said nothing about it.

#![no_main]
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use tansu_sans_io::record::{Record, deflated};

fuzz_target!(|data: &[u8]| {
    if let Ok(batch) = deflated::Batch::try_from(Bytes::copy_from_slice(data)) {
        // Both directions: the owned conversion takes the uncompressed branch
        // when the attributes say so, the borrowed one always inflates.
        let _ = Vec::<Record>::try_from(&batch);
        let _ = Vec::<Record>::try_from(batch);
    }
});
