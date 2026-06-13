// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Adapted for bluedb: only `SPLIT_FIELDS_FILE_NAME` is carried over from
// `quickwit-common/src/shared_consts.rs`; the rest of that file concerns
// indexing/deletion grace periods and chitchat keys that the read path does not
// touch.

/// File name for the encoded list of fields in the split.
pub const SPLIT_FIELDS_FILE_NAME: &str = "split_fields";
