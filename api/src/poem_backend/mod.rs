// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use poem_openapi::Tags;

mod basic;
mod index;
mod log;
mod runtime;

#[derive(Tags)]
pub enum ApiTags {
    /// General information.
    General,
}

pub use basic::BasicApi;
pub use index::IndexApi;
pub use log::middleware_log;
pub use runtime::attach_poem_to_runtime;
