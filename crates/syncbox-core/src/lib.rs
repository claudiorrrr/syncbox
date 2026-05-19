//! syncbox-core — the sync engine.
//!
//! Everything here is UI- and platform-agnostic. The macOS GUI
//! (`src-tauri`) and the headless CLI (`syncbox-cli`) both build their
//! front-ends on top of these modules.

pub mod config;
pub mod ignore_patterns;
pub mod pair;
pub mod peer;
pub mod sync;
