//! Gitignore-style pattern matching for the sync folder.
//!
//! The user can drop a `.syncboxignore` file at the root of their synced
//! folder. We also bake in a small set of obviously-noisy defaults so a
//! freshly-paired folder doesn't try to sync `.git`, `node_modules`, or
//! `.DS_Store` out of the gate.

use anyhow::Result;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use std::path::{Path, PathBuf};

/// Patterns we always ignore even if the user didn't write a
/// `.syncboxignore`. These are things almost no one wants to sync.
pub const BUILTIN: &[&str] = &[
    ".git/",
    ".hg/",
    ".svn/",
    ".DS_Store",
    "Thumbs.db",
    "desktop.ini",
    "node_modules/",
    "target/",
    ".venv/",
    "__pycache__/",
    "*.swp",
    "*.tmp",
    "*.partial",
    "~$*",
];

pub struct IgnoreSet {
    matcher: Gitignore,
    root: PathBuf,
}

impl IgnoreSet {
    pub fn load(root: &Path) -> Result<Self> {
        let mut b = GitignoreBuilder::new(root);
        for p in BUILTIN {
            b.add_line(None, p).ok();
        }
        let user = root.join(".syncboxignore");
        if user.exists() {
            if let Some(err) = b.add(&user) {
                tracing::warn!(error = ?err, "could not load .syncboxignore");
            }
        }
        let matcher = b
            .build()
            .map_err(|e| anyhow::anyhow!("gitignore build: {e}"))?;
        Ok(Self {
            matcher,
            root: root.to_path_buf(),
        })
    }

    /// `rel` is a path relative to the sync root. Returns true if the entry
    /// should be skipped by both the watcher and the initial scan.
    pub fn is_ignored(&self, rel: &Path, is_dir: bool) -> bool {
        let abs = self.root.join(rel);
        self.matcher.matched(&abs, is_dir).is_ignore()
    }
}
