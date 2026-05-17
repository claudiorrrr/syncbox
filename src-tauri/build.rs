use std::process::Command;

fn main() {
    // Build number = count of commits on the current branch. Increments by one
    // with each commit, so every release has a distinct version. Baked in as
    // SYNCBOX_BUILD and read via option_env! in lib.rs. Falls back to "0" when
    // git is unavailable (e.g. building from a source tarball).
    let build = Command::new("git")
        .args(["rev-list", "--count", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "0".to_string());
    println!("cargo:rustc-env=SYNCBOX_BUILD={build}");

    // Re-run when a commit lands so the build number stays current. build.rs
    // runs with the crate dir as CWD, so .git is one level up.
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/refs");

    tauri_build::build()
}
