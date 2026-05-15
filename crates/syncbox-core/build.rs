fn main() {
    // Optionally bake a default pair-server host into the binary.
    //
    // Put the URL in `pair-server.txt` at the repository root (gitignored).
    // If present, its contents become the compiled-in default; otherwise the
    // app falls back to a harmless placeholder. This keeps a personal domain
    // out of the source tree while letting local builds "just work".
    //
    // build.rs runs with the crate directory as CWD, so the repo root is two
    // levels up: crates/syncbox-core/ -> ../../
    let path = "../../pair-server.txt";
    println!("cargo:rerun-if-changed={path}");
    if let Ok(url) = std::fs::read_to_string(path) {
        let url = url.trim();
        if !url.is_empty() {
            println!("cargo:rustc-env=SYNCBOX_DEFAULT_PAIR_SERVER={url}");
        }
    }
}
