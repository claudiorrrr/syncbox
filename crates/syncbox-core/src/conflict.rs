use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Produce the filename used when we have to set aside a losing copy.
/// e.g. `notes.md` → `notes.conflict-laptop-1736812345.md`
pub fn conflict_path(original: &Path, host: &str) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let parent = original.parent().unwrap_or(Path::new(""));
    let stem = original
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("untitled");
    let ext = original.extension().and_then(|s| s.to_str());

    let safe_host: String = host
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '-' })
        .collect();

    let new_name = match ext {
        Some(e) if !e.is_empty() => format!("{stem}.conflict-{safe_host}-{ts}.{e}"),
        _ => format!("{stem}.conflict-{safe_host}-{ts}"),
    };
    parent.join(new_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_extension() {
        let p = conflict_path(Path::new("/tmp/foo/bar.txt"), "laptop");
        let s = p.to_string_lossy();
        assert!(s.contains("/tmp/foo/bar.conflict-laptop-"));
        assert!(s.ends_with(".txt"));
    }

    #[test]
    fn no_extension() {
        let p = conflict_path(Path::new("/tmp/foo/README"), "laptop");
        let s = p.to_string_lossy();
        assert!(s.contains("README.conflict-laptop-"));
    }

    #[test]
    fn sanitizes_host() {
        let p = conflict_path(Path::new("/tmp/a.md"), "weird name!!");
        assert!(p.to_string_lossy().contains("weird-name--"));
    }
}
