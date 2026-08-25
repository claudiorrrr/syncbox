//! Read-only audit of the docs store vs the blob store.
//!
//! Answers one question: how many bytes of blob data are kept alive only by
//! *losing* doc entries — an older author's row for a key whose winning entry
//! is a newer hash or a tombstone. GC protects every entry in the records
//! table, so those blobs can never be swept.
//!
//! Usage: cargo run -p syncbox-core --example blob_audit -- <docs.redb> <blobs/data>

use anyhow::{Context, Result};
use iroh_docs::store::{fs::Store, Query};
use std::collections::{HashMap, HashSet};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let docs_path = args.next().context("arg 1: path to docs.redb")?;
    let data_path = args.next().context("arg 2: path to blobs/data")?;

    let mut store = Store::persistent(&docs_path)?;
    let namespaces: Vec<_> = store.list_namespaces()?.collect::<Result<Vec<_>, _>>()?;

    // hash -> size, as recorded by the doc entries.
    let mut all_hashes: HashMap<String, u64> = HashMap::new();
    let mut winning: HashSet<String> = HashSet::new();
    let mut losing_rows = 0usize;

    for (ns, _cap) in &namespaces {
        let mut ns_rows = 0usize;
        let mut ns_bytes = 0u64;

        // Every row in the records table — this is exactly what the GC
        // protect callback walks.
        for entry in store.get_many(*ns, Query::all().include_empty())? {
            let entry = entry?;
            let rec = entry.entry().record();

            if rec.content_len() == 0 {
                continue;
            }

            ns_rows += 1;
            ns_bytes += rec.content_len();
            all_hashes.insert(rec.content_hash().to_hex().to_string(), rec.content_len());
        }

        // The subset the sync engine actually mirrors to disk.
        for entry in store.get_many(*ns, Query::single_latest_per_key().include_empty())? {
            let entry = entry?;
            let rec = entry.entry().record();

            if rec.content_len() == 0 {
                continue;
            }

            winning.insert(rec.content_hash().to_hex().to_string());
        }

        println!("ns {ns}  rows={ns_rows}  {:.2} GB", gb(ns_bytes));
    }

    // Safety check after a sweep: a big winning blob with no file on disk means
    // GC took something a folder still needs. Small blobs live inline in
    // blobs.db, so only look at ones too big to inline.
    const INLINE_MAX: u64 = 16 * 1024;
    let mut missing = 0usize;
    for (hash, len) in &all_hashes {
        if !winning.contains(hash) || *len <= INLINE_MAX {
            continue;
        }

        let p = std::path::Path::new(&data_path).join(format!("{hash}.data"));
        if !p.exists() {
            missing += 1;
        }
    }

    let mut dead_bytes = 0u64;
    let mut dead: Vec<(String, u64)> = Vec::new();

    for (hash, len) in &all_hashes {
        if winning.contains(hash) {
            continue;
        }

        losing_rows += 1;
        dead_bytes += len;
        dead.push((hash.clone(), *len));
    }

    // Cross-check against what is actually on disk.
    let mut on_disk = 0u64;
    for (hash, _) in &dead {
        let p = std::path::Path::new(&data_path).join(format!("{hash}.data"));
        if let Ok(md) = std::fs::metadata(&p) {
            on_disk += md.len();
        }
    }

    println!("namespaces:            {}", namespaces.len());
    println!("distinct hashes (all): {}", all_hashes.len());
    println!("distinct hashes (win): {}", winning.len());
    println!("orphan-by-losing-row:  {losing_rows}");
    println!("  bytes per doc:       {:.2} GB", gb(dead_bytes));
    println!("  bytes present on fs: {:.2} GB", gb(on_disk));
    println!("winning but missing:   {missing}");

    dead.sort_by_key(|(_, len)| std::cmp::Reverse(*len));
    println!("\ntop 15 reclaimable blobs:");
    for (hash, len) in dead.iter().take(15) {
        println!("  {:.2} GB  {hash}", gb(*len));
    }

    Ok(())
}

fn gb(bytes: u64) -> f64 {
    bytes as f64 / 1024.0 / 1024.0 / 1024.0
}
