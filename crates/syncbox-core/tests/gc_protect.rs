//! GC must sweep a blob that only a *losing* doc row still points at.
//!
//! iroh-docs keys entries by `(namespace, key, author)`. When device B replaces
//! or deletes a file device A wrote, A's row survives and keeps pointing at the
//! superseded hash — `doc.del` is author-scoped, and an edit writes a row under
//! the editing author only. iroh-docs' own protect callback walks *every* row,
//! so that dead blob is pinned for the life of the store, on every peer.
//!
//! syncbox protects only the winning entry per key, which is exactly the set
//! `reconcile_remote` mirrors to disk.

use anyhow::{bail, Result};
use bytes::Bytes;
use futures_lite::StreamExt;
use iroh_docs::store::Query;
use std::time::{Duration, Instant};
use syncbox_core::peer::Node;

const KEY: &[u8] = b"clip.mov";
const OLD_CONTENT: &[u8] = b"superseded bytes, nobody wants these";
const NEW_CONTENT: &[u8] = b"the version that won";

const GC_INTERVAL: Duration = Duration::from_secs(1);
const SWEEP_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test]
async fn sweeps_blob_kept_alive_by_losing_row() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let node = Node::spawn_with_gc(dir.path(), GC_INTERVAL).await?;

    let doc = node.docs.create().await?;
    let author_a = node.docs.api().author_default().await?;
    let author_b = node.docs.api().author_create().await?;

    // Two blobs in the store. `temp_tag` rather than the default permanent
    // tag: a tag is itself a GC root, which would mask what we're testing.
    let old_tag = node.store.add_slice(OLD_CONTENT).temp_tag().await?;
    let new_tag = node.store.add_slice(NEW_CONTENT).temp_tag().await?;
    let old = old_tag.hash();
    let new = new_tag.hash();

    // Device A published the old version; device B then replaced it. Both rows
    // now live in the records table, B's newer.
    doc.set_hash(
        author_a,
        Bytes::from_static(KEY),
        old,
        OLD_CONTENT.len() as u64,
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    doc.set_hash(
        author_b,
        Bytes::from_static(KEY),
        new,
        NEW_CONTENT.len() as u64,
    )
    .await?;

    // Guard the premise: B must be the winner, or the test proves nothing.
    let winner = doc.get_one(Query::single_latest_per_key()).await?;
    match winner {
        Some(entry) if entry.content_hash() == new => {}
        other => bail!("expected B's entry to win, got {other:?}"),
    }

    // Both rows are still there — this is the shape GC has to cope with.
    let rows = doc.get_many(Query::all()).await?.count().await;
    assert_eq!(rows, 2, "both authors' rows should survive");

    drop(old_tag);
    drop(new_tag);

    let swept = wait_for_sweep(&node, old).await?;
    assert!(swept, "superseded blob still present after GC");
    assert!(
        node.store.blobs().has(new).await?,
        "winning blob was swept — GC protected the wrong set"
    );

    Ok(())
}

/// Poll until the blob is gone, or we run out of patience.
async fn wait_for_sweep(node: &Node, hash: iroh_blobs::Hash) -> Result<bool> {
    let deadline = Instant::now() + SWEEP_TIMEOUT;

    while Instant::now() < deadline {
        if !node.store.blobs().has(hash).await? {
            return Ok(true);
        }
        tokio::time::sleep(GC_INTERVAL).await;
    }

    Ok(false)
}
