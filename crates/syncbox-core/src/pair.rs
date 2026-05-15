//! Short-code pairing client.
//!
//! Talks to the rendezvous server (`pair-server/`, a Cloudflare Worker by
//! default). One side `POST`s its iroh ticket and gets a 6-character code;
//! the other side `GET`s the code back into a ticket. The server only ever
//! holds the (already public) ticket, briefly.

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize)]
struct CodeResp {
    code: String,
    expires: u64,
}

#[derive(Deserialize)]
struct TicketResp {
    ticket: String,
}

/// A freshly issued pairing code.
#[derive(Debug, Clone)]
pub struct PairCode {
    pub code: String,
    pub expires_unix: u64,
}

/// Resolve which pair-server URL to use. Precedence, highest first:
///   1. `SYNCBOX_PAIR_SERVER` environment variable
///   2. the `configured` value (from config.json / a `--pair-server` flag)
///   3. the compiled-in [`crate::config::DEFAULT_PAIR_SERVER`]
pub fn resolve_server(configured: Option<&str>) -> String {
    std::env::var("SYNCBOX_PAIR_SERVER")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            configured
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| crate::config::DEFAULT_PAIR_SERVER.to_string())
        .trim_end_matches('/')
        .to_string()
}

fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build http client")
}

/// Publish `ticket` to the rendezvous server, returning the short code.
pub async fn create_code(server: &str, ticket: &str) -> Result<PairCode> {
    let res = client()?
        .post(format!("{server}/pair"))
        .json(&serde_json::json!({ "ticket": ticket }))
        .send()
        .await
        .context("pair server unreachable")?;
    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        bail!("pair server {status}: {body}");
    }
    let parsed: CodeResp = res.json().await.context("parse pair-server response")?;
    Ok(PairCode {
        code: parsed.code,
        expires_unix: parsed.expires,
    })
}

/// Redeem a short code back into a ticket. Accepts `ABC-123` or `ABC123`.
pub async fn redeem_code(server: &str, code: &str) -> Result<String> {
    let normalized: String = code
        .trim()
        .to_uppercase()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect();
    if normalized.len() != 6 {
        bail!("code must be 6 characters");
    }
    let display = format!("{}-{}", &normalized[..3], &normalized[3..]);

    let res = client()?
        .get(format!("{server}/pair/{display}"))
        .send()
        .await
        .context("pair server unreachable")?;
    if res.status() == reqwest::StatusCode::NOT_FOUND {
        bail!("code not found or expired");
    }
    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        bail!("pair server {status}: {body}");
    }
    let parsed: TicketResp = res.json().await.context("parse pair-server response")?;
    Ok(parsed.ticket)
}
