# syncbox pair-server

A Cloudflare Worker that swaps a 6-character code for an iroh `DocTicket`.

## Why

Pairing two devices in the syncbox app means handing the iroh ticket from one
to the other. The raw ticket is ~200 characters of base32 — fine for
copy-paste, terrible to read out over a phone call. This worker is a
short-lived dead-drop:

```
device A  ──POST /pair {ticket}──▶  worker  ──code──▶  device A
device B  ──GET  /pair/<code>───▶   worker  ──ticket──▶  device B
                                    (deletes)
```

- Codes are 6 alphanumeric characters from a confusable-free alphabet
  (`ABCDEFGHJKMNPQRSTUVWXYZ23456789`), displayed as `ABC-123`.
- Codes expire in 5 minutes.
- Each code is single-use: the GET deletes the entry.
- The worker never sees plaintext sync data — only the iroh ticket, which is
  itself just public discovery info plus a doc capability.

## Deploy

Pick the host you want to serve from and set it in `wrangler.toml` — replace
`pair.DOMAIN.COM` in the `routes` block with your own subdomain. The zone
must live in the Cloudflare account you log in with; wrangler then creates
the DNS record and TLS certificate automatically.

```bash
cd pair-server
bun install                                    # or npm/pnpm
bunx wrangler login                            # one-time
bunx wrangler kv namespace create PAIR         # prints an id
# paste the id into wrangler.toml under [[kv_namespaces]] -> id
# edit wrangler.toml: routes pattern -> pair.<your-domain>
bunx wrangler deploy
```

Verify it's live:

```bash
curl https://pair.<your-domain>/                # → "syncbox pair-server"
```

Point each Mac at it — one of:

- set the `SYNCBOX_PAIR_SERVER` environment variable, or
- type the URL into the syncbox window under **Advanced → Pair server URL**.

The host is never hard-coded in the client binary.

## Endpoints

`POST /pair`  →  `{code: "ABC-123", expires: <unix-seconds>}`
Body: `{"ticket": "<iroh DocTicket string>"}` (max 4 KiB).

`GET /pair/<code>`  →  `{ticket: "<iroh DocTicket string>"}`
404 once expired or already redeemed. Hyphen in the code is optional.

`GET /`  →  liveness ping.

## Cost

Single Worker request per pair. Free tier (100k req/day) is plenty for
personal use.
