// syncbox pair-server
//
// Tiny Cloudflare Worker that swaps a one-time 6-character code for an iroh
// DocTicket. Both sides of the pairing flow are stateless and the worker
// only ever sees the (already-public) ticket — no auth, no logging.
//
// POST /pair        body: {ticket: "..."}  → {code: "ABC-123", expires}
// GET  /pair/<code>                          → {ticket: "..."} (then deletes)

export interface Env {
  PAIR: KVNamespace;
}

const TTL_SECONDS = 5 * 60;
const ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"; // no 0/O/1/I/L
const CODE_LEN = 6;
const MAX_TICKET_BYTES = 4096;

function randomCode(): string {
  const buf = new Uint8Array(CODE_LEN);
  crypto.getRandomValues(buf);
  let out = "";
  for (let i = 0; i < CODE_LEN; i++) {
    out += ALPHABET[buf[i] % ALPHABET.length];
  }
  return out.slice(0, 3) + "-" + out.slice(3);
}

function corsHeaders(): Record<string, string> {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET, POST, OPTIONS",
    "access-control-allow-headers": "content-type",
    "access-control-max-age": "86400",
  };
}

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...corsHeaders() },
  });
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);

    if (req.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    if (req.method === "POST" && url.pathname === "/pair") {
      let body: { ticket?: string };
      try {
        body = await req.json();
      } catch {
        return json(400, { error: "invalid json" });
      }
      const ticket = (body.ticket ?? "").trim();
      if (!ticket) return json(400, { error: "missing ticket" });
      if (ticket.length > MAX_TICKET_BYTES) {
        return json(413, { error: "ticket too large" });
      }

      // Try a few times in the unlikely event of collision with an active code.
      for (let attempt = 0; attempt < 5; attempt++) {
        const code = randomCode();
        const key = `c:${code}`;
        const existing = await env.PAIR.get(key);
        if (existing) continue;
        await env.PAIR.put(key, ticket, { expirationTtl: TTL_SECONDS });
        return json(200, {
          code,
          expires: Math.floor(Date.now() / 1000) + TTL_SECONDS,
        });
      }
      return json(503, { error: "code allocation failed, retry" });
    }

    if (req.method === "GET" && url.pathname.startsWith("/pair/")) {
      const code = url.pathname.slice("/pair/".length).toUpperCase().trim();
      // Accept both "ABC-123" and "ABC123" formats.
      const normalized = code.replace(/-/g, "");
      if (normalized.length !== CODE_LEN) {
        return json(400, { error: "invalid code" });
      }
      const display = normalized.slice(0, 3) + "-" + normalized.slice(3);
      const key = `c:${display}`;
      const ticket = await env.PAIR.get(key);
      if (!ticket) return json(404, { error: "not found or expired" });
      // One-shot: delete after read.
      await env.PAIR.delete(key);
      return json(200, { ticket });
    }

    if (req.method === "GET" && url.pathname === "/") {
      return new Response("syncbox pair-server\n", {
        status: 200,
        headers: corsHeaders(),
      });
    }

    return json(404, { error: "not found" });
  },
};
