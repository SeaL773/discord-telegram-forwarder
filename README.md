# discord-tg-forwarder

Python 3.11+ Discord → Telegram forwarder consuming the loopback-only `discord-message-bridge` through Docker's `host.docker.internal:17891` gateway. It implements WS-first boundary reconciliation, durable at-least-once fanout, rule hot reload, media forwarding/fallback, rate limits, dead letters, and a minimal health server.

## Setup

1. Keep Bridge bound to `127.0.0.1`. M0 was verified directly with Docker Desktop's `host.docker.internal`; no `netsh` portproxy or firewall change is currently required.
2. Copy `.env.example` to `.env` and fill `TG_BOT_TOKEN` and `BRIDGE_TOKEN`. Never commit `.env`. Compose `env_file` preserves a single `$` literally here, so do not double it. After editing `.env`, run `docker compose up -d`; `docker restart` does not reload environment files.
3. Replace placeholder IDs in `rules.yaml`. Rules are first-match-wins and default to drop.
4. Run `docker compose up -d --build`.

Bridge auth is sent only as an Authorization header. REST uses `after` and `limit=500`. No event body, attachment URL, token, Telegram response body, or auth header is logged.

Discord message and embed-description Markdown headings (`#` through `######`), bold, italic, and bold-italic emphasis are converted to Telegram HTML. User text is HTML-escaped before these supported markers are transformed; unsupported Markdown remains literal.

## Rules

Each optional matcher accepts a scalar or list: `guild_id`, `channel_id`, `event_type`, `author_id`, `author_name`; `keyword` is a case-insensitive regex and `is_dm` is boolean. A rule uses `action: drop` or `forward_to`, which accepts one mapping/scalar or a list. Targets require a nonempty scalar `chat_id`; optional `thread_id` must be a nonnegative integer. Duplicate targets are removed while retaining order. Invalid hot reloads are reported as configuration errors, leave the previous immutable snapshot active, and do not stop the watcher.

Rules are trusted administrator input. Keyword candidate text is bounded to 4096 characters, but pathological regular expressions should still be avoided.

## Reliability

The client connects WS first, receives `ready`, starts its bounded reader, then reconciles REST from the durable cursor through that boundary. A WS failure during reconciliation discards that session replay. On expired cursor (409), it atomically clears in-flight state, increments the gap counter, and acknowledges the ready boundary before attempting a bounded best-effort metadata-only admin alert; it then discards that WS epoch and reconnects so buffered overlap cannot cross the gap boundary. Initial startup takes the newest 500-event snapshot, applies one rules snapshot, forwards only the newest ten matching events per actual channel, and explicitly processes other snapshot entries as drops. Schema-invalid events with a trustworthy cursor, plus explicitly classified deterministic attachment preparation failures, are durably dead-lettered and acknowledged in pipeline order; malformed transport frames without a trustworthy cursor still force a reconnect.

State is one mode-0600 atomic `/data/state.json` in the private mode-0700 Compose volume. First-start bootstrap order, event payloads, exact frozen target/drop decisions, next index, ready boundary, in-flight target phase and retry counts are durable. Recovery finishes persisted in-flight and bootstrap work before opening Bridge, so later rule changes cannot alter those decisions. Target dead letters carry a stable identity: if a process stops after the dead-letter append is fsynced but before terminal state persistence, recovery marks that target terminal without resending or appending a duplicate record; other targets remain independent. This is a recoverable cross-file protocol, not a claim of atomicity across the state and dead-letter files. Dead letters contain sensitive recovery payloads, are mode 0600, require a retention policy, and must not be placed on a shared volume.

Discord embed author/title/description/fields/footer/source links are included in escaped Telegram HTML. Media joins the attachment queue in deterministic `attachments → image → images[] → thumbnail` order, with duplicate URLs removed. Media is downloaded in an ordered prepare stage ahead of the rate-limited send/commit stage, using a bounded prepared queue. Only HTTPS port-443 URLs on the exact hosts `cdn.discordapp.com`, `media.discordapp.net`, `images-ext-1.discordapp.net`, `images-ext-2.discordapp.net`, and `pbs.twimg.com` are accepted; DNS must resolve only to public addresses and redirects are rejected. Bridge, Telegram, and media use separate clients with environment proxy trust disabled. Defaults cap each event at 20 media items, 20 MiB each, and 40 MiB aggregate. Telegram batches preserve order, and burst capacity permits one 2-10 item media group without changing configured sustained refill rates. A failed media target durably switches to a text notification containing fallbacks for all original media URLs; only well-formed HTTP(S) URLs with a host and no whitespace/control characters enter Telegram HTML `href`, while invalid or unsupported values become the fixed plain-text label `Attachment unavailable`. Formatter truncation reserves space for complete fallback entries at both Telegram limits. Recovery preserves each target's independent media/fallback phase. A crash after a partial media batch may duplicate an earlier batch on recovery; this is the intentional at-least-once duplicate window.

The Bridge replay buffer is separately bounded, normally at 10,000 events. If the durable cursor expires, the explicit 409 policy alerts and skips to the current ready boundary.

## Health

`GET /healthz` binds to `127.0.0.1` for the container-internal healthcheck. Connected and idle remains 200 `ok`. It returns 200 `degraded` during the first 300 seconds disconnected and 503 `unhealthy` afterward; it also returns 503 when connected with outstanding queued/in-flight work but no durable cursor progress for 300 seconds. Cursor progress or clearing all outstanding work restores 200. Fields include a cursor prefix, queue depth, in-flight flag, disconnect seconds, last-event age, `stall_seconds`, and a nonsecret `reason`. One admin alert is sent per unhealthy episode and distinguishes a stalled forwarding pipeline from a disconnected Bridge; recovery produces no alert.

## Development

Use Docker rather than system Python:

```sh
docker run --rm -v "$PWD:/workspace:ro" python:3.12-slim sh -c 'cp -R /workspace /tmp/project && cd /tmp/project && pip install -r requirements-dev.txt && pytest -q && python -m compileall -q src tests'
docker compose config
```
