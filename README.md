# discord-tg-forwarder

Python 3.11+ Discord → Telegram forwarder consuming the loopback-only `discord-message-bridge` through Docker's `host.docker.internal:17891` gateway. It implements WS-first boundary reconciliation, durable at-least-once fanout, rule hot reload, media forwarding/fallback, rate limits, dead letters, and a minimal health server.

For a complete collector → Bridge → Forwarder deployment runbook, see
[`docs/installation.md`](docs/installation.md).

## Setup

Docker Compose 2.24.0 or newer is required because `docker-compose.yml` uses the long-form `env_file.required` option.

1. Keep Bridge bound to `127.0.0.1`. Docker Desktop exposes the host through `host.docker.internal`, so a `netsh` portproxy or firewall exception is normally unnecessary.
2. Copy `.env.example` to `.env` and fill `TG_BOT_TOKEN`, `BRIDGE_TOKEN`, and numeric `ADMIN_CHAT_ID`. Never commit `.env`. Compose `env_file` preserves a single `$` literally here, so do not double it. After editing `.env`, run `docker compose up -d`; `docker restart` does not reload environment files.
3. `ADMIN_CHAT_ID` identifies the Telegram chat that receives metadata-only health and replay-gap alerts; it is required but is not embedded in tracked configuration.
4. Replace placeholder IDs in `rules.yaml`. Rules are first-match-wins and default to drop.
5. Run `docker compose up -d --build`.

Bridge auth is sent only as an Authorization header. REST uses `after` and `limit=500`. No event body, attachment URL, token, Telegram response body, or auth header is logged.

Discord message and embed-description Markdown headings (`#` through `######`), bold, italic, and bold-italic emphasis are converted to Telegram HTML. User text is HTML-escaped before these supported markers are transformed; unsupported Markdown remains literal. `telegram.rich_messages_enabled` is a strict boolean and defaults to `false`. When enabled, short messages retain compact classic HTML, while replies, embeds, fenced code, multiple extracted media items, long content, and meaningful edits use Telegram Bot API 10.2 Rich Messages when all downloaded media is photo or video. Rich media is uploaded directly in one multipart request; source attachment URLs are not delegated to Telegram for fetching.

## Rules

Each optional matcher accepts a scalar or list: `guild_id`, `channel_id`, `event_type`, `author_id`, `author_name`; `keyword` is a case-insensitive regex and `is_dm` is boolean. `channel_name` is optional display metadata that makes ID-based rules readable. `enabled` is optional and defaults to `true` for backward compatibility. A matching disabled rule is a terminal drop, so hot reload stops forwarding that channel immediately without falling through to later or default rules.

A rule uses `action: drop` or `forward_to`, which accepts one mapping/scalar or a list. Targets require a nonempty scalar `chat_id`; optional `thread_id` must be a nonnegative integer. Duplicate targets are removed while retaining order. Invalid hot reloads are reported as configuration errors, leave the previous immutable snapshot active, and do not stop the watcher. At process startup, disabled Telegram forum targets are closed and previously disabled targets that become enabled are reopened. The General topic is never managed, and topics are never deleted, so existing history remains available. Successful topic state is stored locally to avoid repeating unchanged API calls on every restart.

Rules are trusted administrator input. Keyword candidate text is bounded to 4096 characters, but pathological regular expressions should still be avoided.

### Forum topic controls

An optional deployment override can mount `.local/rules.yaml` read-only into the container. Each generated rule keeps its stable Discord channel ID and adds a readable `channel_name`; normally the only field to edit by hand is `enabled`.

- Changing `enabled` is hot-reloaded within about one second and immediately changes routing.
- Telegram `closeForumTopic` / `reopenForumTopic` synchronization is intentionally startup-only. After editing switches, restart with `docker compose -f docker-compose.yml -f .local/compose.override.yaml restart forwarder`.
- Existing mapped channels retain their previous switch value. Legacy rules without `enabled` remain enabled. A newly discovered channel with no topic mapping defaults to disabled and does not create a Topic.
- To opt into a new channel, first generate the disabled readable rule, set `enabled: true`, then rerun the helper so it creates the Topic and writes the target mapping.

Refresh the catalog and topic rules with the tracked helper after setting `CATALOG_PATH` to the collector's `channel-catalog.result.json`. The helper reads `TG_FORUM_CHAT_ID` and `DISCORD_GUILD_ID` from `.env`; neither deployment ID is embedded in the tracked script.

```sh
docker compose -f docker-compose.yml -f .local/compose.override.yaml build forwarder
docker compose -f docker-compose.yml -f .local/compose.override.yaml run --rm \
  -v "$CATALOG_PATH:/catalog.json:ro" \
  -v "$PWD/.local:/local" \
  -v "$PWD/.local/sync_topics.py:/sync_topics.py:ro" \
  forwarder python /sync_topics.py
```

Topic creation uses a durable `.local/topic-create-pending.json` intent. If creation may have succeeded but the mapping could not be saved, later runs stop instead of creating a duplicate. Reconcile that channel against Telegram, add the confirmed thread ID to `topic-map.json`, and rerun; the helper then clears the completed intent. Never clear an unresolved intent and retry blindly.

Telegram does not expose a Topic state query suitable for reconciliation. On the first upgrade, enabled Topics are recorded as the baseline without issuing reopen calls for every enabled Topic. Therefore manual Telegram-side changes can drift from local state; use this rules-driven workflow rather than manually closing Topics. Successful API state is local evidence, not an authoritative Telegram query result. At startup, cached Topic states are pruned to the targets referenced by the current rules; removing and later re-adding a target establishes a new local baseline.

Treat `.env`, `.local/rules.yaml`, `.local/topic-map.json`, and `.local/sync_topics.py` as administrator-only control files. On Windows, remove inherited `Authenticated Users` write access and grant only the deployment account, `SYSTEM`, and `Administrators`; POSIX mode bits shown for DrvFS mounts are not a substitute for Windows ACLs.

## Reliability

The client connects WS first, receives `ready`, starts its bounded reader, then reconciles REST from the durable cursor through that boundary. A WS failure during reconciliation discards that session replay. On expired cursor (409), it atomically clears in-flight state, increments the gap counter, and acknowledges the ready boundary before attempting a bounded best-effort metadata-only admin alert; it then discards that WS epoch and reconnects so buffered overlap cannot cross the gap boundary. Initial startup takes the newest 500-event snapshot, applies one rules snapshot, forwards only the newest ten matching events per actual channel, and explicitly processes other snapshot entries as drops. Schema-invalid events with a trustworthy cursor, plus explicitly classified deterministic attachment preparation failures, are durably dead-lettered and acknowledged in pipeline order; malformed transport frames without a trustworthy cursor still force a reconnect.

State is one mode-0600 atomic `/data/state.json` in the private mode-0700 Compose volume. First-start bootstrap order, event payloads, exact frozen target/drop decisions, next index, ready boundary, in-flight target phase (`rich` → `media` → `fallback`), retry counts, and successful synchronization states for currently managed Topics are durable. A persisted rich target resumes rich delivery even if the feature gate is later disabled. Rate limits do not consume retry budget. Ambiguous rich network, timeout, 5xx, or malformed-success outcomes retry only rich and dead-letter on exhaustion; they never cross-format fallback. Only definite 400, 404, or 413 Rich Message rejection durably enters classic media delivery, while 401 and 403 dead-letter directly. Legacy media/fallback state remains loadable. Topic-state pruning does not alter cursor, bootstrap, or in-flight recovery data. Recovery finishes persisted in-flight and bootstrap work before opening Bridge, so later rule changes cannot alter those already-frozen decisions; disabling a rule is not a retroactive cancellation of durable work. Target dead letters carry a stable identity: if a process stops after the dead-letter append is fsynced but before terminal state persistence, recovery scans the active dead-letter file and all retained rotations, marks that target terminal without resending or appending a duplicate record, and leaves other targets independent. This is a recoverable cross-file protocol, not a claim of atomicity across the state and dead-letter files.

### Privacy and retention

- `/data/state.json` can temporarily contain complete Discord event payloads for bootstrap and in-flight recovery, including messages that routing will ultimately drop.
- `/data/failed-events.ndjson` contains complete failed events and Telegram destination IDs. Before an append would make the active file exceed `state.dead_letter_max_bytes`, the forwarder first writes and fsyncs that complete record to a same-directory `.pending` file, then shifts the active file to `.1` and older files through `.N`, and finally atomically promotes `.pending` to active. It retains exactly `state.dead_letter_backup_count` backups. Defaults are 32 MiB and two backups, so steady-state retained storage is at most three generations and approximately 96 MiB total; during an interrupted rotation one additional `.pending` generation can exist temporarily, and a single record larger than 32 MiB may make the newly created active file exceed that approximation because records are never silently truncated or redacted.
- Rotation uses same-directory replace operations, mode-0600 files, parent-directory fsync, and regular-file/symlink checks. Startup recovery also scans and completes an interrupted `.pending` rotation. Identity scanning remains bounded even when a complete target record exceeds the normal recovery-line parser limit because target identities are serialized before the full payload.
- There is intentionally no age-based deletion: expiring a rotated record by age could erase the stable identity still needed after a crash between dead-letter fsync and state persistence. Size/count retention is finite while preserving every identity that remains within the configured recovery window.
- Disabling or removing a rule prevents future routing but does not retroactively erase durable state, dead letters, Telegram messages, Bridge replay memory, or collector NDJSON files.
- Keep the `/data` volume private and exclude it, `.env`, `.local/rules.yaml`, `.local/topic-map.json`, catalog results, logs, and backups from source control and shared backup destinations.
- Discord message content may include personal data and private attachment URLs. Operate this pipeline only for accounts, servers, channels, and Telegram destinations where you are authorized to collect and forward the data.

Bootstrap and in-flight payloads remain in `state.json` until their ordered recovery step is durably completed. They are not minimized earlier because doing so would remove the frozen event/decision data required to preserve current ordered recovery semantics.

Before publishing a fork, scan the complete Git history as well as the current tree. Numeric chat/channel/guild IDs are not authentication secrets, but they can identify private communities and should be replaced with synthetic examples. Earlier revisions of a privately deployed repository may still contain those identifiers after the current tree is sanitized; rewrite all affected refs or publish a clean history containing only the reviewed tree.

Discord embed author/title/description/fields/footer/source links are included in escaped Telegram HTML. Media joins the attachment queue in deterministic `attachments → image → images[] → thumbnail` order, with duplicate URLs removed. Media is downloaded in an ordered prepare stage ahead of the rate-limited send/commit stage, using a bounded prepared queue. Only HTTPS port-443 URLs on the exact hosts `cdn.discordapp.com`, `media.discordapp.net`, `images-ext-1.discordapp.net`, `images-ext-2.discordapp.net`, and `pbs.twimg.com` are accepted; DNS must resolve only to public addresses and redirects are rejected. Bridge, Telegram, and media use separate clients with environment proxy trust disabled. Defaults cap each event at 20 media items, 20 MiB each, and 40 MiB aggregate. Telegram batches preserve order, and burst capacity permits one 2-10 item media group without changing configured sustained refill rates. A failed media target durably switches to a text notification containing fallbacks for all original media URLs; only well-formed HTTP(S) URLs with a host and no whitespace/control characters enter Telegram HTML `href`, while invalid or unsupported values become the fixed plain-text label `Attachment unavailable`. Formatter truncation reserves space for complete fallback entries at both Telegram limits. Recovery preserves each target's independent media/fallback phase. A crash after a partial media batch may duplicate an earlier batch on recovery; this is the intentional at-least-once duplicate window.

The Bridge replay buffer is separately bounded, normally at 10,000 events. If the durable cursor expires, the explicit 409 policy alerts and skips to the current ready boundary.

## Health

`GET /healthz` binds to `127.0.0.1` for the container-internal healthcheck. Connected and idle remains 200 `ok`. It returns 200 `degraded` during the first 300 seconds disconnected and 503 `unhealthy` afterward; it also returns 503 when connected with outstanding queued/in-flight work but no durable cursor progress for 300 seconds. Cursor progress or clearing all outstanding work restores 200. Fields include a cursor prefix, queue depth, in-flight flag, disconnect seconds, last-event age, `stall_seconds`, and a nonsecret `reason`. One admin alert is sent per unhealthy episode and distinguishes a stalled forwarding pipeline from a disconnected Bridge; recovery produces no alert.

## Development

Use Docker rather than system Python:

```sh
docker run --rm -v "$PWD:/workspace:ro" python:3.12-slim sh -c 'mkdir /tmp/project && cp -a /workspace/. /tmp/project/ && cd /tmp/project && pip install -r requirements-dev.txt && pytest -q && python -m compileall -q src tests .local/sync_topics.py'
docker compose config
```

## License and upstream boundary

This forwarder is a separate process that communicates with `discord-message-bridge` over HTTP/WebSocket; it does not include Vencord or `MessageLoggerEnhanced` source code. It is released under the **MIT License** (see `LICENSE`). The upstream collector patch has separate GPL-3.0 obligations documented in the Bridge repository.
