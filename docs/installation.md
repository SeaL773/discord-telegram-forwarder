# Installation and Deployment Guide

This guide is written as an operator runbook. It is suitable for a human or an
LLM agent to execute step by step; there is intentionally no one-click installer.

## Architecture

The deployment has three independently managed components:

1. A patched Vencord `MessageLoggerEnhanced` collector running inside Discord
   Desktop on Windows.
2. `discord-message-bridge` on Windows, reading the collector NDJSON journal
   and exposing an authenticated loopback REST/WebSocket API.
3. `discord-tg-forwarder` in Docker, consuming the Bridge and sending selected
   events to Telegram.

## Prerequisites

- Windows Discord Stable.
- Node.js 20 or newer and pnpm.
- Git.
- Docker Desktop with Docker Compose 2.35.0 or newer.
- A Telegram bot token and the numeric chat IDs used for alerts and forwarding.
- Authorization to collect and forward messages from every selected Discord
  channel.

Keep the Bridge bound to `127.0.0.1`. Do not expose its plain HTTP/WebSocket
endpoint to a LAN or the public internet.

## 1. Install the collector and Bridge

Clone `discord-message-bridge`, then follow its `README.md` to:

1. Check out the documented pinned Vencord and MessageLoggerEnhanced commits.
2. Apply `patches/message-logger-enhanced-collector.patch` to the plugin.
3. Run the patch verifier against the clean source checkouts.
4. Build and inject Vencord into Discord Stable.
5. Start the Bridge and keep its generated bearer token private.

Before enabling `MessageLoggerEnhanced`, generate a narrow
`selected_channels` catalog request with the Bridge repository's offline
generator. The request is the mandatory NDJSON allowlist. Re-enable the plugin
or reconnect Discord after replacing it.

For new installations the plugin, created-message persistence, broad server
caching, and attachment downloads default to disabled. Explicitly enable only
the plugin settings required for the selected deployment. Attachment downloads
are not required by the Forwarder and should normally remain disabled.

Verify the Bridge locally:

```powershell
$token = (Get-Content "$env:APPDATA\Vencord\MessageLoggerData\bridge-token.txt" -Raw).Trim()
curl.exe "http://127.0.0.1:17891/v1/health"
curl.exe -H "Authorization: Bearer $token" "http://127.0.0.1:17891/v1/events?limit=1"
```

Expected health response:

```json
{"status":"ok"}
```

## 2. Configure the Forwarder

Copy the environment template:

```sh
cp .env.example .env
```

Set these required values in `.env`:

- `TG_BOT_TOKEN`: Telegram bot token.
- `BRIDGE_TOKEN`: bearer token created by the Bridge.
- `ADMIN_CHAT_ID`: Telegram chat receiving metadata-only health/gap alerts.

The optional Topic provisioning helper also reads:

- `TG_FORUM_CHAT_ID`
- `DISCORD_GUILD_ID`

Never commit `.env`, catalog results, local rules, Topic mappings, runtime
state, or dead-letter files.

## 3. Configure routing

Edit `rules.yaml` or mount a private rules file. Rules are first-match-wins and
the safe default action is `drop`. Start with disabled rules and enable only
the Discord channels that should be forwarded.

Each forwarding target requires a Telegram `chat_id`; forum destinations also
require a `thread_id`. The tracked `rules.yaml` contains placeholders only.

Private deployments can use `.local/sync_topics.py` to generate readable rules
and Telegram forum Topics from a collector catalog. Follow the command and
recovery guidance in the main `README.md`; unresolved Topic creation intents
must be reconciled rather than blindly retried.

## 4. Build and start

```sh
docker compose up -d --build
```

After changing `.env`, use `docker compose up -d`; `docker restart` does not
reload Compose environment files.

Inspect status without printing secrets:

```sh
docker compose ps
docker compose logs --tail=100 forwarder
```

The container-internal health endpoint should report `ok` while connected and
idle, `degraded` during the initial disconnect grace period, and `unhealthy`
after a prolonged disconnect or forwarding stall.

## 5. Upgrade safely

1. Review upstream commit changes before updating either pinned dependency.
2. Regenerate and verify the canonical collector patch.
3. Run the Bridge lint, typecheck, tests, build, patch verification, and full
   pinned Vencord build.
4. Run the Forwarder test, compile, and Compose validation commands documented
   in `README.md`.
5. Rebuild/reinject Vencord, restart Discord, and confirm the active catalog
   scope before starting downstream forwarding.
6. Rebuild the Forwarder container and verify health and one selected channel.

## Data deletion

`Clear All Local Logs` in the patched plugin independently attempts to clear
IndexedDB and both NDJSON journals. Restart the Bridge afterward to clear its
in-memory replay buffer. This does not remove Forwarder state/dead letters,
Telegram messages, backups, or synchronized copies; handle each separately.
