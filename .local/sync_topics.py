from __future__ import annotations

import asyncio
import json
import math
import os
import tempfile
from pathlib import Path
from typing import Any

import httpx
import yaml


CHAT_ID = "<TELEGRAM_FORUM_CHAT_ID>"
GUILD_ID = "<DISCORD_GUILD_ID>"
CATALOG_PATH = Path("/catalog.json")
LOCAL_DIR = Path("/local")
MAP_PATH = LOCAL_DIR / "topic-map.json"
RULES_PATH = LOCAL_DIR / "rules.yaml"
PENDING_PATH = LOCAL_DIR / "topic-create-pending.json"
MAX_RETRY_AFTER_S = 60.0
MAX_ATTEMPTS = 3


def read_regular_text(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"input path must be a regular file: {path.name}")
    return path.read_text(encoding="utf-8")


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}-", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def topic_name(channel: dict[str, Any]) -> str:
    channel_name = str(channel["name"]).strip()
    parent_name = str(channel.get("parent_name") or "").strip()
    value = f"{parent_name}／{channel_name}" if parent_name else channel_name
    return value[:128]


def load_catalog() -> list[dict[str, Any]]:
    catalog = json.loads(read_regular_text(CATALOG_PATH))
    if catalog.get("schema_version") != 1 or str(catalog.get("guild", {}).get("id")) != GUILD_ID:
        raise RuntimeError("catalog does not match requested guild")
    channels = catalog.get("channels")
    if not isinstance(channels, list) or not channels:
        raise RuntimeError("catalog has no channels")
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for channel in channels:
        if not isinstance(channel, dict) or channel.get("status") != "ok":
            raise RuntimeError("catalog contains unresolved channel")
        channel_id = str(channel.get("id") or "")
        if not channel_id.isdigit() or channel_id in seen or not str(channel.get("name") or "").strip():
            raise RuntimeError("catalog contains invalid channel metadata")
        seen.add(channel_id)
        result.append(channel)
    return result


def load_mapping() -> dict[str, int]:
    if MAP_PATH.is_symlink():
        raise RuntimeError("topic map must not be a symlink")
    raw = json.loads(read_regular_text(MAP_PATH)) if MAP_PATH.exists() else {}
    return validate_mapping(raw)


def validate_mapping(raw: Any) -> dict[str, int]:
    if not isinstance(raw, dict):
        raise RuntimeError("topic map must be a mapping")
    result: dict[str, int] = {}
    seen_threads: set[int] = set()
    for channel_id, thread_id in raw.items():
        if (
            not isinstance(channel_id, str)
            or not channel_id.isdecimal()
            or str(int(channel_id)) != channel_id
            or not isinstance(thread_id, int)
            or isinstance(thread_id, bool)
            or thread_id <= 1
            or thread_id in seen_threads
        ):
            raise RuntimeError("topic map contains invalid mapping")
        result[channel_id] = thread_id
        seen_threads.add(thread_id)
    return result


def save_mapping(mapping: dict[str, int]) -> None:
    validated = validate_mapping(mapping)
    atomic_write(MAP_PATH, json.dumps(validated, ensure_ascii=False, indent=2) + "\n")


def save_pending(channel_id: str, name: str) -> None:
    atomic_write(PENDING_PATH, json.dumps({
        "version": 1,
        "channel_id": channel_id,
        "name": name,
    }, ensure_ascii=False, indent=2) + "\n")


def clear_pending() -> None:
    try:
        PENDING_PATH.unlink()
    except FileNotFoundError:
        return
    directory_fd = os.open(PENDING_PATH.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def recover_pending(mapping: dict[str, int]) -> None:
    if PENDING_PATH.is_symlink():
        raise RuntimeError("pending topic creation record must not be a symlink")
    if not PENDING_PATH.exists():
        return
    raw = json.loads(read_regular_text(PENDING_PATH))
    if (
        not isinstance(raw, dict)
        or set(raw) != {"version", "channel_id", "name"}
        or raw.get("version") != 1
        or not isinstance(raw.get("channel_id"), str)
        or not raw["channel_id"].isdecimal()
        or not isinstance(raw.get("name"), str)
        or not raw["name"].strip()
    ):
        raise RuntimeError("invalid pending topic creation record")
    channel_id = raw["channel_id"]
    if channel_id in mapping:
        clear_pending()
        return
    raise RuntimeError(
        f"pending topic creation for channel={channel_id}; reconcile Telegram and topic-map.json before retrying"
    )


def load_enabled() -> dict[str, bool]:
    if RULES_PATH.is_symlink():
        raise RuntimeError("rules file must not be a symlink")
    if not RULES_PATH.exists():
        return {}
    document = yaml.safe_load(read_regular_text(RULES_PATH)) or {}
    if not isinstance(document, dict):
        raise RuntimeError("rules file must be a mapping")
    rules = document.get("rules", [])
    if not isinstance(rules, list):
        raise RuntimeError("rules file has invalid rules")
    result: dict[str, bool] = {}
    for rule in rules:
        if not isinstance(rule, dict) or not isinstance(rule.get("match"), dict):
            raise RuntimeError("rules file contains an invalid rule")
        channel_id = str(rule["match"].get("channel_id") or "")
        enabled = rule.get("enabled", True)
        if not channel_id.isdigit() or channel_id in result or not isinstance(enabled, bool):
            raise RuntimeError("rules file contains invalid channel state")
        result[channel_id] = enabled
    return result


def resolve_enabled(catalog_ids: set[str], mapping: dict[str, int], existing: dict[str, bool]) -> dict[str, bool]:
    if set(mapping) - catalog_ids:
        raise RuntimeError("topic map contains channels outside current catalog")
    if set(existing) - catalog_ids:
        raise RuntimeError("rules file contains channels outside current catalog")
    return {
        channel_id: existing.get(channel_id, channel_id in mapping)
        for channel_id in catalog_ids
    }


def save_rules(channels: list[dict[str, Any]], mapping: dict[str, int], enabled_by_channel: dict[str, bool]) -> None:
    rules = []
    for channel in channels:
        channel_id = str(channel["id"])
        enabled = enabled_by_channel[channel_id]
        rule = {
            "name": f"cat-stocks-{channel_id}",
            "channel_name": topic_name(channel),
            "enabled": enabled,
            "match": {"guild_id": GUILD_ID, "channel_id": channel_id},
        }
        if channel_id in mapping:
            rule.update({
                "action": "forward",
                "forward_to": {"chat_id": CHAT_ID, "thread_id": mapping[channel_id]},
            })
        elif enabled:
            raise RuntimeError("enabled channel has no topic mapping")
        else:
            rule["action"] = "drop"
        rules.append(rule)
    document = {"rules": rules, "default_action": "drop"}
    atomic_write(RULES_PATH, yaml.safe_dump(document, allow_unicode=True, sort_keys=False))


async def telegram_call(client: httpx.AsyncClient, method: str, data: dict[str, Any]) -> Any:
    token = os.environ.get("TG_BOT_TOKEN", "")
    if not token:
        raise RuntimeError("TG_BOT_TOKEN is required")
    for attempt in range(MAX_ATTEMPTS):
        try:
            response = await client.post(f"https://api.telegram.org/bot{token}/{method}", data=data)
        except (httpx.TimeoutException, httpx.NetworkError):
            if attempt == MAX_ATTEMPTS - 1:
                break
            await asyncio.sleep(2 ** attempt)
            continue
        try:
            body = response.json()
        except ValueError:
            body = None
        error_code = body.get("error_code") if isinstance(body, dict) else None
        code = int(error_code) if isinstance(error_code, (int, float)) and not isinstance(error_code, bool) else response.status_code
        if response.status_code == 429 or code == 429:
            try:
                if not isinstance(body, dict):
                    raise TypeError("missing Telegram error body")
                retry_after = float(body["parameters"]["retry_after"])
                if not math.isfinite(retry_after) or retry_after < 0:
                    raise ValueError("invalid retry delay")
            except (KeyError, TypeError, ValueError):
                retry_after = 2 ** attempt
            if attempt < MAX_ATTEMPTS - 1:
                await asyncio.sleep(min(MAX_RETRY_AFTER_S, max(0, retry_after)))
                continue
        elif response.status_code >= 500 or code >= 500:
            if attempt < MAX_ATTEMPTS - 1:
                await asyncio.sleep(2 ** attempt)
                continue
        elif body is None:
            raise RuntimeError(f"{method} returned invalid JSON status={response.status_code}")
        elif response.status_code >= 400 or 400 <= code < 500:
            raise RuntimeError(f"{method} failed status={response.status_code} code={code}")
        elif isinstance(body, dict) and body.get("ok") is True:
            return body.get("result")
        else:
            raise RuntimeError(f"{method} returned a malformed success status={response.status_code}")
        raise RuntimeError(f"{method} failed status={response.status_code} code={code}")
    raise RuntimeError(f"{method} failed after {MAX_ATTEMPTS} attempts")


async def main() -> None:
    channels = load_catalog()
    mapping = load_mapping()
    recover_pending(mapping)
    existing_enabled = load_enabled()
    catalog_ids = {str(channel["id"]) for channel in channels}
    enabled_by_channel = resolve_enabled(catalog_ids, mapping, existing_enabled)

    async with httpx.AsyncClient(trust_env=False, timeout=30) as client:
        for index, channel in enumerate(channels, start=1):
            channel_id = str(channel["id"])
            name = topic_name(channel)
            if not enabled_by_channel[channel_id]:
                action = "disabled"
            elif channel_id in mapping:
                await telegram_call(client, "editForumTopic", {
                    "chat_id": CHAT_ID,
                    "message_thread_id": mapping[channel_id],
                    "name": name,
                })
                action = "renamed"
            else:
                save_pending(channel_id, name)
                topic = await telegram_call(client, "createForumTopic", {
                    "chat_id": CHAT_ID,
                    "name": name,
                })
                if not isinstance(topic, dict):
                    raise RuntimeError("createForumTopic returned an invalid topic")
                thread_id = topic.get("message_thread_id")
                if not isinstance(thread_id, int) or isinstance(thread_id, bool) or thread_id <= 1:
                    raise RuntimeError("createForumTopic returned an invalid thread ID")
                mapping[channel_id] = thread_id
                save_mapping(mapping)
                clear_pending()
                action = "created"
            print(f"{index}/{len(channels)} {action} channel={channel_id} thread={mapping.get(channel_id)}", flush=True)
            await asyncio.sleep(0.25)

    save_mapping(mapping)
    save_rules(channels, mapping, enabled_by_channel)
    print(json.dumps({"channels": len(channels), "topics": len(mapping), "rules_path": str(RULES_PATH)}))


if __name__ == "__main__":
    asyncio.run(main())
