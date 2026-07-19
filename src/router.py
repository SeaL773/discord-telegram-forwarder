from __future__ import annotations

import re
import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .LoggerManager import log_error
from .models import Target


FIELDS = ("guild_id", "channel_id", "event_type", "author_id", "author_name")


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else [value]


def _message(event: dict[str, Any]) -> dict[str, Any]:
    value = event.get("message", {})
    return value if isinstance(value, dict) else {}


def event_value(event: dict[str, Any], field: str) -> Any:
    message = _message(event)
    aliases = {
        "guild_id": ("guild_id", "guildId"),
        "channel_id": ("channel_id", "channelId"),
        "author_id": ("author_id", "authorId"),
        "author_name": ("author_name", "authorName", "global_name", "globalName", "username"),
    }
    if field == "event_type":
        return event.get("event_type")
    for key in aliases[field]:
        if key in message:
            return message[key]
        if key in event:
            return event[key]
    author = message.get("author")
    if isinstance(author, dict):
        if field == "author_id":
            return author.get("id")
        if field == "author_name":
            return author.get("display_name") or author.get("displayName") or author.get("global_name") or author.get("globalName") or author.get("username")
    return None


@dataclass(frozen=True, slots=True)
class Rule:
    name: str
    match: dict[str, Any]
    targets: tuple[Target, ...]
    drop: bool


@dataclass(frozen=True, slots=True)
class RuleSnapshot:
    rules: tuple[Rule, ...]
    default_targets: tuple[Target, ...]

    def route(self, event: dict[str, Any]) -> list[Target]:
        for rule in self.rules:
            if _matches(rule.match, event):
                return [] if rule.drop else list(rule.targets)
        return list(self.default_targets)


def _targets(value: Any) -> tuple[Target, ...]:
    if value is None:
        return ()
    entries = value if isinstance(value, list) else [value]
    result: list[Target] = []
    seen: set[str] = set()
    for entry in entries:
        entry = {"chat_id": entry} if not isinstance(entry, dict) else entry
        target = Target(str(entry["chat_id"]), int(entry["thread_id"]) if entry.get("thread_id") is not None else None)
        if target.key not in seen:
            seen.add(target.key)
            result.append(target)
    return tuple(result)


def _matches(match: dict[str, Any], event: dict[str, Any]) -> bool:
    for field in FIELDS:
        if field in match:
            expected = {str(v) for v in _list(match[field])}
            if str(event_value(event, field)) not in expected:
                return False
    message = _message(event)
    content = str(message.get("content", event.get("content", "")))[:4096]
    if "keyword" in match and re.search(str(match["keyword"]), content, re.IGNORECASE) is None:
        return False
    if "is_dm" in match:
        guild = event_value(event, "guild_id")
        actual = bool(message.get("is_dm", event.get("is_dm", guild in (None, ""))))
        if actual is not bool(match["is_dm"]):
            return False
    return True


def parse_rules(raw: Any) -> RuleSnapshot:
    if not isinstance(raw, dict):
        raise ValueError("rules root must be a mapping")
    parsed: list[Rule] = []
    raw_rules = raw.get("rules", [])
    if not isinstance(raw_rules, list):
        raise ValueError("rules must be a list")
    allowed_match = set(FIELDS) | {"keyword", "is_dm"}
    for item in raw_rules:
        if not isinstance(item, dict) or not isinstance(item.get("match", {}), dict):
            raise ValueError("invalid rule")
        match = item.get("match", {})
        unknown = set(match) - allowed_match
        if unknown:
            raise ValueError("unknown match field")
        if "is_dm" in match and not isinstance(match["is_dm"], bool):
            raise ValueError("is_dm must be boolean")
        for field in FIELDS:
            if field in match and isinstance(match[field], (dict, bool)):
                raise ValueError("invalid match value")
        if "keyword" in match:
            re.compile(str(match["keyword"]))
        action = item.get("action")
        if action not in (None, "drop", "forward"):
            raise ValueError("invalid action")
        drop = action == "drop" or item.get("drop") is True
        targets = _targets(item.get("forward_to"))
        if drop and targets:
            raise ValueError("drop and forward_to are mutually exclusive")
        if not drop and not targets:
            raise ValueError("forward rule has no targets")
        parsed.append(Rule(str(item.get("name", "unnamed")), dict(match), targets, drop))
    default = raw.get("default_action", "drop")
    if default == "drop":
        default_targets: tuple[Target, ...] = ()
    elif isinstance(default, dict):
        default_targets = _targets(default.get("forward_to"))
    else:
        raise ValueError("default forward must define targets")
    return RuleSnapshot(tuple(parsed), default_targets)


class Router:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.snapshot = self._read()
        self._mtime_ns = self.path.stat().st_mtime_ns

    def _read(self) -> RuleSnapshot:
        return parse_rules(yaml.safe_load(self.path.read_text(encoding="utf-8")) or {})

    def reload_if_changed(self) -> bool:
        mtime = self.path.stat().st_mtime_ns
        if mtime == self._mtime_ns:
            return False
        candidate = self._read()
        self.snapshot = candidate
        self._mtime_ns = mtime
        return True

    def route(self, event: dict[str, Any]) -> list[Target]:
        return self.snapshot.route(event)

    async def watch(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                self.reload_if_changed()
            except (OSError, ValueError, yaml.YAMLError, re.error) as exc:
                log_error(f"rules reload rejected error={type(exc).__name__}")
            try:
                await asyncio.wait_for(stop.wait(), timeout=1)
            except TimeoutError:
                continue
