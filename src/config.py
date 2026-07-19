from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


_ENV = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")


@dataclass(frozen=True, slots=True)
class AppConfig:
    bridge_url: str
    reconnect_max_backoff_s: float
    telegram_global_per_s: float
    telegram_chat_per_min: float
    media_max_bytes: int
    media_timeout_s: float
    media_max_attachments: int
    media_max_total_bytes: int
    prepared_queue_size: int
    state_path: Path
    dead_letter_path: Path
    rules_path: Path
    health_host: str
    health_port: int
    admin_chat_id: str
    queue_size: int
    tg_token: str
    bridge_token: str


def _expand(value: Any) -> Any:
    if isinstance(value, str):
        return _ENV.sub(lambda m: os.environ.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def load_config(path: str | Path = "config.yaml") -> AppConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("config root must be a mapping")
    raw = _expand(raw)
    bridge = raw.get("bridge", {})
    telegram = raw.get("telegram", {})
    state = raw.get("state", {})
    health = raw.get("health", {})
    config = AppConfig(
        bridge_url=str(bridge.get("url", "http://host.docker.internal:17891")).rstrip("/"),
        reconnect_max_backoff_s=float(bridge.get("reconnect_max_backoff_s", 60)),
        telegram_global_per_s=float(telegram.get("rate_limit_global_per_s", 25)),
        telegram_chat_per_min=float(telegram.get("rate_limit_per_chat_per_min", 18)),
        media_max_bytes=int(telegram.get("media_max_bytes", 20 * 1024 * 1024)),
        media_timeout_s=float(telegram.get("media_download_timeout_s", 15)),
        media_max_attachments=int(telegram.get("media_max_attachments", 20)),
        media_max_total_bytes=int(telegram.get("media_max_total_bytes", 40 * 1024 * 1024)),
        prepared_queue_size=int(raw.get("prepared_queue_size", 4)),
        state_path=Path(state.get("path", "/data/state.json")),
        dead_letter_path=Path(state.get("dead_letter_path", "/data/failed-events.ndjson")),
        rules_path=Path(raw.get("rules_path", "/app/rules.yaml")),
        health_host=str(health.get("host", "127.0.0.1")),
        health_port=int(health.get("port", 8080)),
        admin_chat_id=str(raw.get("admin_chat_id", "<TELEGRAM_ADMIN_CHAT_ID>")),
        queue_size=int(raw.get("queue_size", 10000)),
        tg_token=os.environ.get("TG_BOT_TOKEN", ""),
        bridge_token=os.environ.get("BRIDGE_TOKEN", ""),
    )
    if not config.bridge_url.startswith(("http://", "https://")):
        raise ValueError("bridge.url must be http(s)")
    positive = {
        "bridge.reconnect_max_backoff_s": config.reconnect_max_backoff_s,
        "telegram.rate_limit_global_per_s": config.telegram_global_per_s,
        "telegram.rate_limit_per_chat_per_min": config.telegram_chat_per_min,
        "telegram.media_max_bytes": config.media_max_bytes,
        "telegram.media_download_timeout_s": config.media_timeout_s,
        "telegram.media_max_attachments": config.media_max_attachments,
        "telegram.media_max_total_bytes": config.media_max_total_bytes,
        "prepared_queue_size": config.prepared_queue_size,
        "queue_size": config.queue_size,
    }
    if any(value <= 0 for value in positive.values()):
        raise ValueError("numeric config values must be positive")
    if config.telegram_global_per_s > 30 or config.telegram_chat_per_min > 20:
        raise ValueError("telegram limits exceed safe ranges")
    if config.media_max_bytes > 50 * 1024 * 1024:
        raise ValueError("media limit exceeds Bot API upload limit")
    if config.media_max_attachments > 100 or config.media_max_total_bytes > 100 * 1024 * 1024 or config.prepared_queue_size > 100:
        raise ValueError("pipeline limits exceed safe ranges")
    if not 1 <= config.health_port <= 65535:
        raise ValueError("health.port out of range")
    if config.queue_size > 1_000_000:
        raise ValueError("queue_size out of range")
    return config
