from __future__ import annotations

import sys
from importlib import import_module
from typing import Protocol


class _Logger(Protocol):
    def remove(self) -> None: ...
    def add(self, sink: object, **kwargs: object) -> int: ...
    def info(self, message: str) -> None: ...
    def error(self, message: str) -> None: ...
    def level(self, name: str, **kwargs: object) -> _Level: ...


class _Level(Protocol):
    icon: str


logger: _Logger = import_module("loguru").logger


def log_error(message: str) -> None:
    logger.error(message)


class LoggerManager:
    @staticmethod
    def configure() -> None:
        logger.remove()
        logger.level("DEBUG", color="<blue>", icon="*️⃣DDDEBUG")
        logger.level("INFO", color="<white>", icon="ℹ️IIIINFO")
        logger.level("SUCCESS", color="<green>", icon="✅SUCCESS")
        logger.level("WARNING", color="<yellow>", icon="⚠️WARNING")
        logger.level("ERROR", color="<red>", icon="⭕EEERROR")
        logger.add(sys.stdout, level="INFO", colorize=False, backtrace=False, diagnose=False, format="{time:MM-DD HH:mm:ss} [{level.icon}] {message}")


def event_meta(cursor: str, event: dict[str, object]) -> str:
    message = event.get("message")
    message = message if isinstance(message, dict) else {}
    return "guild={} channel={} type={} cursor={}".format(
        message.get("guild_id") or message.get("guildId") or "dm",
        message.get("channel_id") or message.get("channelId") or "unknown",
        event.get("event_type") or "unknown",
        cursor[:8],
    )
