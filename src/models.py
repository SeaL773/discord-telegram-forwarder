from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class Target:
    chat_id: str
    thread_id: int | None = None

    @property
    def key(self) -> str:
        return f"{self.chat_id}:{self.thread_id or ''}"


@dataclass(frozen=True, slots=True)
class Envelope:
    cursor: str
    event: dict[str, Any]
    force_drop: bool = False

    @classmethod
    def from_frame(cls, frame: dict[str, Any]) -> "Envelope":
        if frame.get("type") != "event" or not isinstance(frame.get("cursor"), str):
            raise ValueError("invalid event frame")
        event = frame.get("event")
        if not isinstance(event, dict):
            raise ValueError("invalid event body")
        message = event.get("message")
        if (
            event.get("schema_version") != 1
            or event.get("event_type") not in {"CREATED", "EDITED", "DELETED", "GHOST_PINGED"}
            or not isinstance(event.get("captured_at"), str)
            or not event["captured_at"]
            or not isinstance(message, dict)
            or not isinstance(message.get("id"), str)
            or not message["id"]
            or not isinstance(message.get("channel_id"), str)
            or not message["channel_id"]
        ):
            raise ValueError("invalid event schema")
        return cls(frame["cursor"], event)


@dataclass(frozen=True, slots=True)
class Attachment:
    url: str
    filename: str
    content_type: str | None = None
    declared_size: int | None = None


@dataclass(frozen=True, slots=True)
class DownloadedMedia:
    attachment: Attachment
    data: bytes
    content_type: str
    kind: str


@dataclass(frozen=True, slots=True)
class FormattedMessage:
    text: str
    caption: str


@dataclass(slots=True)
class PreparedEvent:
    envelope: Envelope
    targets: list[Target]
    formatted: FormattedMessage
    media: list[DownloadedMedia] = field(default_factory=list)
    fallback_urls: list[str] = field(default_factory=list)
    attachment_urls: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class WorkItem:
    envelope: Envelope
    frozen_targets: tuple[Target, ...] | None = None
