from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from typing import Any


class HealthMonitor:
    def __init__(self, connected: Callable[[], bool], cursor: Callable[[], str | None], queue_depth: Callable[[], int], in_flight: Callable[[], bool], alert: Callable[[str], Awaitable[bool]], clock: Any = time.monotonic, outstanding: Callable[[], bool] | None = None) -> None:
        self.connected = connected
        self.cursor = cursor
        self.queue_depth = queue_depth
        self.in_flight = in_flight
        self.alert = alert
        self.clock = clock
        self.outstanding = outstanding or (lambda: self.queue_depth() > 0 or self.in_flight())
        self.disconnected_since: float | None = None
        self.alerted = False
        self.last_event_at: float | None = None
        self.fatal = False
        self.last_cursor = self.cursor()
        self.stall_started_at: float | None = None
        self.unhealthy_reason: str | None = None
        self.was_connected = self.connected()

    def event_received(self) -> None:
        self.last_event_at = self.clock()

    def worker_failed(self) -> None:
        self.fatal = True

    def snapshot(self) -> tuple[int, dict[str, Any]]:
        now = self.clock()
        connected = self.connected()
        cursor = self.cursor()
        outstanding = self.outstanding()
        if connected and not self.was_connected:
            self.alerted = False
            self.stall_started_at = now if outstanding else None
        elif cursor != self.last_cursor:
            self.last_cursor = cursor
            self.stall_started_at = now if outstanding else None
        elif not outstanding:
            self.stall_started_at = None
        elif self.stall_started_at is None:
            self.stall_started_at = now
        stall_seconds = 0.0 if self.stall_started_at is None else max(0.0, now - self.stall_started_at)
        if self.fatal:
            self.unhealthy_reason = "worker_failed"
            status, code = "unhealthy", 503
        elif connected and outstanding and stall_seconds >= 300:
            self.unhealthy_reason = "pipeline_stalled"
            status, code = "unhealthy", 503
        elif connected:
            self.disconnected_since = None
            self.alerted = False
            self.unhealthy_reason = None
            status, code = "ok", 200
        else:
            if self.disconnected_since is None:
                self.disconnected_since = now
            duration = now - self.disconnected_since
            status, code = ("degraded", 200) if duration < 300 else ("unhealthy", 503)
            self.unhealthy_reason = None if code == 200 else "bridge_disconnected"
        self.was_connected = connected
        disconnected_for = 0.0 if connected else now - (self.disconnected_since or now)
        last_event_age = None if self.last_event_at is None else max(0.0, now - self.last_event_at)
        return code, {"status": status, "cursor": cursor[:8] if cursor else None, "queue_depth": self.queue_depth(), "in_flight": self.in_flight(), "disconnect_seconds": round(disconnected_for, 3), "last_event_age_seconds": None if last_event_age is None else round(last_event_age, 3), "stall_seconds": round(stall_seconds, 3), "reason": self.unhealthy_reason}

    async def maybe_alert(self) -> None:
        code, body = self.snapshot()
        if code == 503 and not self.alerted:
            reason = body.get("reason")
            if reason == "worker_failed":
                text = "Forwarding worker failed unexpectedly; the forwarding pipeline stopped"
            elif reason == "pipeline_stalled":
                text = "Forwarding pipeline stalled with outstanding work and no durable cursor progress for at least 5 minutes"
            else:
                text = "Discord Bridge disconnected for at least 5 minutes"
            if await self.alert(text):
                self.alerted = True

    async def serve(self, host: str, port: int) -> asyncio.AbstractServer:
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=2)
                code, body = self.snapshot()
                if not line.startswith(b"GET /healthz "):
                    code, body = 404, {"status": "not_found"}
                payload = json.dumps(body, separators=(",", ":")).encode()
                reason = "OK" if code == 200 else "Service Unavailable" if code == 503 else "Not Found"
                writer.write(f"HTTP/1.1 {code} {reason}\r\nContent-Type: application/json\r\nContent-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode() + payload)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        return await asyncio.start_server(handle, host, port)
