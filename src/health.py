from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from typing import Any


class HealthMonitor:
    def __init__(self, connected: Callable[[], bool], cursor: Callable[[], str | None], queue_depth: Callable[[], int], in_flight: Callable[[], bool], alert: Callable[[str], Awaitable[bool]], clock: Any = time.monotonic) -> None:
        self.connected = connected
        self.cursor = cursor
        self.queue_depth = queue_depth
        self.in_flight = in_flight
        self.alert = alert
        self.clock = clock
        self.disconnected_since: float | None = None
        self.alerted = False
        self.last_event_at: float | None = None
        self.fatal = False

    def event_received(self) -> None:
        self.last_event_at = self.clock()

    def worker_failed(self) -> None:
        self.fatal = True

    def snapshot(self) -> tuple[int, dict[str, Any]]:
        now = self.clock()
        if self.fatal:
            status, code = "unhealthy", 503
        elif self.connected():
            self.disconnected_since = None
            self.alerted = False
            status, code = "ok", 200
        else:
            if self.disconnected_since is None:
                self.disconnected_since = now
            duration = now - self.disconnected_since
            status, code = ("degraded", 200) if duration < 300 else ("unhealthy", 503)
        cursor = self.cursor()
        disconnected_for = 0.0 if self.connected() else now - (self.disconnected_since or now)
        last_event_age = None if self.last_event_at is None else max(0.0, now - self.last_event_at)
        return code, {"status": status, "cursor": cursor[:8] if cursor else None, "queue_depth": self.queue_depth(), "in_flight": self.in_flight(), "disconnect_seconds": round(disconnected_for, 3), "last_event_age_seconds": None if last_event_age is None else round(last_event_age, 3)}

    async def maybe_alert(self) -> None:
        code, _ = self.snapshot()
        if code == 503 and not self.alerted:
            if await self.alert("Discord Bridge disconnected for at least 5 minutes"):
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
