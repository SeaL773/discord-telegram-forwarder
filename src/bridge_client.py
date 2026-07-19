from __future__ import annotations

import asyncio
import json
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from typing import Any

import httpx
import websockets

from .models import Envelope
from . import LoggerManager


class CursorExpired(Exception):
    def __init__(self, ready_cursor: str | None) -> None:
        super().__init__("bridge cursor expired")
        self.ready_cursor = ready_cursor


class SessionLost(Exception):
    pass


class BridgeClient:
    def __init__(self, base_url: str, token: str, client: httpx.AsyncClient, buffer_size: int = 10000, connector: Any = websockets.connect) -> None:
        self.base_url = base_url.rstrip("/")
        self.ws_url = self.base_url.replace("http://", "ws://").replace("https://", "wss://") + "/v1/events"
        self.client = client
        self.headers = {"Authorization": f"Bearer {token}"}
        self.buffer_size = buffer_size
        self.connector = connector
        self.connected = False
        self.last_connected_at: float | None = None

    @staticmethod
    def _parse_ready(raw: str | bytes) -> str | None:
        try:
            frame = json.loads(raw)
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
            raise SessionLost("invalid bridge ready frame") from exc
        if not isinstance(frame, dict) or frame.get("type") != "ready":
            raise SessionLost("bridge did not send ready")
        cursor = frame.get("latest_cursor")
        if cursor is not None and (not isinstance(cursor, str) or not cursor):
            raise SessionLost("invalid ready boundary")
        return cursor

    async def _reader(self, ws: Any, queue: asyncio.Queue[dict[str, Any] | BaseException | None]) -> None:
        try:
            async for raw in ws:
                try:
                    frame = json.loads(raw)
                    if not isinstance(frame, dict) or frame.get("type") != "event":
                        raise ValueError("invalid websocket frame")
                    Envelope.from_frame(frame)
                    await queue.put(frame)
                except (json.JSONDecodeError, ValueError) as exc:
                    LoggerManager.log_error(f"bridge frame rejected error={type(exc).__name__}")
                    await queue.put(SessionLost("invalid websocket event frame"))
                    return
        except websockets.ConnectionClosed:
            return
        finally:
            self.connected = False
            await queue.put(None)

    async def rest_pages(self, after: str | None, *, snapshot: bool = False) -> AsyncIterator[Envelope]:
        cursor = after
        while True:
            params: dict[str, Any] = {"limit": 500}
            if cursor is not None:
                params["after"] = cursor
            response = await self.client.get(f"{self.base_url}/v1/events", headers=self.headers, params=params, timeout=30)
            if response.status_code == 409:
                body = response.json()
                raise CursorExpired(body.get("buffer_latest_cursor"))
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict) or not isinstance(body.get("events"), list) or not isinstance(body.get("has_more"), bool):
                raise SessionLost("invalid REST replay page")
            for frame in body["events"]:
                try:
                    if not isinstance(frame, dict):
                        raise ValueError("invalid REST frame")
                    yield Envelope.from_frame(frame)
                except ValueError as exc:
                    LoggerManager.log_error(f"bridge frame rejected error={type(exc).__name__}")
                    raise SessionLost("invalid REST event frame") from exc
            if snapshot or not body.get("has_more"):
                return
            next_cursor = body.get("next_cursor")
            if not isinstance(next_cursor, str) or next_cursor == cursor:
                raise RuntimeError("invalid replay pagination")
            cursor = next_cursor

    async def bootstrap(self, ready_cursor: str | None, snapshot: Any) -> list[dict[str, Any]]:
        events = [event async for event in self.rest_pages(None, snapshot=True)]
        if ready_cursor is not None:
            boundary = next((index for index, item in enumerate(events) if item.cursor == ready_cursor), None)
            if boundary is None:
                raise SessionLost("bootstrap snapshot missed ready boundary")
            events = events[: boundary + 1]
        keep: set[str] = set()
        by_channel: dict[str, list[Envelope]] = {}
        for envelope in events:
            if snapshot.route(envelope.event):
                message = envelope.event.get("message", {})
                channel = str(message.get("channel_id") or message.get("channelId") or "") if isinstance(message, dict) else ""
                by_channel.setdefault(channel, []).append(envelope)
        for values in by_channel.values():
            keep.update(item.cursor for item in values[-10:])
        return [{"cursor": item.cursor, "event": item.event, "targets": [{"chat_id": target.chat_id, "thread_id": target.thread_id} for target in snapshot.route(item.event)] if item.cursor in keep else []} for item in events]

    async def capture_bootstrap(self, snapshot: Any) -> tuple[str | None, list[dict[str, Any]]]:
        queue: asyncio.Queue[dict[str, Any] | BaseException | None] = asyncio.Queue(self.buffer_size)
        async with self.connector(self.ws_url, additional_headers=self.headers, ping_timeout=45, max_queue=None, proxy=None) as ws:
            ready_cursor = self._parse_ready(await ws.recv())
            if ready_cursor is None:
                return None, []
            self.connected = True
            reader = asyncio.create_task(self._reader(ws, queue))
            try:
                plan = await self.bootstrap(ready_cursor, snapshot)
                if reader.done():
                    raise SessionLost("websocket died during bootstrap snapshot")
                return ready_cursor, plan
            finally:
                self.connected = False
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)

    async def session(self, ack: str | None, _snapshot: Any, on_gap: Callable[[str | None], Awaitable[None]]) -> AsyncGenerator[Envelope, None]:
        queue: asyncio.Queue[dict[str, Any] | BaseException | None] = asyncio.Queue(self.buffer_size)
        async with self.connector(self.ws_url, additional_headers=self.headers, ping_timeout=45, max_queue=None, proxy=None) as ws:
            ready_cursor = self._parse_ready(await ws.recv())
            self.connected = True
            reader = asyncio.create_task(self._reader(ws, queue))
            replay: list[Envelope] = []
            try:
                try:
                    if ack == ready_cursor:
                        replay = []
                    elif ack is None:
                        raise SessionLost("bootstrap boundary appeared; restart bootstrap")
                    else:
                        replay = [item async for item in self.rest_pages(ack)]
                        if ready_cursor is not None and all(item.cursor != ready_cursor for item in replay):
                            raise SessionLost("REST replay missed ready boundary")
                except CursorExpired:
                    await on_gap(ready_cursor)
                    replay = []
                if reader.done():
                    raise SessionLost("websocket died during replay")
                seen: set[str] = set()
                seen_order: deque[str] = deque()
                for envelope in replay:
                    if envelope.cursor not in seen:
                        seen.add(envelope.cursor)
                        seen_order.append(envelope.cursor)
                        yield envelope
                    if ready_cursor is not None and envelope.cursor == ready_cursor:
                        break
                while True:
                    frame = await queue.get()
                    if frame is None:
                        raise SessionLost("websocket disconnected")
                    if isinstance(frame, BaseException):
                        raise frame
                    envelope = Envelope.from_frame(frame)
                    if envelope.cursor in seen:
                        continue
                    seen.add(envelope.cursor)
                    seen_order.append(envelope.cursor)
                    while len(seen_order) > self.buffer_size:
                        seen.discard(seen_order.popleft())
                    yield envelope
            finally:
                self.connected = False
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
