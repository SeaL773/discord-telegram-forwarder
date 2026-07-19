from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any

import httpx

from .formatter import add_fallbacks
from .models import DownloadedMedia, Envelope, FormattedMessage, Target
from .state import StateStore


class TokenBucket:
    def __init__(self, capacity: float, refill_per_second: float, clock: Any = time.monotonic) -> None:
        self.capacity = capacity
        self.tokens = capacity
        self.refill_per_second = refill_per_second
        self.updated = clock()
        self.clock = clock

    def refresh(self) -> None:
        now = self.clock()
        self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.refill_per_second)
        self.updated = now

    def wait_for(self, cost: int) -> float:
        self.refresh()
        return max(0.0, (cost - self.tokens) / self.refill_per_second)

    def consume(self, cost: int) -> None:
        if cost > self.tokens:
            raise RuntimeError("token bucket underflow")
        self.tokens -= cost


class DualLimiter:
    def __init__(self, global_bucket: TokenBucket, chat_refill_per_minute: float, sleep: Any = asyncio.sleep, chat_burst_capacity: float = 10) -> None:
        self.global_bucket = global_bucket
        self.chat_refill_per_minute = chat_refill_per_minute
        self.chat_burst_capacity = chat_burst_capacity
        self.chat_buckets: dict[str, TokenBucket] = {}
        self.sleep = sleep
        self.lock = asyncio.Lock()

    def chat_bucket(self, chat_id: str) -> TokenBucket:
        if chat_id not in self.chat_buckets:
            self.chat_buckets[chat_id] = TokenBucket(self.chat_burst_capacity, self.chat_refill_per_minute / 60, self.global_bucket.clock)
        return self.chat_buckets[chat_id]

    async def acquire(self, chat_id: str, cost: int) -> None:
        if cost < 1 or cost > self.global_bucket.capacity or cost > self.chat_burst_capacity:
            raise ValueError("rate cost exceeds bucket capacity")
        async with self.lock:
            chat = self.chat_bucket(chat_id)
            while True:
                wait = max(self.global_bucket.wait_for(cost), chat.wait_for(cost))
                if wait <= 0:
                    self.global_bucket.consume(cost)
                    chat.consume(cost)
                    return
                await self.sleep(wait)


@dataclass(frozen=True, slots=True)
class ApiResult:
    ok: bool
    retry_after: float | None = None
    retryable: bool = False
    reason: str = ""


@dataclass(frozen=True, slots=True)
class RequestBatch:
    method: str
    data: dict[str, Any]
    files: dict[str, tuple[str, bytes, str]] | None
    cost: int


class TgSender:
    def __init__(self, token: str, client: httpx.AsyncClient, state: StateStore, global_per_s: float = 25, chat_per_min: float = 18, sleep: Any = asyncio.sleep) -> None:
        self.client = client
        self.state = state
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.limiter = DualLimiter(TokenBucket(max(10, global_per_s), global_per_s), chat_per_min, sleep)
        self.sleep = sleep
        self._attempt_lock = asyncio.Lock()

    async def send_event(self, envelope: Envelope, targets: list[Target], formatted: FormattedMessage, media: list[DownloadedMedia], fallback_urls: list[str], attachment_urls: list[str] | None = None) -> None:
        if self.state.in_flight is None:
            await self.state.begin(envelope, targets)
        inflight = self.state.in_flight
        if inflight is None or inflight.get("cursor") != envelope.cursor:
            raise RuntimeError("in-flight cursor mismatch")
        records = inflight.get("targets", [])
        normal_formatted = add_fallbacks(formatted, fallback_urls)
        all_urls = attachment_urls if attachment_urls is not None else fallback_urls
        fallback_formatted = add_fallbacks(formatted, all_urls)
        for index, target in enumerate(targets):
            if index < len(records) and records[index].get("status") != "pending":
                continue
            await self._send_target(index, envelope, target, normal_formatted, fallback_formatted, media)
        await self.state.finish(envelope.cursor, "forwarded")

    async def send_alert(self, chat_id: str, text: str) -> bool:
        target = Target(chat_id)
        batch = RequestBatch("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, None, 1)
        failures = 0
        while True:
            result = await self._attempt(batch, target)
            if result.ok:
                return True
            if result.retry_after is not None:
                await self.sleep(result.retry_after)
                continue
            if not result.retryable:
                return False
            failures += 1
            if failures >= 3:
                return False
            await self.sleep(2 ** (failures - 1))

    async def _send_target(self, index: int, envelope: Envelope, target: Target, formatted: FormattedMessage, fallback_formatted: FormattedMessage, media: list[DownloadedMedia]) -> None:
        target_state = self.state.in_flight["targets"][index] if self.state.in_flight else {}
        failures = int(target_state.get("retries", 0))
        if target_state.get("phase") == "fallback":
            await self._send_fallback(index, envelope, target, fallback_formatted)
            return
        batches = self._requests(target, formatted, media)
        media_delivery = bool(media)
        for batch in batches:
            while True:
                result = await self._attempt(batch, target)
                if result.ok:
                    break
                if result.retry_after is not None:
                    await self.sleep(result.retry_after)
                    continue
                if result.retryable:
                    failures += 1
                    await self.state.retry(index)
                    if failures < 3:
                        await self.sleep(2 ** (failures - 1))
                        continue
                if media_delivery:
                    await self.state.set_fallback(index)
                    await self._send_fallback(index, envelope, target, fallback_formatted)
                else:
                    await self.state.dead_letter({"cursor": envelope.cursor, "event": envelope.event, "target": {"chat_id": target.chat_id, "thread_id": target.thread_id}, "reason": result.reason})
                    await self.state.terminal(index, "dead_lettered")
                return
        await self.state.terminal(index, "sent")

    async def _send_fallback(self, index: int, envelope: Envelope, target: Target, formatted: FormattedMessage) -> None:
        inflight = self.state.in_flight
        failures = int(inflight["targets"][index].get("fallback_retries", 0)) if inflight else 0
        batch = RequestBatch("sendMessage", {**self._common(target), "text": formatted.text, "parse_mode": "HTML"}, None, 1)
        while True:
            result = await self._attempt(batch, target)
            if result.ok:
                await self.state.terminal(index, "sent")
                return
            if result.retry_after is not None:
                await self.sleep(result.retry_after)
                continue
            if result.retryable:
                failures += 1
                await self.state.retry(index, fallback=True)
                if failures < 3:
                    await self.sleep(2 ** (failures - 1))
                    continue
            await self.state.dead_letter({"cursor": envelope.cursor, "event": envelope.event, "target": {"chat_id": target.chat_id, "thread_id": target.thread_id}, "phase": "fallback", "reason": result.reason})
            await self.state.terminal(index, "dead_lettered")
            return

    def _common(self, target: Target) -> dict[str, Any]:
        common: dict[str, Any] = {"chat_id": target.chat_id}
        if target.thread_id is not None:
            common["message_thread_id"] = str(target.thread_id)
        return common

    def _requests(self, target: Target, formatted: FormattedMessage, media: list[DownloadedMedia]) -> list[RequestBatch]:
        common = self._common(target)
        if not media:
            return [RequestBatch("sendMessage", {**common, "text": formatted.text, "parse_mode": "HTML"}, None, 1)]
        groups: list[list[DownloadedMedia]] = []
        current: list[DownloadedMedia] = []
        current_class = ""
        for item in media:
            item_class = "document" if item.kind == "document" else "visual"
            if current and (item_class != current_class or len(current) == 10):
                groups.append(current)
                current = []
            current_class = item_class
            current.append(item)
        if current:
            groups.append(current)
        result: list[RequestBatch] = []
        caption_available = True
        for group in groups:
            caption = formatted.caption if caption_available else ""
            caption_available = False
            if len(group) == 1:
                item = group[0]
                field = item.kind
                data = {**common, field: "attach://file0"}
                if caption:
                    data.update({"caption": caption, "parse_mode": "HTML"})
                files = {"file0": (item.attachment.filename, item.data, item.content_type)}
                result.append(RequestBatch(f"send{item.kind.title()}", data, files, 1))
                continue
            payload: list[dict[str, Any]] = []
            files = {}
            for index, item in enumerate(group):
                entry: dict[str, Any] = {"type": item.kind, "media": f"attach://file{index}"}
                if index == 0 and caption:
                    entry.update({"caption": caption, "parse_mode": "HTML"})
                payload.append(entry)
                files[f"file{index}"] = (item.attachment.filename, item.data, item.content_type)
            result.append(RequestBatch("sendMediaGroup", {**common, "media": json.dumps(payload)}, files, len(group)))
        return result

    async def _attempt(self, batch: RequestBatch, target: Target) -> ApiResult:
        async with self._attempt_lock:
            await self.limiter.acquire(target.chat_id, batch.cost)
            try:
                response = await self.client.post(f"{self.base_url}/{batch.method}", data=batch.data, files=batch.files, timeout=30)
            except (httpx.TimeoutException, httpx.NetworkError):
                return ApiResult(False, retryable=True, reason="network")
            try:
                body = response.json()
            except ValueError:
                body = None
            status = response.status_code
            error_code = body.get("error_code") if isinstance(body, dict) else None
            code = int(error_code) if isinstance(error_code, (int, float)) else status
            if status == 429 or code == 429:
                try:
                    if not isinstance(body, dict):
                        raise TypeError("missing Telegram error body")
                    retry_after = float(body["parameters"]["retry_after"])
                except (ValueError, KeyError, TypeError):
                    return ApiResult(False, retryable=True, reason="malformed_429")
                return ApiResult(False, retry_after=retry_after, reason="rate_limited")
            if status >= 500 or code >= 500:
                return ApiResult(False, retryable=True, reason=f"http_{code}")
            if status >= 400 or 400 <= code < 500:
                return ApiResult(False, reason=f"http_{code}")
            if not isinstance(body, dict) or body.get("ok") is not True:
                return ApiResult(False, retryable=True, reason="malformed_success")
            return ApiResult(True)
