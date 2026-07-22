from __future__ import annotations

import asyncio
import random
import signal
from typing import Any, Protocol

import httpx
import websockets

from .LoggerManager import LoggerManager, event_meta, log_error, logger
from .bridge_client import BridgeClient, SessionLost
from .config import load_config
from .formatter import format_event
from .health import HealthMonitor
from .media import MediaHandler, extract_attachments
from .models import Attachment, DownloadedMedia, Envelope, EventPreparationError, FormattedMessage, PreparedEvent, RejectedEvent, Target, WorkItem
from .router import Router
from .state import StateStore
from .tg_sender import TgSender


class PendingCursors:
    def __init__(self) -> None:
        self._values: set[str] = set()

    def add(self, cursor: str) -> bool:
        if cursor in self._values:
            return False
        self._values.add(cursor)
        return True

    def terminal(self, cursor: str) -> None:
        self._values.discard(cursor)

    @property
    def count(self) -> int:
        return len(self._values)

    async def await_terminal(self, queue: asyncio.Queue[Any], stop: asyncio.Event) -> bool:
        await queue.join()
        return not stop.is_set()


class ReconnectBackoff:
    def __init__(self, maximum: float) -> None:
        self.maximum = maximum
        self.delay = 1.0

    def ready(self) -> None:
        self.delay = 1.0

    def failed(self) -> float:
        current = min(self.delay, self.maximum)
        self.delay = min(self.delay * 2, self.maximum)
        return current


class EventRouter(Protocol):
    def route(self, event: dict[str, Any]) -> list[Target]: ...


class EventMedia(Protocol):
    async def download_all(self, event: dict[str, Any], attachments: list[Attachment] | None = None) -> tuple[list[DownloadedMedia], list[str]]: ...


class AlertSender(Protocol):
    async def send_alert(self, chat_id: str, text: str) -> bool: ...


async def wait_for_shutdown(stop: asyncio.Event, tasks: list[asyncio.Task[None]], health: HealthMonitor) -> BaseException | None:
    stop_task = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait([stop_task, *tasks], return_when=asyncio.FIRST_COMPLETED)
        if stop_task in done:
            return None
        completed = next(task for task in done if task is not stop_task)
        health.worker_failed()
        if completed.cancelled():
            return RuntimeError("worker cancelled unexpectedly")
        return completed.exception() or RuntimeError("worker exited unexpectedly")
    finally:
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)


def frozen_targets(raw: list[dict[str, Any]]) -> tuple[Target, ...]:
    return tuple(Target(str(item["chat_id"]), item.get("thread_id")) for item in raw)


def bootstrap_work_item(item: dict[str, Any]) -> WorkItem:
    cursor = item["cursor"]
    if item.get("action") == "drop":
        return WorkItem(Envelope(cursor, {}), ())
    event = item["event"]
    targets = frozen_targets(item["targets"])
    reason = item.get("rejection_reason")
    envelope = RejectedEvent(cursor, event, reason) if isinstance(reason, str) else Envelope(cursor, event)
    return WorkItem(envelope, targets)


def pending_targets_are_fallback_only(inflight: dict[str, Any] | None, cursor: str) -> bool:
    if inflight is None or inflight.get("cursor") != cursor:
        return False
    pending_targets = [item for item in inflight.get("targets", []) if item.get("status") == "pending"]
    return bool(pending_targets) and all(item.get("phase", "media") == "fallback" for item in pending_targets)


async def prepare_work(work: WorkItem, router: EventRouter, media: EventMedia, inflight: dict[str, Any] | None) -> PreparedEvent | RejectedEvent:
    envelope = work.envelope
    if isinstance(envelope, RejectedEvent):
        return envelope
    try:
        targets = list(work.frozen_targets) if work.frozen_targets is not None else router.route(envelope.event)
        if not targets:
            return PreparedEvent(envelope, targets, FormattedMessage("", ""))
        attachments = extract_attachments(envelope.event)
        formatted = format_event(envelope.event, len(attachments))
    except EventPreparationError as exc:
        return RejectedEvent(envelope.cursor, envelope.event, str(exc))
    all_urls = [item.url for item in attachments]
    fallback_only = pending_targets_are_fallback_only(inflight, envelope.cursor)
    downloaded, failed = ([], all_urls) if fallback_only else await media.download_all(envelope.event, attachments)
    return PreparedEvent(envelope, targets, formatted, downloaded, failed, all_urls)


async def persist_gap_and_alert(state: StateStore, sender: AlertSender, admin_chat_id: str, ready: str | None, alert_timeout_s: float = 30) -> None:
    await state.gap_to(ready)
    try:
        await asyncio.wait_for(sender.send_alert(admin_chat_id, "Bridge replay gap detected; skipped to current boundary. Metadata only."), timeout=alert_timeout_s)
    except TimeoutError:
        log_error("bridge gap alert timed out")
    except Exception as exc:
        log_error(f"bridge gap alert failed error={type(exc).__name__}")


async def run() -> None:
    LoggerManager.configure()
    config = load_config()
    if not config.tg_token or not config.bridge_token:
        raise RuntimeError("TG_BOT_TOKEN and BRIDGE_TOKEN are required")
    state = StateStore(
        config.state_path,
        config.dead_letter_path,
        dead_letter_max_bytes=config.dead_letter_max_bytes,
        dead_letter_backup_count=config.dead_letter_backup_count,
    )
    router = Router(config.rules_path)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    async with (
        httpx.AsyncClient(trust_env=False, follow_redirects=False) as bridge_http,
        httpx.AsyncClient(trust_env=False, follow_redirects=False) as telegram_http,
        httpx.AsyncClient(trust_env=False, follow_redirects=False) as media_http,
    ):
        bridge = BridgeClient(config.bridge_url, config.bridge_token, bridge_http, config.queue_size)
        media = MediaHandler(media_http, config.media_max_bytes, config.media_timeout_s, config.media_max_attachments, config.media_max_total_bytes)
        sender = TgSender(config.tg_token, telegram_http, state, config.telegram_global_per_s, config.telegram_chat_per_min, rich_messages_enabled=config.rich_messages_enabled)
        work_queue: asyncio.Queue[WorkItem] = asyncio.Queue(config.queue_size)
        prepared_queue: asyncio.Queue[PreparedEvent | RejectedEvent] = asyncio.Queue(config.prepared_queue_size)
        pending = PendingCursors()
        health = HealthMonitor(lambda: bridge.connected, lambda: state.ack, lambda: work_queue.qsize() + prepared_queue.qsize(), lambda: state.in_flight is not None, lambda text: sender.send_alert(config.admin_chat_id, text), outstanding=lambda: pending.count > 0 or state.in_flight is not None)
        server = await health.serve(config.health_host, config.health_port)
        await sender.sync_forum_topics(router.snapshot.topic_states())

        async def enqueue(item: WorkItem) -> None:
            if pending.add(item.envelope.cursor):
                await work_queue.put(item)

        async def enqueue_recovery() -> None:
            inflight = state.in_flight
            if inflight is not None:
                await enqueue(WorkItem(Envelope(str(inflight["cursor"]), inflight["event"]), frozen_targets(inflight["targets"])))
            bootstrap = state.bootstrap
            if bootstrap is not None:
                for item in bootstrap["items"][bootstrap["next_index"] :]:
                    await enqueue(bootstrap_work_item(item))

        async def gap(ready: str | None) -> None:
            await persist_gap_and_alert(state, sender, config.admin_chat_id, ready)

        async def consume() -> None:
            await enqueue_recovery()
            backoff = ReconnectBackoff(config.reconnect_max_backoff_s)
            while not stop.is_set():
                try:
                    if not await pending.await_terminal(work_queue, stop):
                        return
                    if state.ack is None:
                        ready, plan = await bridge.capture_bootstrap(router.snapshot, backoff.ready)
                        if ready is not None:
                            await state.save_bootstrap(ready, plan)
                            for item in plan:
                                await enqueue(bootstrap_work_item(item))
                            continue
                    async for envelope in bridge.session(state.ack, router.snapshot, gap, backoff.ready):
                        health.event_received()
                        await enqueue(WorkItem(envelope))
                except (OSError, httpx.HTTPError, websockets.WebSocketException, SessionLost):
                    await asyncio.sleep(backoff.failed() + random.random())

        async def prepare() -> None:
            while not stop.is_set():
                work = await work_queue.get()
                try:
                    await prepared_queue.put(await prepare_work(work, router, media, state.in_flight))
                except BaseException:
                    work_queue.task_done()
                    pending.terminal(work.envelope.cursor)
                    raise

        async def send_commit() -> None:
            while not stop.is_set():
                prepared = await prepared_queue.get()
                try:
                    if isinstance(prepared, RejectedEvent):
                        await state.reject_event(prepared.cursor, prepared.event, prepared.reason)
                        logger.info(event_meta(prepared.cursor, prepared.event))
                    elif not prepared.targets:
                        await state.finish(prepared.envelope.cursor, "dropped")
                    else:
                        await sender.send_event(prepared.envelope, prepared.targets, prepared.formatted, prepared.media, prepared.fallback_urls, prepared.attachment_urls)
                    if isinstance(prepared, PreparedEvent):
                        logger.info(event_meta(prepared.envelope.cursor, prepared.envelope.event))
                finally:
                    cursor = prepared.cursor if isinstance(prepared, RejectedEvent) else prepared.envelope.cursor
                    pending.terminal(cursor)
                    prepared_queue.task_done()
                    work_queue.task_done()

        async def alerts() -> None:
            while not stop.is_set():
                await health.maybe_alert()
                await asyncio.sleep(15)

        tasks = [asyncio.create_task(consume()), asyncio.create_task(prepare()), asyncio.create_task(send_commit()), asyncio.create_task(alerts()), asyncio.create_task(router.watch(stop))]
        failure = await wait_for_shutdown(stop, tasks, health)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        server.close()
        await server.wait_closed()
        if failure is not None:
            raise failure


if __name__ == "__main__":
    asyncio.run(run())
