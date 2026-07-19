from __future__ import annotations

import asyncio
import random
import signal
from typing import Any

import httpx
import websockets

from .LoggerManager import LoggerManager, event_meta, logger
from .bridge_client import BridgeClient, SessionLost
from .config import load_config
from .formatter import format_event
from .health import HealthMonitor
from .media import MediaHandler, extract_attachments
from .models import Envelope, PreparedEvent, Target, WorkItem
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

    async def await_terminal(self, queue: asyncio.Queue[Any], stop: asyncio.Event) -> bool:
        await queue.join()
        return not stop.is_set()


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


async def run() -> None:
    LoggerManager.configure()
    config = load_config()
    if not config.tg_token or not config.bridge_token:
        raise RuntimeError("TG_BOT_TOKEN and BRIDGE_TOKEN are required")
    state = StateStore(config.state_path, config.dead_letter_path)
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
        sender = TgSender(config.tg_token, telegram_http, state, config.telegram_global_per_s, config.telegram_chat_per_min)
        work_queue: asyncio.Queue[WorkItem] = asyncio.Queue(config.queue_size)
        prepared_queue: asyncio.Queue[PreparedEvent] = asyncio.Queue(config.prepared_queue_size)
        pending = PendingCursors()
        health = HealthMonitor(lambda: bridge.connected, lambda: state.ack, work_queue.qsize, lambda: state.in_flight is not None, lambda text: sender.send_alert(config.admin_chat_id, text))
        server = await health.serve(config.health_host, config.health_port)

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
                    await enqueue(WorkItem(Envelope(item["cursor"], item["event"]), frozen_targets(item["targets"])))

        async def gap(ready: str | None) -> None:
            await sender.send_alert(config.admin_chat_id, "Bridge replay gap detected; skipped to current boundary. Metadata only.")
            await state.gap_to(ready)

        async def consume() -> None:
            await enqueue_recovery()
            backoff = 1.0
            while not stop.is_set():
                try:
                    if not await pending.await_terminal(work_queue, stop):
                        return
                    if state.ack is None:
                        ready, plan = await bridge.capture_bootstrap(router.snapshot)
                        if ready is not None:
                            await state.save_bootstrap(ready, plan)
                            for item in plan:
                                await enqueue(WorkItem(Envelope(item["cursor"], item["event"]), frozen_targets(item["targets"])))
                            continue
                    async for envelope in bridge.session(state.ack, router.snapshot, gap):
                        health.event_received()
                        await enqueue(WorkItem(envelope))
                    backoff = 1.0
                except (OSError, httpx.HTTPError, websockets.WebSocketException, SessionLost):
                    await asyncio.sleep(min(backoff, config.reconnect_max_backoff_s) + random.random())
                    backoff = min(backoff * 2, config.reconnect_max_backoff_s)

        async def prepare() -> None:
            while not stop.is_set():
                work = await work_queue.get()
                try:
                    targets = list(work.frozen_targets) if work.frozen_targets is not None else router.route(work.envelope.event)
                    formatted = format_event(work.envelope.event)
                    attachments = extract_attachments(work.envelope.event)
                    all_urls = [item.url for item in attachments]
                    inflight = state.in_flight
                    fallback_only = inflight is not None and inflight.get("cursor") == work.envelope.cursor and any(item.get("phase") == "fallback" for item in inflight["targets"])
                    downloaded, failed = ([], all_urls) if fallback_only or not targets else await media.download_all(work.envelope.event)
                    await prepared_queue.put(PreparedEvent(work.envelope, targets, formatted, downloaded, failed, all_urls))
                except BaseException:
                    work_queue.task_done()
                    pending.terminal(work.envelope.cursor)
                    raise

        async def send_commit() -> None:
            while not stop.is_set():
                prepared = await prepared_queue.get()
                try:
                    if not prepared.targets:
                        await state.finish(prepared.envelope.cursor, "dropped")
                    else:
                        await sender.send_event(prepared.envelope, prepared.targets, prepared.formatted, prepared.media, prepared.fallback_urls, prepared.attachment_urls)
                    logger.info(event_meta(prepared.envelope.cursor, prepared.envelope.event))
                finally:
                    pending.terminal(prepared.envelope.cursor)
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
