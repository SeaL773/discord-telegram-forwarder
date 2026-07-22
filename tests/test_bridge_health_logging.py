import asyncio
import json
from importlib import import_module
import httpx
pytest = import_module("pytest")

from src.LoggerManager import event_meta
from src.bridge_client import BridgeClient, CursorExpired, SessionLost, _EventQueue
from src.health import HealthMonitor
from src.models import Envelope, RejectedEvent, WorkItem
from src.main import PendingCursors, ReconnectBackoff, wait_for_shutdown
from src.router import parse_rules


def frame(cursor, channel="c", content="body"):
    return {"type": "event", "cursor": cursor, "event": {"schema_version": 1, "event_type": "CREATED", "captured_at": "2026-01-01T00:00:00Z", "message": {"id": f"m-{cursor}", "channel_id": channel, "content": content}}}


class FakeWs:
    def __init__(self, frames):
        self.frames = list(frames)
        self.ready_sent = False
        self.ready = json.dumps({"type": "ready", "latest_cursor": "r2"})

    async def recv(self):
        self.ready_sent = True
        return self.ready

    def __aiter__(self): return self
    async def __anext__(self):
        if not self.frames:
            await asyncio.Future()
        return json.dumps(self.frames.pop(0))


class FakeConnect:
    def __init__(self, ws): self.ws = ws; self.kwargs = None
    async def __aenter__(self): return self.ws
    async def __aexit__(self, *args): return False

    def __call__(self, *args, **kwargs): self.kwargs = kwargs; return self


@pytest.mark.asyncio
async def test_ws_reader_starts_before_rest_ready_overlap_once():
    ws = FakeWs([frame("r2"), frame("live")])
    replay_continued_past_ready = False
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client, connector=FakeConnect(ws))
        async def pages(after, *, snapshot=False):
            nonlocal replay_continued_past_ready
            assert after == "old" and snapshot is False
            assert ws.ready_sent
            await asyncio.sleep(0)
            yield Envelope("r1", frame("r1")["event"])
            yield Envelope("r2", frame("r2")["event"])
            replay_continued_past_ready = True
            raise AssertionError("REST replay continued past ready boundary")
        bridge.rest_pages = pages
        stream = bridge.session("old", parse_rules({"rules": [], "default_action": "drop"}), lambda cursor: asyncio.sleep(0))
        result = [await anext(stream), await anext(stream), await anext(stream)]
        await stream.aclose()
    assert [x.cursor for x in result] == ["r1", "r2", "live"]
    assert replay_continued_past_ready is False
    assert bridge.connector.kwargs["proxy"] is None


@pytest.mark.asyncio
async def test_bad_ws_event_is_epoch_fatal_and_later_good_event_not_emitted():
    bad = frame("bad")
    bad["event"]["schema_version"] = 2
    ws = FakeWs([bad, frame("good")])
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client, connector=FakeConnect(ws))
        stream = bridge.session("r2", parse_rules({"rules": [], "default_action": "drop"}), lambda cursor: asyncio.sleep(0))
        rejected = await anext(stream)
        assert isinstance(rejected, RejectedEvent) and rejected.cursor == "bad"
        assert (await anext(stream)).cursor == "good"


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", ["{", "[]", json.dumps({"type": "event", "cursor": "", "event": {}}), json.dumps({"type": "event", "cursor": "c", "event": []})])
async def test_invalid_ws_transport_frame_is_epoch_fatal(raw):
    class RawWs(FakeWs):
        async def __anext__(self):
            if not self.frames:
                await asyncio.Future()
            return self.frames.pop(0)
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client, connector=FakeConnect(RawWs([raw])))
        stream = bridge.session("r2", parse_rules({"rules": [], "default_action": "drop"}), lambda cursor: asyncio.sleep(0))
        with pytest.raises(SessionLost):
            await anext(stream)


@pytest.mark.parametrize("payload", [
    "{",
    b"\xff",
    "[]",
    json.dumps({"type": "not-ready", "latest_cursor": None}),
    json.dumps({"type": "ready", "latest_cursor": ""}),
    json.dumps({"type": "ready", "latest_cursor": 7}),
])
def test_malformed_ready_frames_are_session_lost(payload):
    with pytest.raises(SessionLost):
        BridgeClient._parse_ready(payload)


@pytest.mark.asyncio
async def test_null_bootstrap_race_restarts_when_next_ready_is_non_null():
    ws = FakeWs([])
    ws.ready = json.dumps({"type": "ready", "latest_cursor": "first-event"})
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client, connector=FakeConnect(ws))
        stream = bridge.session(None, parse_rules({"rules": [], "default_action": "drop"}), lambda cursor: asyncio.sleep(0))
        with pytest.raises(SessionLost, match="restart bootstrap"):
            await anext(stream)


@pytest.mark.asyncio
async def test_durable_ack_rejects_null_ready_boundary_without_rest():
    ws = FakeWs([])
    ws.ready = json.dumps({"type": "ready", "latest_cursor": None})
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client, connector=FakeConnect(ws))
        async def pages(after, *, snapshot=False):
            assert after != "durable" and snapshot is False, "REST must not run without a ready boundary"
            yield Envelope("never", frame("never")["event"])
        bridge.rest_pages = pages
        stream = bridge.session("durable", parse_rules({"rules": [], "default_action": "drop"}), lambda cursor: asyncio.sleep(0))
        with pytest.raises(SessionLost, match="ready boundary missing"):
            await anext(stream)


@pytest.mark.asyncio
async def test_ack_equals_ready_skips_rest():
    ws = FakeWs([frame("live")])
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client, connector=FakeConnect(ws))
        async def pages(after, *, snapshot=False):
            assert after == "r2" and snapshot is False
            if after == "never":
                yield Envelope("never", frame("never")["event"])
            raise AssertionError("REST must not be called")
        bridge.rest_pages = pages
        stream = bridge.session("r2", parse_rules({"rules": [], "default_action": "drop"}), lambda cursor: asyncio.sleep(0))
        assert (await anext(stream)).cursor == "live"
        await stream.aclose()


@pytest.mark.asyncio
async def test_normal_replay_and_bootstrap_require_ready_boundary():
    ws = FakeWs([])
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client, connector=FakeConnect(ws))
        async def pages(after, *, snapshot=False):
            assert snapshot is (after is None)
            yield Envelope("r1", frame("r1")["event"])
        bridge.rest_pages = pages
        stream = bridge.session("old", parse_rules({"rules": [], "default_action": "drop"}), lambda cursor: asyncio.sleep(0))
        with pytest.raises(SessionLost):
            await anext(stream)
        with pytest.raises(SessionLost):
            await bridge.bootstrap("missing", parse_rules({"rules": [], "default_action": "drop"}))


@pytest.mark.asyncio
async def test_409_gap_discards_current_ws_epoch_after_callback():
    order = []
    ws = FakeWs([frame("live")])
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client, connector=FakeConnect(ws))
        async def pages(after, *, snapshot=False):
            assert after == "expired" and snapshot is False
            if after == "never":
                yield Envelope("never", frame("never")["event"])
            raise CursorExpired("r2")
        async def gap(cursor): order.append(("gap", cursor))
        bridge.rest_pages = pages
        stream = bridge.session("expired", parse_rules({"rules": [], "default_action": "drop"}), gap)
        with pytest.raises(SessionLost, match="restart after replay gap"):
            await anext(stream)
    assert order == [("gap", "r2")]


@pytest.mark.asyncio
async def test_rest_after_pagination_and_409():
    requests = []
    def handler(request):
        requests.append(dict(request.url.params))
        if request.url.params.get("after") == "expired":
            return httpx.Response(409, json={"error": "cursor_expired", "buffer_latest_cursor": "ready"})
        if request.url.params.get("after") == "a":
            return httpx.Response(200, json={"events": [frame("b")], "next_cursor": "b", "has_more": False})
        return httpx.Response(200, json={"events": [frame("a")], "next_cursor": "a", "has_more": True})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        bridge = BridgeClient("http://bridge", "token", client)
        assert [x.cursor async for x in bridge.rest_pages(None)] == ["a", "b"]
        with pytest.raises(CursorExpired):
            [x async for x in bridge.rest_pages("expired")]
    assert requests[0] == {"limit": "500"}
    assert requests[1] == {"limit": "500", "after": "a"}


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [
    httpx.Response(409, text="not-json"),
    httpx.Response(409, json={"buffer_latest_cursor": ""}),
    httpx.Response(200, text="not-json"),
    httpx.Response(200, json={"events": [], "next_cursor": "a", "has_more": True}),
])
async def test_rest_protocol_failures_are_normalized_to_session_lost(response):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: response)) as client:
        bridge = BridgeClient("http://bridge", "token", client)
        with pytest.raises(SessionLost):
            [item async for item in bridge.rest_pages("a")]


@pytest.mark.asyncio
async def test_reader_completion_drains_buffer_then_reports_disconnect():
    class ClosingWs(FakeWs):
        async def __anext__(self):
            if self.frames:
                return json.dumps(self.frames.pop(0))
            raise StopAsyncIteration
    ws = ClosingWs([frame("one"), frame("two")])
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client, buffer_size=2, connector=FakeConnect(ws))
        stream = bridge.session("r2", parse_rules({"rules": [], "default_action": "drop"}), lambda cursor: asyncio.sleep(0))
        assert [(await anext(stream)).cursor, (await anext(stream)).cursor] == ["one", "two"]
        with pytest.raises(SessionLost, match="disconnected"):
            await anext(stream)
    assert bridge.connector.kwargs["max_queue"] == 16


@pytest.mark.asyncio
async def test_full_reader_queue_cancels_without_sentinel_deadlock():
    ws = FakeWs([frame("one"), frame("two")])
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client, buffer_size=1)
        queue = asyncio.Queue(maxsize=1)
        reader = asyncio.create_task(bridge._reader(ws, queue))
        await asyncio.sleep(0)
        reader.cancel()
        await asyncio.wait_for(asyncio.gather(reader, return_exceptions=True), 0.2)


@pytest.mark.asyncio
async def test_next_item_cancellation_does_not_leave_orphaned_queue_getter():
    queue = _EventQueue()
    async def wait_forever() -> None:
        await asyncio.Event().wait()
    reader = asyncio.create_task(wait_forever())
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client)
        pending = asyncio.create_task(bridge._next_item(queue, reader))
        await asyncio.sleep(0)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending

        item = Envelope("cursor", {"message": {}})
        await queue.put(item)
        assert queue.get_nowait() is item

    reader.cancel()
    await asyncio.gather(reader, return_exceptions=True)


@pytest.mark.asyncio
async def test_next_item_cancellation_restores_item_already_taken_by_getter():
    class CancellingQueue(_EventQueue):
        outer_task: asyncio.Task[Envelope | RejectedEvent] | None = None

        async def get(self) -> Envelope | RejectedEvent:
            item = await super().get()
            assert self.outer_task is not None
            self.outer_task.cancel()
            return item

    queue = CancellingQueue()
    async def wait_forever() -> None:
        await asyncio.Event().wait()
    reader = asyncio.create_task(wait_forever())
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client)
        pending = asyncio.create_task(bridge._next_item(queue, reader))
        queue.outer_task = pending
        await asyncio.sleep(0)
        item = Envelope("cursor", {"message": {}})
        await queue.put(item)
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert queue.get_nowait() is item

    reader.cancel()
    await asyncio.gather(reader, return_exceptions=True)


def test_ready_resets_reconnect_backoff_immediately_but_invalid_ready_does_not():
    backoff = ReconnectBackoff(32)
    backoff.failed(); backoff.failed()
    assert backoff.delay == 4
    with pytest.raises(SessionLost):
        BridgeClient._parse_ready("{}")
    assert backoff.delay == 4
    assert BridgeClient._parse_ready(json.dumps({"type": "ready", "latest_cursor": "r"})) == "r"
    backoff.ready()
    assert backoff.delay == 1


@pytest.mark.asyncio
async def test_bootstrap_complete_snapshot_newest_ten_actual_channel_and_drops():
    events = [Envelope(str(i), {"event_type": "CREATED", "message": {"channel_id": "a" if i < 12 else "b", "content": "x"}}) for i in range(15)]
    rules = parse_rules({"rules": [{"match": {"channel_id": "a"}, "forward_to": {"chat_id": "1"}}], "default_action": "drop"})
    route_calls = 0

    class Snapshot:
        def route(self, event):
            nonlocal route_calls
            route_calls += 1
            return rules.route(event)

    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client)
        async def pages(after, *, snapshot=False):
            assert after is None and snapshot is True
            for item in events: yield item
        bridge.rest_pages = pages
        result = await bridge.bootstrap("14", Snapshot())
    assert route_calls == len(events)
    assert [x["cursor"] for x in result] == [str(i) for i in range(15)]
    assert [x["cursor"] for x in result if x.get("targets")] == [str(i) for i in range(2, 12)]
    assert result[:2] == [{"cursor": "0", "action": "drop"}, {"cursor": "1", "action": "drop"}]
    assert result[12:] == [
        {"cursor": "12", "action": "drop"},
        {"cursor": "13", "action": "drop"},
        {"cursor": "14", "action": "drop"},
    ]
    assert all("event" in item and item["targets"] for item in result[2:12])


@pytest.mark.asyncio
async def test_health_threshold_one_alert_per_episode():
    now = [0.0]
    connected = [False]
    alerts = []
    async def alert(text): alerts.append(text); return True
    health = HealthMonitor(lambda: connected[0], lambda: "secretcursor", lambda: 2, lambda: True, alert, clock=lambda: now[0])
    assert health.snapshot()[0] == 200
    now[0] = 301
    assert health.snapshot()[0] == 503
    await health.maybe_alert(); await health.maybe_alert()
    assert len(alerts) == 1
    connected[0] = True
    health.snapshot()
    connected[0] = False; now[0] = 700; health.snapshot(); now[0] = 1001
    await health.maybe_alert()
    assert len(alerts) == 2
    assert health.snapshot()[1]["cursor"] == "secretcu"
    payload = health.snapshot()[1]
    assert payload["disconnect_seconds"] >= 300
    assert "last_event_age_seconds" in payload


@pytest.mark.asyncio
async def test_connected_pipeline_stall_health_uses_durable_cursor_progress_and_outstanding_work():
    now = [0.0]
    cursor = ["a"]
    queue_depth = [0]
    inflight = [False]
    alerts = []
    async def alert(text): alerts.append(text); return True
    health = HealthMonitor(lambda: True, lambda: cursor[0], lambda: queue_depth[0], lambda: inflight[0], alert, clock=lambda: now[0])

    assert health.snapshot() == (200, {"status": "ok", "cursor": "a", "queue_depth": 0, "in_flight": False, "disconnect_seconds": 0.0, "last_event_age_seconds": None, "stall_seconds": 0.0, "reason": None})
    now[0] = 1000
    assert health.snapshot()[0] == 200

    queue_depth[0] = 1
    assert health.snapshot()[0] == 200
    now[0] = 1299.999
    assert health.snapshot()[0] == 200
    now[0] = 1300
    code, payload = health.snapshot()
    assert code == 503 and payload["reason"] == "pipeline_stalled" and payload["stall_seconds"] == 300.0
    await health.maybe_alert(); await health.maybe_alert()
    assert alerts == ["Forwarding pipeline stalled with outstanding work and no durable cursor progress for at least 5 minutes"]

    cursor[0] = "b"
    assert health.snapshot()[0] == 200
    now[0] = 1600
    assert health.snapshot()[0] == 503
    queue_depth[0] = 0
    assert health.snapshot()[0] == 200


def test_privacy_safe_log_metadata_only():
    event = {"event_type": "CREATED", "message": {"guild_id": "g", "channel_id": "c", "content": "PRIVATE", "attachments": [{"url": "https://secret/?token=x"}]}}
    result = event_meta("cursor-secret", event)
    assert result == "guild=g channel=c type=CREATED cursor=cursor-s"
    assert "PRIVATE" not in result and "https" not in result


def test_cross_session_pending_cursor_only_queues_once_and_terminal_releases():
    pending = PendingCursors()
    assert pending.add("in-flight")
    assert not pending.add("in-flight")
    pending.terminal("in-flight")
    assert pending.add("in-flight")


@pytest.mark.asyncio
async def test_worker_failure_marks_health_unhealthy_and_propagates():
    alerts = []
    async def alert(text): alerts.append(text); return True
    health = HealthMonitor(lambda: True, lambda: None, lambda: 0, lambda: False, alert)
    stop = asyncio.Event()
    async def crash(): raise RuntimeError("boom")
    failure = await wait_for_shutdown(stop, [asyncio.create_task(crash())], health)
    assert isinstance(failure, RuntimeError)
    assert health.snapshot()[0] == 503
    await health.maybe_alert()
    assert alerts == ["Forwarding worker failed unexpectedly; the forwarding pipeline stopped"]


@pytest.mark.asyncio
async def test_next_bridge_epoch_waits_for_local_terminal_work():
    queue: asyncio.Queue[WorkItem] = asyncio.Queue()
    stop = asyncio.Event()
    await queue.put(WorkItem(Envelope("pending", frame("pending")["event"])))
    wait = asyncio.create_task(PendingCursors().await_terminal(queue, stop))
    await asyncio.sleep(0)
    assert not wait.done()
    queue.task_done()
    assert await wait is True
