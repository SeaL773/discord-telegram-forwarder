import asyncio
import json
from importlib import import_module
import httpx
pytest = import_module("pytest")

from src.LoggerManager import event_meta
from src.bridge_client import BridgeClient, CursorExpired, SessionLost
from src.health import HealthMonitor
from src.models import Envelope, WorkItem
from src.main import PendingCursors, wait_for_shutdown
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
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client, connector=FakeConnect(ws))
        async def pages(after, *, snapshot=False):
            assert after == "old" and snapshot is False
            assert ws.ready_sent
            await asyncio.sleep(0)
            yield Envelope("r1", frame("r1")["event"])
            yield Envelope("r2", frame("r2")["event"])
        bridge.rest_pages = pages
        stream = bridge.session("old", parse_rules({"rules": [], "default_action": "drop"}), lambda cursor: asyncio.sleep(0))
        result = [await anext(stream), await anext(stream), await anext(stream)]
        await stream.aclose()
    assert [x.cursor for x in result] == ["r1", "r2", "live"]
    assert bridge.connector.kwargs["proxy"] is None


@pytest.mark.asyncio
async def test_bad_ws_event_is_epoch_fatal_and_later_good_event_not_emitted():
    bad = frame("bad")
    bad["event"]["schema_version"] = 2
    ws = FakeWs([bad, frame("good")])
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client, connector=FakeConnect(ws))
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
async def test_409_callback_runs_before_post_ready_ws():
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
        item = await anext(stream)
        order.append(("event", item.cursor))
        await stream.aclose()
    assert order == [("gap", "r2"), ("event", "live")]


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
async def test_bootstrap_complete_snapshot_newest_ten_actual_channel_and_drops():
    events = [Envelope(str(i), {"event_type": "CREATED", "message": {"channel_id": "a" if i < 12 else "b", "content": "x"}}) for i in range(15)]
    rules = parse_rules({"rules": [{"match": {"channel_id": "a"}, "forward_to": {"chat_id": "1"}}], "default_action": "drop"})
    async with httpx.AsyncClient() as client:
        bridge = BridgeClient("http://bridge", "token", client)
        async def pages(after, *, snapshot=False):
            assert after is None and snapshot is True
            for item in events: yield item
        bridge.rest_pages = pages
        result = await bridge.bootstrap("14", rules)
    assert [x["cursor"] for x in result] == [str(i) for i in range(15)]
    assert [x["cursor"] for x in result if x["targets"]] == [str(i) for i in range(2, 12)]


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
    async def alert(_text): return True
    health = HealthMonitor(lambda: True, lambda: None, lambda: 0, lambda: False, alert)
    stop = asyncio.Event()
    async def crash(): raise RuntimeError("boom")
    failure = await wait_for_shutdown(stop, [asyncio.create_task(crash())], health)
    assert isinstance(failure, RuntimeError)
    assert health.snapshot()[0] == 503


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
