import json
from importlib import import_module
from pathlib import Path

import httpx
pytest = import_module("pytest")

from src.formatter import format_event
from src.main import pending_targets_are_fallback_only
from src.models import Attachment, DownloadedMedia, Envelope, Target
from src.state import StateStore
from src.tg_sender import DualLimiter, TgSender, TokenBucket


@pytest.mark.asyncio
async def test_token_bucket_waits():
    now = [0.0]
    waits = []

    async def sleep(value):
        waits.append(value)
        now[0] += value

    bucket = TokenBucket(10, 10, clock=lambda: now[0])
    limiter = DualLimiter(bucket, 1, sleep=sleep, chat_burst_capacity=1)
    await limiter.acquire("chat", 1)
    await limiter.acquire("chat", 1)
    assert waits == [60.0]


@pytest.mark.asyncio
async def test_dual_limiter_does_not_consume_global_while_chat_waits():
    now = [0.0]
    async def sleep(value): now[0] += value
    global_bucket = TokenBucket(5, 5, clock=lambda: now[0])
    limiter = DualLimiter(global_bucket, 1, sleep=sleep)
    limiter.chat_bucket("a").tokens = 0
    await limiter.acquire("a", 1)
    assert global_bucket.tokens == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("cost", [2, 10])
async def test_low_refill_rates_accept_media_group_burst_without_changing_refill_rates(cost):
    now = [0.0]
    waits = []

    async def sleep(value):
        waits.append(value)
        now[0] += value

    global_bucket = TokenBucket(10, 1, clock=lambda: now[0])
    limiter = DualLimiter(global_bucket, 1, sleep=sleep)
    await limiter.acquire("chat", cost)
    assert waits == []
    assert global_bucket.refill_per_second == 1
    assert limiter.chat_bucket("chat").refill_per_second == pytest.approx(1 / 60)
    global_bucket.tokens = 0
    limiter.chat_bucket("chat").tokens = 0
    await limiter.acquire("chat", cost)
    assert waits == [cost * 60.0]


@pytest.mark.parametrize(
    "targets,expected",
    [
        ([{"status": "pending", "phase": "fallback"}, {"status": "pending", "phase": "media"}], False),
        ([{"status": "sent", "phase": "media"}, {"status": "pending", "phase": "media"}], False),
        ([{"status": "sent", "phase": "media"}, {"status": "pending", "phase": "fallback"}], True),
    ],
)
def test_recovery_download_skip_uses_only_pending_target_phases(targets, expected):
    assert pending_targets_are_fallback_only({"cursor": "c", "targets": targets}, "c") is expected


@pytest.mark.asyncio
async def test_429_exact_wait_does_not_consume_failure_budget(tmp_path: Path):
    responses = [httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 2.5}}), httpx.Response(200, json={"ok": True})]
    waits = []

    async def sleep(value): waits.append(value)
    def handler(_request): return responses.pop(0)
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = TgSender("token", client, state, 1000, 1000, sleep=sleep)
        envelope = Envelope("c", {"event_type": "CREATED", "message": {"content": "x"}})
        await sender.send_event(envelope, [Target("1")], format_event(envelope.event), [], [])
    assert waits == [2.5]
    assert state.ack == "c" and not state.dead_letter_path.exists()


@pytest.mark.asyncio
async def test_persistent_three_failures_dead_letter_then_ack(tmp_path: Path):
    attempts = 0
    async def sleep(_value): return None
    def handler(_request):
        nonlocal attempts
        attempts += 1
        return httpx.Response(500)
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    event = {"event_type": "CREATED", "message": {"content": "recover me"}}
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = TgSender("token", client, state, 1000, 1000, sleep=sleep)
        await sender.send_event(Envelope("c", event), [Target("1")], format_event(event), [], [])
    assert attempts == 3 and state.ack == "c"
    record = json.loads(state.dead_letter_path.read_text().strip())
    assert record["event"]["message"]["content"] == "recover me"


@pytest.mark.asyncio
async def test_media_download_can_complete_before_sender_rate_wait(tmp_path: Path):
    sent = []
    def handler(request):
        sent.append(request.url.path)
        return httpx.Response(200, json={"ok": True})
    media = DownloadedMedia(Attachment("https://cdn/x", "x.png"), b"abc", "image/png", "photo")
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = TgSender("token", client, state, 1000, 1000)
        event = {"event_type": "CREATED", "message": {"content": "x"}}
        await sender.send_event(Envelope("c", event), [Target("1", 7)], format_event(event), [media], [])
    assert sent == ["/bottoken/sendPhoto"]


def test_media_batches_over_ten_and_mixed_are_not_dropped(tmp_path: Path):
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    client = httpx.AsyncClient()
    sender = TgSender("token", client, state)
    photo = lambda i: DownloadedMedia(Attachment(f"https://cdn/{i}", f"{i}.png"), b"x", "image/png", "photo")
    document = lambda i: DownloadedMedia(Attachment(f"https://cdn/{i}", f"{i}.bin"), b"x", "application/octet-stream", "document")
    batches = sender._requests(Target("1"), format_event({"event_type": "CREATED", "message": {"content": "x"}}), [photo(i) for i in range(11)] + [document(11), document(12)])
    assert [(batch.method, batch.cost) for batch in batches] == [("sendMediaGroup", 10), ("sendPhoto", 1), ("sendMediaGroup", 2)]


def test_media_group_parse_mode_only_inside_captioned_input_media(tmp_path: Path):
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    sender = TgSender("token", httpx.AsyncClient(), state)
    media = [DownloadedMedia(Attachment(f"https://cdn/{index}", f"{index}.png"), b"x", "image/png", "photo") for index in range(2)]
    batch = sender._requests(Target("1", 7), format_event({"event_type": "CREATED", "message": {"content": "x"}}), media)[0]
    assert batch.method == "sendMediaGroup"
    assert batch.data.keys() == {"chat_id", "message_thread_id", "media"}
    payload = json.loads(batch.data["media"])
    assert payload[0]["parse_mode"] == "HTML" and "caption" in payload[0]
    assert "parse_mode" not in payload[1]


@pytest.mark.asyncio
async def test_ok_false_error_code_classification_and_alert_retry(tmp_path: Path):
    responses = [httpx.Response(200, json={"ok": False, "error_code": 429, "parameters": {"retry_after": 3}}), httpx.Response(200, json={"ok": False, "error_code": 500}), httpx.Response(200, json={"ok": True})]
    waits = []
    async def sleep(value): waits.append(value)
    def handler(_request): return responses.pop(0)
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = TgSender("token", client, state, 1000, 1000, sleep=sleep)
        assert await sender.send_alert("1", "metadata")
    assert waits == [3.0, 1]
    assert state.in_flight is None


@pytest.mark.asyncio
async def test_media_400_switches_to_message_fallback(tmp_path: Path):
    requests = []
    def handler(request):
        requests.append((request.url.path, request.content))
        return httpx.Response(400, json={"ok": False, "error_code": 400}) if request.url.path.endswith("sendPhoto") else httpx.Response(200, json={"ok": True})
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    media = DownloadedMedia(Attachment("https://cdn.discordapp.com/x", "x.png"), b"x", "image/png", "photo")
    event = {"event_type": "CREATED", "message": {"content": "x"}}
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000).send_event(Envelope("c", event), [Target("1")], format_event(event), [media], [media.attachment.url], [media.attachment.url])
    assert [path for path, _ in requests] == ["/bottoken/sendPhoto", "/bottoken/sendMessage"] and state.ack == "c"
    assert requests[1][1].count(b"Attachment") == 1
    assert requests[1][1].count(b"cdn.discordapp.com%2Fx") == 1


@pytest.mark.asyncio
async def test_fallback_failure_deadletters_and_restart_resumes_without_media(tmp_path: Path):
    async def no_sleep(_value): return None
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    event = {"event_type": "CREATED", "message": {"content": "x"}}
    envelope = Envelope("c", event)
    await state.begin(envelope, [Target("1")])
    await state.set_fallback(0)
    calls = []
    def handler(request): calls.append(request.url.path); return httpx.Response(500)
    restarted = StateStore(state.path, state.dead_letter_path)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, restarted, 1000, 1000, sleep=no_sleep).send_event(envelope, [Target("1")], format_event(event), [], [], ["https://cdn.discordapp.com/x"])
    assert calls == ["/bottoken/sendMessage"] * 3
    assert restarted.ack == "c" and restarted.dead_letter_path.exists()


@pytest.mark.asyncio
async def test_restart_preserves_fallback_and_media_phases_per_target(tmp_path: Path):
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    event = {"event_type": "CREATED", "message": {"content": "x"}}
    envelope = Envelope("c", event)
    targets = [Target("fallback"), Target("media")]
    await state.begin(envelope, targets)
    await state.set_fallback(0)
    restarted = StateStore(state.path, state.dead_letter_path)
    paths = []

    def handler(request):
        paths.append((request.url.path, request.content))
        return httpx.Response(200, json={"ok": True})

    media = DownloadedMedia(Attachment("https://cdn.discordapp.com/x", "x.png"), b"image", "image/png", "photo")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, restarted, 1000, 1000).send_event(envelope, targets, format_event(event), [media], [], [media.attachment.url])
    assert [path for path, _ in paths] == ["/bottoken/sendMessage", "/bottoken/sendPhoto"]
    assert b"cdn.discordapp.com" in paths[0][1]
    assert paths[0][1].count(b"Attachment") == 1
    assert paths[0][1].count(b"cdn.discordapp.com%2Fx") == 1
    assert b"image" in paths[1][1]
    assert restarted.ack == "c"


@pytest.mark.asyncio
async def test_restart_ignores_sent_target_and_keeps_pending_media_delivery(tmp_path: Path):
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    event = {"event_type": "CREATED", "message": {"content": "x"}}
    envelope = Envelope("c", event)
    targets = [Target("sent"), Target("media")]
    await state.begin(envelope, targets)
    await state.terminal(0, "sent")
    restarted = StateStore(state.path, state.dead_letter_path)
    paths = []

    def handler(request):
        paths.append(request.url.path)
        return httpx.Response(200, json={"ok": True})

    media = DownloadedMedia(Attachment("https://cdn.discordapp.com/x", "x.png"), b"image", "image/png", "photo")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, restarted, 1000, 1000).send_event(envelope, targets, format_event(event), [media], [], [media.attachment.url])
    assert paths == ["/bottoken/sendPhoto"]
    assert restarted.ack == "c"


@pytest.mark.asyncio
async def test_failed_download_url_appears_once_during_normal_text_delivery(tmp_path: Path):
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    event = {"event_type": "CREATED", "message": {"content": "x"}}
    url = "https://cdn.discordapp.com/failed"
    bodies = []

    def handler(request):
        bodies.append(request.content)
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000).send_event(Envelope("c", event), [Target("1")], format_event(event), [], [url], [url])
    assert len(bodies) == 1
    assert bodies[0].count(b"Attachment") == 1
    assert bodies[0].count(b"cdn.discordapp.com%2Ffailed") == 1


@pytest.mark.asyncio
async def test_lone_surrogate_formats_persists_sends_and_acks(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    event = {"event_type": "CREATED", "message": {"content": "bad\ud800value"}}
    formatted = format_event(event)
    assert "bad�value" in formatted.text
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000).send_event(Envelope("c", event), [Target("1")], formatted, [], [])
    assert len(requests) == 1
    assert b"bad%EF%BF%BDvalue" in requests[0].content
    assert state.ack == "c"
