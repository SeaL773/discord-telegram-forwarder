import json
from importlib import import_module
from pathlib import Path

import httpx
pytest = import_module("pytest")

from src.media import MediaHandler, extract_attachments
from src.models import Envelope, Target
from src.state import StateStore


@pytest.mark.asyncio
async def test_state_atomic_fanout_restart_and_dead_letter_order(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    envelope = Envelope("cursor", {"message": {"content": "secret"}})
    await state.begin(envelope, [Target("1"), Target("2")])
    await state.terminal(0, "sent")
    await state.retry(1)
    restarted = StateStore(state.path, state.dead_letter_path)
    inflight = restarted.in_flight
    assert inflight is not None
    assert [x["status"] for x in inflight["targets"]] == ["sent", "pending"]
    assert inflight["targets"][1]["retries"] == 1
    await restarted.dead_letter({"cursor": "cursor", "event": envelope.event})
    assert restarted.dead_letter_path.exists()
    await restarted.terminal(1, "dead_lettered")
    await restarted.finish("cursor", "forwarded")
    persisted = json.loads(restarted.path.read_text())
    assert persisted["last_acked_cursor"] == "cursor" and persisted["in_flight"] is None


@pytest.mark.asyncio
async def test_state_and_dead_letter_round_trip_unpaired_surrogate(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    event = {"message": {"content": "bad\ud800value"}}
    await state.begin(Envelope("cursor", event), [Target("1")])
    assert "\\ud800" in state.path.read_text(encoding="ascii")
    restarted = StateStore(state.path, state.dead_letter_path)
    inflight = restarted.in_flight
    assert inflight is not None and inflight["event"] == event
    await restarted.dead_letter({"cursor": "cursor", "event": event})
    assert json.loads(restarted.dead_letter_path.read_text(encoding="ascii"))["event"] == event


@pytest.mark.asyncio
async def test_media_extract_mapping_limits_and_fallback():
    calls = []

    def handler(request: httpx.Request):
        calls.append(str(request.url))
        if request.url.path.endswith("ok"):
            return httpx.Response(200, headers={"content-type": "image/png", "content-length": "3"}, content=b"img")
        if request.url.path.endswith("large"):
            return httpx.Response(200, headers={"content-length": "21"}, content=b"x" * 21)
        return httpx.Response(403)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async def resolver(_host): return ["8.8.8.8"]
        media = MediaHandler(client, 20, 15, allowed_hosts={"cdn.discordapp.com"}, resolver=resolver)
        event = {"message": {"attachments": {"a": {"url": "https://cdn.discordapp.com/ok", "filename": "a.png"}, "b": {"url": "https://cdn.discordapp.com/large"}, "c": {"url": "https://cdn.discordapp.com/fail"}}}}
        assert len(extract_attachments(event)) == 3
        downloaded, failed = await media.download_all(event)
    assert downloaded[0].kind == "photo" and downloaded[0].data == b"img"
    assert failed == ["https://cdn.discordapp.com/large", "https://cdn.discordapp.com/fail"]
    assert calls == ["https://cdn.discordapp.com/ok", "https://cdn.discordapp.com/large", "https://cdn.discordapp.com/fail"]


@pytest.mark.asyncio
async def test_media_response_type_wins_and_bad_content_length_falls_back():
    def handler(request):
        if request.url.path == "/type":
            return httpx.Response(200, headers={"content-type": "video/mp4"}, content=b"v")
        return httpx.Response(200, headers={"content-length": "bad"}, content=b"x")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async def resolver(_host): return ["8.8.8.8"]
        media = MediaHandler(client, 20, 15, allowed_hosts={"cdn.discordapp.com"}, resolver=resolver)
        downloaded, failed = await media.download_all({"message": {"attachments": [{"url": "https://cdn.discordapp.com/type", "content_type": "image/png"}, {"url": "https://cdn.discordapp.com/bad"}]}})
    assert downloaded[0].kind == "video"
    assert failed == ["https://cdn.discordapp.com/bad"]


@pytest.mark.asyncio
@pytest.mark.parametrize("url", [
    "http://cdn.discordapp.com/x",
    "https://example.com/x",
    "https://cdn.discordapp.com:444/x",
    "https://127.0.0.1/x",
])
async def test_media_ssrf_rejects_unsafe_urls_without_request(url):
    calls = []
    def handler(request): calls.append(request); return httpx.Response(200, content=b"x")
    async def resolver(_host): return ["8.8.8.8"]
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        downloaded, failed = await MediaHandler(client, 20, 15, resolver=resolver).download_all({"message": {"attachments": [{"url": url}]}})
    assert downloaded == [] and failed == [url] and calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("address", ["127.0.0.1", "10.0.0.1", "169.254.1.1", "224.0.0.1", "0.0.0.0"])
async def test_media_dns_private_ranges_rejected(address):
    calls = []
    async def resolver(_host): return [address]
    def handler(request): calls.append(request); return httpx.Response(200, content=b"x")
    url = "https://cdn.discordapp.com/x"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        downloaded, failed = await MediaHandler(client, 20, 15, resolver=resolver).download_all({"message": {"attachments": [{"url": url}]}})
    assert downloaded == [] and failed == [url] and calls == []


@pytest.mark.asyncio
async def test_media_redirect_is_fallback_without_second_request():
    calls = []
    async def resolver(_host): return ["8.8.8.8"]
    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(302, headers={"location": "http://127.0.0.1/private"})
    url = "https://cdn.discordapp.com/x"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=False) as client:
        downloaded, failed = await MediaHandler(client, 20, 15, resolver=resolver).download_all({"message": {"attachments": [{"url": url}]}})
    assert downloaded == [] and failed == [url] and calls == [url]


@pytest.mark.asyncio
async def test_media_attachment_and_aggregate_limits_keep_small_multibatch():
    async def resolver(_host): return ["8.8.8.8"]
    def handler(_request): return httpx.Response(200, headers={"content-type": "image/png"}, content=b"1234")
    attachments = [{"url": f"https://cdn.discordapp.com/{index}"} for index in range(22)]
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        media, failed = await MediaHandler(client, 10, 15, max_attachments=20, max_total_bytes=40, resolver=resolver).download_all({"message": {"attachments": attachments}})
    assert len(media) == 10 and len(failed) == 12


def test_state_corruption_is_preserved_and_fails(tmp_path: Path):
    path = tmp_path / "state.json"
    path.write_text('{"version":1,"last_acked_cursor":7}')
    with pytest.raises(RuntimeError):
        StateStore(path, tmp_path / "dead")
    assert (tmp_path / "state.json.corrupt").exists()


def test_config_defaults_health_to_loopback(tmp_path: Path, monkeypatch):
    from src.config import load_config

    monkeypatch.setenv("TG_BOT_TOKEN", "fake")
    monkeypatch.setenv("BRIDGE_TOKEN", "fake")
    path = tmp_path / "config.yaml"
    path.write_text("{}", encoding="utf-8")
    assert load_config(path).health_host == "127.0.0.1"


@pytest.mark.asyncio
async def test_bootstrap_plan_crash_restart_preserves_order_targets_and_index(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead")
    items = [
        {"cursor": "a", "event": {"body": "one"}, "targets": []},
        {"cursor": "b", "event": {"body": "two"}, "targets": [{"chat_id": "1", "thread_id": 7}]},
    ]
    await state.save_bootstrap("b", items)
    restarted = StateStore(state.path, state.dead_letter_path)
    bootstrap = restarted.bootstrap
    assert bootstrap is not None
    assert restarted.ack is None and bootstrap["next_index"] == 0
    await restarted.finish("a", "dropped")
    restarted = StateStore(state.path, state.dead_letter_path)
    bootstrap = restarted.bootstrap
    assert bootstrap is not None
    assert restarted.ack == "a" and bootstrap["next_index"] == 1
    assert bootstrap["items"][1] == items[1]
    await restarted.begin(Envelope("b", items[1]["event"]), [Target("1", 7)])
    await restarted.terminal(0, "sent")
    await restarted.finish("b", "forwarded")
    assert restarted.bootstrap is None and restarted.ack == "b"


@pytest.mark.asyncio
async def test_state_invariants_and_private_permissions_symlink_rejection(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead")
    event = Envelope("c", {"body": "private"})
    await state.begin(event, [Target("1")])
    with pytest.raises(RuntimeError): await state.begin(event, [Target("1")])
    with pytest.raises(RuntimeError): await state.finish("other", "forwarded")
    with pytest.raises(ValueError): await state.finish("c", "unknown")
    with pytest.raises(IndexError): await state.retry(4)
    await state.dead_letter({"event": event.event})
    assert state.path.stat().st_mode & 0o777 == 0o600
    assert state.dead_letter_path.stat().st_mode & 0o777 == 0o600
    link = tmp_path / "link"
    link.symlink_to(state.dead_letter_path)
    linked = StateStore(tmp_path / "other-state", link)
    with pytest.raises(OSError): await linked.dead_letter({"x": 1})
