import json
import os
from importlib import import_module
from pathlib import Path

import httpx
pytest = import_module("pytest")

from src.media import ALLOWED_MEDIA_HOSTS, MediaHandler, extract_attachments
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
async def test_rich_retry_counter_and_phase_survive_restart(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    await state.begin(Envelope("cursor", {"message": {"content": "payload"}}), [Target("1")], "rich")
    await state.retry(0, rich=True)

    restarted = StateStore(state.path, state.dead_letter_path)
    assert restarted.in_flight is not None
    assert restarted.in_flight["targets"][0]["phase"] == "rich"
    assert restarted.in_flight["targets"][0]["rich_retries"] == 1


@pytest.mark.asyncio
async def test_set_media_persist_failure_rolls_back_rich_phase(tmp_path: Path, monkeypatch):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    await state.begin(Envelope("cursor", {"message": {"content": "payload"}}), [Target("1")], "rich")

    async def fail_persist():
        raise OSError("disk full")

    monkeypatch.setattr(state, "_persist", fail_persist)
    with pytest.raises(OSError, match="disk full"):
        await state.set_media(0)
    assert state.in_flight is not None and state.in_flight["targets"][0]["phase"] == "rich"


def target_dead_letter_record(cursor="cursor", chat_id="1"):
    return {"cursor": cursor, "event": {"message": {"content": "payload"}}, "target": {"chat_id": chat_id, "thread_id": None}, "reason": "http_400"}


@pytest.mark.asyncio
async def test_target_dead_letter_crash_before_append_keeps_target_pending(tmp_path: Path, monkeypatch):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    await state.begin(Envelope("cursor", {"x": 1}), [Target("1")])
    monkeypatch.setattr(state, "_append_dead_letter", lambda _record: (_ for _ in ()).throw(OSError("append failed")))
    with pytest.raises(OSError, match="append failed"):
        await state.dead_letter_target(0, target_dead_letter_record())
    inflight = state.in_flight
    assert inflight is not None and inflight["targets"][0]["status"] == "pending"
    assert not state.dead_letter_path.exists()


@pytest.mark.asyncio
async def test_target_dead_letter_recovers_after_append_before_terminal_without_duplicate(tmp_path: Path, monkeypatch):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    await state.begin(Envelope("cursor", {"x": 1}), [Target("1"), Target("2")])
    original_persist = state._persist
    monkeypatch.setattr(state, "_persist", lambda: (_ for _ in ()).throw(OSError("terminal persist failed")))
    with pytest.raises(OSError, match="terminal persist failed"):
        await state.dead_letter_target(0, target_dead_letter_record())
    assert state.dead_letter_path.read_text().count("\n") == 1

    monkeypatch.setattr(state, "_persist", original_persist)
    restarted = StateStore(state.path, state.dead_letter_path)
    await restarted.recover_target_dead_letters()
    inflight = restarted.in_flight
    assert restarted.dead_letter_path.read_text().count("\n") == 1
    assert inflight is not None and [target["status"] for target in inflight["targets"]] == ["dead_lettered", "pending"]
    assert restarted.data["stats"]["dead_lettered"] == 1

    again = StateStore(state.path, state.dead_letter_path)
    await again.recover_target_dead_letters()
    assert again.dead_letter_path.read_text().count("\n") == 1
    assert again.data["stats"]["dead_lettered"] == 1


@pytest.mark.asyncio
async def test_dead_letter_rotates_before_threshold_and_bounds_backups(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson", dead_letter_max_bytes=100, dead_letter_backup_count=2)
    records = [{"sequence": index, "payload": "x" * 45} for index in range(4)]

    for record in records:
        await state.dead_letter(record)

    paths = [state.dead_letter_path, Path(f"{state.dead_letter_path}.1"), Path(f"{state.dead_letter_path}.2")]
    assert all(path.is_file() for path in paths)
    assert not Path(f"{state.dead_letter_path}.3").exists()
    retained = [json.loads(line)["sequence"] for path in reversed(paths) for line in path.read_text().splitlines()]
    assert retained == [1, 2, 3]
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in paths)


@pytest.mark.asyncio
async def test_target_dead_letter_recovers_identity_from_rotated_file_without_duplicate(tmp_path: Path, monkeypatch):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson", dead_letter_max_bytes=160, dead_letter_backup_count=2)
    await state.begin(Envelope("cursor", {"x": 1}), [Target("1")])
    original_persist = state._persist
    monkeypatch.setattr(state, "_persist", lambda: (_ for _ in ()).throw(OSError("terminal persist failed")))
    with pytest.raises(OSError, match="terminal persist failed"):
        await state.dead_letter_target(0, target_dead_letter_record())
    monkeypatch.setattr(state, "_persist", original_persist)

    await state.dead_letter({"payload": "x" * 160})
    rotated = Path(f"{state.dead_letter_path}.1")
    assert "target:cursor:0:1:" in rotated.read_text()

    restarted = StateStore(state.path, state.dead_letter_path, dead_letter_max_bytes=160, dead_letter_backup_count=2)
    await restarted.recover_target_dead_letters()
    await restarted.recover_target_dead_letters()

    inflight = restarted.in_flight
    assert inflight is not None and inflight["targets"][0]["status"] == "dead_lettered"
    assert restarted.data["stats"]["dead_lettered"] == 2
    assert sum(path.read_text().count("target:cursor:0:1:") for path in (state.dead_letter_path, rotated)) == 1


@pytest.mark.asyncio
async def test_rotated_recovery_tolerates_malformed_records_and_rejects_symlink(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson", dead_letter_max_bytes=160, dead_letter_backup_count=2)
    await state.begin(Envelope("cursor", {"x": 1}), [Target("1")])
    identity = state._target_dead_letter_id(0)
    Path(f"{state.dead_letter_path}.2").write_bytes(b"not-json\n" + json.dumps({"dead_letter_id": identity}).encode() + b"\n")

    restarted = StateStore(state.path, state.dead_letter_path, dead_letter_max_bytes=160, dead_letter_backup_count=2)
    await restarted.recover_target_dead_letters()
    assert restarted.in_flight is not None and restarted.in_flight["targets"][0]["status"] == "dead_lettered"

    linked_state = StateStore(tmp_path / "other-state.json", tmp_path / "other-dead.ndjson", dead_letter_max_bytes=160, dead_letter_backup_count=2)
    await linked_state.begin(Envelope("other", {"x": 1}), [Target("1")])
    Path(f"{linked_state.dead_letter_path}.1").symlink_to(Path(f"{state.dead_letter_path}.2"))
    with pytest.raises(OSError, match="symlink"):
        await linked_state.recover_target_dead_letters()


@pytest.mark.asyncio
async def test_empty_active_after_crash_does_not_evict_recovery_identity(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson", dead_letter_max_bytes=100, dead_letter_backup_count=2)
    await state.begin(Envelope("cursor", {"x": 1}), [Target("1")])
    identity = state._target_dead_letter_id(0)
    oldest = Path(f"{state.dead_letter_path}.2")
    oldest.write_text(json.dumps({"dead_letter_id": identity}) + "\n", encoding="ascii")
    state.dead_letter_path.touch()

    await state.dead_letter({"payload": "x" * 150})

    assert oldest.exists()
    restarted = StateStore(state.path, state.dead_letter_path, dead_letter_max_bytes=100, dead_letter_backup_count=2)
    await restarted.recover_target_dead_letters()
    assert restarted.in_flight is not None and restarted.in_flight["targets"][0]["status"] == "dead_lettered"


@pytest.mark.asyncio
async def test_recovery_survives_interrupted_rotation_shift(tmp_path: Path, monkeypatch):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson", dead_letter_max_bytes=100, dead_letter_backup_count=2)
    await state.begin(Envelope("cursor", {"x": 1}), [Target("1")])
    identity = state._target_dead_letter_id(0)
    state.dead_letter_path.write_text(json.dumps({"dead_letter_id": identity}) + "\n", encoding="ascii")
    Path(f"{state.dead_letter_path}.1").write_text('{"old":true}\n', encoding="ascii")
    original_replace = os.replace

    def fail_active_shift(source, destination):
        if Path(source) == state.dead_letter_path:
            raise OSError("crash during rotation")
        original_replace(source, destination)

    monkeypatch.setattr(os, "replace", fail_active_shift)
    with pytest.raises(OSError, match="crash during rotation"):
        await state.dead_letter({"payload": "x" * 150})
    monkeypatch.setattr(os, "replace", original_replace)

    restarted = StateStore(state.path, state.dead_letter_path, dead_letter_max_bytes=100, dead_letter_backup_count=2)
    await restarted.recover_target_dead_letters()
    assert restarted.in_flight is not None and restarted.in_flight["targets"][0]["status"] == "dead_lettered"
    assert sum(path.read_text().count(identity) for path in restarted._dead_letter_paths() if path.exists()) == 1


@pytest.mark.asyncio
async def test_oversized_target_record_keeps_recoverable_identity_prefix(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("src.state.DLQ_MAX_RECOVERY_RECORD_BYTES", 64)
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson", dead_letter_max_bytes=100, dead_letter_backup_count=2)
    await state.begin(Envelope("cursor", {"x": 1}), [Target("1")])
    original_persist = state._persist
    monkeypatch.setattr(state, "_persist", lambda: (_ for _ in ()).throw(OSError("terminal persist failed")))
    with pytest.raises(OSError, match="terminal persist failed"):
        await state.dead_letter_target(0, {**target_dead_letter_record(), "event": {"payload": "x" * 256}})
    monkeypatch.setattr(state, "_persist", original_persist)

    restarted = StateStore(state.path, state.dead_letter_path, dead_letter_max_bytes=100, dead_letter_backup_count=2)
    await restarted.recover_target_dead_letters()

    assert restarted.in_flight is not None and restarted.in_flight["targets"][0]["status"] == "dead_lettered"
    assert restarted.dead_letter_path.read_bytes().startswith(b'{"dead_letter_id":"target:cursor:0:1:",')


@pytest.mark.asyncio
async def test_target_dead_letter_normal_transition_is_terminal_and_single_record(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    await state.begin(Envelope("cursor", {"x": 1}), [Target("1"), Target("2")])
    await state.dead_letter_target(0, target_dead_letter_record())
    record = json.loads(state.dead_letter_path.read_text().strip())
    inflight = state.in_flight
    assert record["dead_letter_id"] == "target:cursor:0:1:"
    assert inflight is not None and [target["status"] for target in inflight["targets"]] == ["dead_lettered", "pending"]
    assert state.dead_letter_path.read_text().count("\n") == 1


@pytest.mark.asyncio
async def test_target_dead_letter_recovery_ignores_incomplete_crash_record(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    await state.begin(Envelope("cursor", {"x": 1}), [Target("1")])
    state.dead_letter_path.write_text('{"dead_letter_id":"incomplete', encoding="ascii")

    restarted = StateStore(state.path, state.dead_letter_path)
    await restarted.recover_target_dead_letters()

    inflight = restarted.in_flight
    assert inflight is not None and inflight["targets"][0]["status"] == "pending"


@pytest.mark.asyncio
async def test_target_dead_letter_recovery_ignores_nested_identity_field(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    await state.begin(Envelope("cursor", {"x": 1}), [Target("1")])
    identity = state._target_dead_letter_id(0)
    state.dead_letter_path.write_text(
        json.dumps({"event": {"dead_letter_id": identity}, "reason": "prepare"}) + "\n",
        encoding="ascii",
    )

    restarted = StateStore(state.path, state.dead_letter_path)
    await restarted.recover_target_dead_letters()

    inflight = restarted.in_flight
    assert inflight is not None and inflight["targets"][0]["status"] == "pending"


@pytest.mark.asyncio
async def test_target_dead_letter_partial_write_retry_is_separated_and_recoverable(tmp_path: Path, monkeypatch):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    await state.begin(Envelope("cursor", {"x": 1}), [Target("1")])
    original_write = os.write
    calls = 0

    def partial_then_raise(fd, payload):
        nonlocal calls
        calls += 1
        if calls == 1:
            return original_write(fd, payload[:17])
        raise OSError("crash during append")

    monkeypatch.setattr(os, "write", partial_then_raise)
    with pytest.raises(OSError, match="crash during append"):
        await state.dead_letter_target(0, target_dead_letter_record())
    monkeypatch.setattr(os, "write", original_write)

    restarted = StateStore(state.path, state.dead_letter_path)
    await restarted.dead_letter_target(0, target_dead_letter_record())
    lines = state.dead_letter_path.read_bytes().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[1])["dead_letter_id"] == "target:cursor:0:1:"

    again = StateStore(state.path, state.dead_letter_path)
    await again.recover_target_dead_letters()
    assert again.data["stats"]["dead_lettered"] == 1
    assert state.dead_letter_path.read_bytes().count(b"\n") == 2


@pytest.mark.asyncio
async def test_target_dead_letter_recovery_tolerates_corrupt_bytes_huge_lines_and_chunk_boundaries(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    await state.begin(Envelope("cursor", {"x": 1}), [Target("1"), Target("2")])
    first_id = b'target:cursor:0:1:'
    second_id = b'target:cursor:1:2:'
    valid_record = json.dumps({
        "padding": "x" * (64 * 1024),
        "dead_letter_id": first_id.decode(),
    }, separators=(",", ":")).encode()
    state.dead_letter_path.write_bytes(
        b'\xff\xfe corrupt\n'
        + b'z' * (2 * 64 * 1024) + b' malformed\n'
        + b'not-json-prefix{"dead_letter_id":"' + first_id + b'"}\n'
        + valid_record + b'\n'
        + b'{"dead_letter_id":"' + second_id + b'"}'
    )

    await state.recover_target_dead_letters()
    inflight = state.in_flight
    assert inflight is not None
    assert [target["status"] for target in inflight["targets"]] == ["dead_lettered", "pending"]
    assert state.data["stats"]["dead_lettered"] == 1


@pytest.mark.asyncio
async def test_target_dead_letter_recovery_scans_once_for_multiple_targets_and_is_idempotent(tmp_path: Path, monkeypatch):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    await state.begin(Envelope("cursor", {"x": 1}), [Target("1"), Target("2"), Target("3")])
    records = [
        {**target_dead_letter_record(chat_id="1"), "dead_letter_id": "target:cursor:0:1:"},
        {**target_dead_letter_record(chat_id="3"), "dead_letter_id": "target:cursor:2:3:"},
    ]
    state.dead_letter_path.write_bytes(b"".join((json.dumps(record, separators=(",", ":")) + "\n").encode() for record in records))
    read_calls = 0
    original_read = os.read

    def counted_read(fd, size):
        nonlocal read_calls
        read_calls += 1
        return original_read(fd, size)

    monkeypatch.setattr(os, "read", counted_read)
    await state.recover_target_dead_letters()
    first_read_calls = read_calls
    await state.recover_target_dead_letters()

    inflight = state.in_flight
    assert inflight is not None
    assert [target["status"] for target in inflight["targets"]] == ["dead_lettered", "pending", "dead_lettered"]
    assert state.data["stats"]["dead_lettered"] == 2
    assert read_calls == first_read_calls
    assert state.dead_letter_path.read_bytes().count(b"\n") == 2


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


def test_embed_media_preserves_attachments_deduplicates_and_prefers_safe_proxy():
    attachment = "https://cdn.discordapp.com/attachments/a/original.png"
    proxy = "https://images-ext-1.discordapp.net/external/hash/image.jpg"
    gallery_proxy = "https://images-ext-2.discordapp.net/external/hash/gallery.jpg"
    event = {"message": {
        "attachments": [{"url": attachment, "filename": "original.png", "content_type": "image/png", "size": 7}],
        "embeds": [{
            "image": {"url": attachment, "proxy_url": "https://evil.example/proxy.png"},
            "images": [
                {"url": "https://pbs.twimg.com/media/gallery.jpg", "proxy_url": gallery_proxy, "content_type": "image/jpeg"},
                {"url": attachment},
            ],
            "thumbnail": {"url": "https://pbs.twimg.com/media/source.jpg", "proxyUrl": proxy, "contentType": "image/jpeg", "size": 9},
            "video": {"url": "https://cdn.discordapp.com/video.mp4"},
        }],
    }}
    attachments = extract_attachments(event)
    assert [(item.url, item.filename, item.content_type, item.declared_size) for item in attachments] == [
        (attachment, "original.png", "image/png", 7),
        (gallery_proxy, "gallery.jpg", "image/jpeg", None),
        (proxy, "image.jpg", "image/jpeg", 9),
    ]


@pytest.mark.asyncio
async def test_embed_images_mapping_shape_preserves_order_and_global_media_cap():
    gallery = {str(index): {"url": f"https://pbs.twimg.com/media/{index}.jpg"} for index in range(25)}
    event = {"message": {
        "attachments": [{"url": "https://cdn.discordapp.com/attachment.jpg"}],
        "embeds": {"images": gallery, "thumbnail": {"url": "https://pbs.twimg.com/media/thumb.jpg"}},
    }}
    attachments = extract_attachments(event)
    assert [item.url for item in attachments[:4]] == [
        "https://cdn.discordapp.com/attachment.jpg",
        "https://pbs.twimg.com/media/0.jpg",
        "https://pbs.twimg.com/media/1.jpg",
        "https://pbs.twimg.com/media/2.jpg",
    ]
    assert len(attachments) == 27

    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(200, headers={"content-type": "image/jpeg"}, content=b"img")

    async def resolver(_host): return ["8.8.8.8"]

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        downloaded, failed = await MediaHandler(client, 20, 15, max_attachments=20, resolver=resolver).download_all(event)
    assert len(downloaded) == 20
    assert [item.attachment.url for item in downloaded] == [item.url for item in attachments[:20]]
    assert failed == [item.url for item in attachments[20:]]
    assert calls == [item.url for item in attachments[:20]]


def test_embed_media_uses_source_when_proxy_host_is_not_explicitly_allowed():
    source = "https://pbs.twimg.com/media/source.jpg"
    attachments = extract_attachments({"message": {"embeds": {"image": {"url": source, "proxy_url": "https://example.com/proxy"}}}})
    assert [item.url for item in attachments] == [source]
    assert ALLOWED_MEDIA_HOSTS == {
        "cdn.discordapp.com",
        "images-ext-1.discordapp.net",
        "images-ext-2.discordapp.net",
        "media.discordapp.net",
        "pbs.twimg.com",
    }


@pytest.mark.asyncio
async def test_malformed_embed_url_falls_back_without_rejecting_event_or_other_media():
    from src.formatter import add_fallbacks
    from src.main import prepare_work
    from src.models import PreparedEvent, WorkItem

    bad = "https://["
    good = "https://pbs.twimg.com/media/good.jpg"
    event = {
        "event_type": "CREATED",
        "message": {
            "content": "keep this text",
            "embeds": [
                {"image": {"url": bad}},
                {"thumbnail": {"url": good}},
            ],
        },
    }

    class Router:
        def route(self, event):
            del event
            return [Target("1")]

    def handler(request):
        assert str(request.url) == good
        return httpx.Response(200, headers={"content-type": "image/jpeg"}, content=b"img")

    async def resolver(_host): return ["8.8.8.8"]

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        media = MediaHandler(client, 20, 15, resolver=resolver)
        prepared = await prepare_work(WorkItem(Envelope("cursor", event)), Router(), media, None)

    assert isinstance(prepared, PreparedEvent)
    assert prepared.envelope.event == event
    assert "keep this text" in prepared.formatted.text
    assert [item.attachment.filename for item in prepared.media] == ["good.jpg"]
    assert prepared.fallback_urls == [bad]
    assert prepared.attachment_urls == [bad, good]
    fallback = add_fallbacks(prepared.formatted, prepared.fallback_urls)
    assert fallback.text.endswith("\nAttachment unavailable")


def test_non_finite_or_negative_declared_media_size_is_ignored():
    event = {"message": {"attachments": [
        {"url": "https://cdn.discordapp.com/a", "size": float("inf")},
        {"url": "https://cdn.discordapp.com/b", "size": float("nan")},
        {"url": "https://cdn.discordapp.com/c", "size": -1},
    ]}}
    assert [item.declared_size for item in extract_attachments(event)] == [None, None, None]


@pytest.mark.asyncio
async def test_external_embed_hosts_download_or_fallback_exactly_once():
    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(200, headers={"content-type": "image/jpeg"}, content=b"img")

    async def resolver(_host): return ["8.8.8.8"]
    accepted = [
        "https://pbs.twimg.com/media/a.jpg",
        "https://images-ext-2.discordapp.net/external/hash/b.jpg",
    ]
    rejected = "https://sub.pbs.twimg.com/media/c.jpg"
    event = {"message": {"embeds": [
        {"image": {"url": accepted[0]}},
        {"thumbnail": {"url": accepted[1]}},
        {"image": {"url": rejected}},
        {"thumbnail": {"url": rejected}},
    ]}}
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=False) as client:
        downloaded, failed = await MediaHandler(client, 20, 15, resolver=resolver).download_all(event)
    assert [item.attachment.url for item in downloaded] == accepted
    assert failed == [rejected]
    assert calls == accepted


@pytest.mark.asyncio
async def test_embed_downloads_prepare_for_sender_and_failed_source_survives():
    good = "https://pbs.twimg.com/media/good.jpg"
    bad = "https://example.com/bad.jpg"

    def handler(_request):
        return httpx.Response(200, headers={"content-type": "image/jpeg"}, content=b"img")

    async def resolver(_host): return ["8.8.8.8"]
    event = {"message": {"embeds": [{"image": {"url": good}}, {"thumbnail": {"url": bad}}]}}
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        downloaded, failed = await MediaHandler(client, 20, 15, resolver=resolver).download_all(event)
    assert len(downloaded) == 1 and downloaded[0].kind == "photo"
    assert downloaded[0].attachment.filename == "good.jpg"
    assert failed == [bad]


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
    path.write_text('admin_chat_id: "123"', encoding="utf-8")
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
async def test_reject_event_deadletters_before_atomic_ack_and_advances_bootstrap(tmp_path: Path, monkeypatch):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    item = {"cursor": "bad", "event": {"schema_version": 2}, "targets": []}
    await state.save_bootstrap("bad", [item])
    await state.reject_event("bad", item["event"], "invalid event schema")
    assert state.ack == "bad" and state.bootstrap is None
    record = json.loads(state.dead_letter_path.read_text().strip())
    assert record == {"cursor": "bad", "event": item["event"], "reason": "invalid event schema", "phase": "prepare"}

    state = StateStore(tmp_path / "other-state.json", tmp_path / "other-dead.ndjson")
    original = state._persist
    async def fail_persist(): raise OSError("disk full")
    monkeypatch.setattr(state, "_persist", fail_persist)
    with pytest.raises(OSError):
        await state.reject_event("retry", {"x": 1}, "bad")
    assert state.ack is None
    assert state.dead_letter_path.read_text().count("\n") == 1
    monkeypatch.setattr(state, "_persist", original)
    await state.reject_event("retry", {"x": 1}, "bad")
    assert state.ack == "retry" and state.dead_letter_path.read_text().count("\n") == 2


@pytest.mark.asyncio
async def test_reject_event_requires_matching_inflight_or_bootstrap_cursor(tmp_path: Path):
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    await state.begin(Envelope("current", {"x": 1}), [Target("1")])
    with pytest.raises(RuntimeError):
        await state.reject_event("other", {"x": 2}, "bad")
    assert not state.dead_letter_path.exists() and state.ack is None


@pytest.mark.asyncio
async def test_dead_letter_short_writes_are_completed_before_ack(tmp_path: Path, monkeypatch):
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    original_write = __import__("os").write
    writes = []

    def short_write(fd, data):
        chunk = data[:max(1, len(data) // 2)]
        writes.append(len(chunk))
        return original_write(fd, chunk)

    monkeypatch.setattr("os.write", short_write)
    await state.reject_event("cursor", {"value": "payload"}, "poison")
    record = json.loads(state.dead_letter_path.read_text().strip())
    assert record["cursor"] == "cursor" and state.ack == "cursor" and len(writes) > 1


@pytest.mark.asyncio
async def test_dead_letter_zero_progress_does_not_advance_ack(tmp_path: Path, monkeypatch):
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    monkeypatch.setattr("os.write", lambda _fd, _data: 0)
    with pytest.raises(OSError, match="no progress"):
        await state.reject_event("cursor", {"value": "payload"}, "poison")
    assert state.ack is None


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
