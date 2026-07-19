from pathlib import Path
from importlib import import_module

import httpx
pytest = import_module("pytest")

from src.formatter import format_event
from src.media import MediaHandler
from src.models import Envelope
from src.router import parse_rules
from src.state import StateStore
from src.tg_sender import TgSender


@pytest.mark.asyncio
async def test_fake_services_end_to_end_pipeline(tmp_path: Path):
    calls: list[tuple[str, bytes]] = []

    def fake_services(request: httpx.Request) -> httpx.Response:
        if request.url.host == "cdn.fake":
            return httpx.Response(200, headers={"content-type": "image/png"}, content=b"fake-image")
        calls.append((request.url.path, request.content))
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})

    event = {
        "event_type": "CREATED",
        "message": {
            "guild_id": "guild",
            "channel_id": "channel",
            "channel_name": "signals",
            "guild_name": "server",
            "content": "fake QA <payload>",
            "author": {"username": "tester"},
            "attachments": [{"url": "https://cdn.fake/image", "filename": "image.png"}],
        },
    }
    rules = parse_rules({"rules": [{"match": {"channel_id": "channel"}, "forward_to": [{"chat_id": "-1001"}, {"chat_id": "-1002", "thread_id": 9}]}], "default_action": "drop"})
    targets = rules.route(event)
    state = StateStore(tmp_path / "state.json", tmp_path / "failed.ndjson")
    async with httpx.AsyncClient(transport=httpx.MockTransport(fake_services)) as client:
        async def resolver(_host): return ["8.8.8.8"]
        downloaded, failed = await MediaHandler(client, 1024, 15, allowed_hosts={"cdn.fake"}, resolver=resolver).download_all(event)
        await TgSender("fake-token", client, state, 1000, 1000).send_event(Envelope("qa-cursor", event), targets, format_event(event), downloaded, failed)
    assert [path for path, _ in calls] == ["/botfake-token/sendPhoto", "/botfake-token/sendPhoto"]
    assert all(b"fake-image" in body for _, body in calls)
    assert state.ack == "qa-cursor" and state.in_flight is None
    assert not state.dead_letter_path.exists()
