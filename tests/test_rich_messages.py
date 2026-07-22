import html
import json
import re
from pathlib import Path
from typing import Any

import httpx
import pytest

from src.formatter import format_event
from src.models import Attachment, DownloadedMedia, Envelope, Target
from src.state import StateStore
from src.tg_sender import TgSender


RICH_BLOCK_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6", "p", "pre", "footer", "hr", "blockquote", "img", "video", "tg-collage"}
SUPPORTED_RICH_TAGS = RICH_BLOCK_TAGS | {"a", "b", "i", "code"}


def rich_tags(value: str) -> list[str]:
    return [match.group(1).lower() for match in re.finditer(r"</?([a-z][a-z0-9-]*)\b", value, re.IGNORECASE)]


def rich_block_count(value: str) -> int:
    return sum(tag in RICH_BLOCK_TAGS for tag in rich_tags(value)) - sum(
        match.group(1).lower() in RICH_BLOCK_TAGS
        for match in re.finditer(r"</([a-z][a-z0-9-]*)>", value, re.IGNORECASE)
    )


def event(content: str = "hello") -> dict[str, Any]:
    return {
        "event_type": "CREATED",
        "message": {
            "channel_id": "channel",
            "channel_name": "signals",
            "guild_name": "Market Desk",
            "content": content,
            "author": {"username": "Alice"},
        },
    }


def media(index: int, kind: str = "photo") -> DownloadedMedia:
    content_type = "image/png" if kind == "photo" else "video/mp4" if kind == "video" else "application/pdf"
    suffix = "png" if kind == "photo" else "mp4" if kind == "video" else "pdf"
    return DownloadedMedia(Attachment(f"https://cdn.discordapp.com/{index}.{suffix}", f"{index}.{suffix}"), f"data-{index}".encode(), content_type, kind)


def rich_event() -> dict[str, Any]:
    value = event("Paragraph <safe>\n\n```py\nprint('<x>')\n```")
    message = value["message"]
    assert isinstance(message, dict)
    message["referenced_message"] = {"content": "quoted <reply>"}
    message["embeds"] = [{"title": "Card <title>", "description": "Embed <body>", "fields": [{"name": "Risk", "value": "Low <high>"}]}]
    return value


def test_all_messages_generate_editorial_rich_html():
    short = format_event(event("short"), extracted_media_count=1)
    assert short.style == "editorial" and short.rich_html is not None
    assert short.text.startswith("<b>Alice</b> in <b>#signals</b>")
    assert short.rich_html.startswith("<h3>#signals</h3><p>short</p>")
    assert "🆕" not in short.text and "👤" not in short.text and "🏷️" not in short.text

    triggers = []
    reply = event(); reply["message"]["referenced_message"] = {"content": "old"}; triggers.append(reply)
    embed = event(); embed["message"]["embeds"] = [{"title": "Card"}]; triggers.append(embed)
    triggers.append(event("```\ncode\n```"))
    triggers.append(event("x" * 1200))
    edited = event("after"); edited["event_type"] = "EDITED"; edited["editHistory"] = [{"content": "before"}]; triggers.append(edited)
    assert all(format_event(value).style == "editorial" for value in triggers)
    assert format_event(event(), extracted_media_count=2).style == "editorial"


@pytest.mark.asyncio
async def test_short_message_uses_rich_when_enabled(tmp_path: Path):
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"ok": True})

    formatted = format_event(event("short"))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = TgSender("token", client, StateStore(tmp_path / "state", tmp_path / "dead"), 1000, 1000, rich_messages_enabled=True)
        await sender.send_event(Envelope("short", event("short")), [Target("1")], formatted, [], [])

    assert [request.url.path for request in requests] == ["/bottoken/sendRichMessage"]


def test_rich_html_escapes_reply_code_embed_and_is_structurally_bounded():
    formatted = format_event(rich_event())
    assert formatted.rich_html is not None
    rich = formatted.rich_html
    assert "<blockquote>quoted &lt;reply&gt;</blockquote>" in rich
    assert '<pre><code class="language-py">print(&#x27;&lt;x&gt;&#x27;)\n</code></pre>' in rich
    assert "Card &lt;title&gt;" in rich and "Embed &lt;body&gt;" in rich and "Low &lt;high&gt;" in rich
    assert "<table" not in rich and len(rich) <= 32768

    huge = event("x" * 40000 + "```\n<code>\n```")
    bounded = format_event(huge).rich_html
    assert bounded is not None and len(bounded) <= 32768
    assert set(rich_tags(bounded)) <= SUPPORTED_RICH_TAGS
    assert html.unescape(re.sub(r"<[^>]+>", "", bounded))

    emoji_bounded = format_event(event("😀" * 21000)).rich_html
    assert emoji_bounded is not None
    assert len(emoji_bounded) <= 32768
    assert len(emoji_bounded.encode("utf-16-le")) // 2 <= 32768
    assert set(rich_tags(emoji_bounded)) <= SUPPORTED_RICH_TAGS
    assert emoji_bounded.encode("utf-16-le").decode("utf-16-le") == emoji_bounded


@pytest.mark.parametrize("event_type,label", [
    ("CREATED", "Market Desk"),
    ("EDITED", "Market Desk · Edited"),
    ("DELETED", "Market Desk · Deleted"),
    ("GHOST_PINGED", "Market Desk · Ghost ping"),
])
def test_restrained_event_metadata_labels(event_type: str, label: str):
    compact_event = event("short")
    compact_event["event_type"] = event_type
    compact = format_event(compact_event)
    assert compact.text.endswith(f"<i>{label}</i>")
    assert "created" not in compact.text

    editorial_event = rich_event()
    editorial_event["event_type"] = event_type
    editorial = format_event(editorial_event)
    assert editorial.rich_html is not None
    assert editorial.rich_html.startswith("<h3>#signals</h3>")
    assert f"<footer><i>Alice · {label}</i></footer>" in editorial.rich_html
    assert "created" not in editorial.rich_html


def test_rich_request_no_media_and_collage_multipart_mapping(tmp_path: Path):
    sender = TgSender("token", httpx.AsyncClient(), StateStore(tmp_path / "state", tmp_path / "dead"), rich_messages_enabled=True)
    formatted = format_event(rich_event())
    assert formatted.rich_html is not None
    plain = sender._rich_request(Target("1", 7), formatted.rich_html, [], [])
    assert plain.method == "sendRichMessage" and plain.files is None and plain.cost == 1
    payload = json.loads(plain.data["rich_message"])
    assert set(payload) == {"html"} and "media" not in payload

    batch = sender._rich_request(Target("1"), formatted.rich_html, [media(0), media(1, "video")], ["https://cdn.discordapp.com/failed", "https://["])
    payload = json.loads(batch.data["rich_message"])
    assert batch.method == "sendRichMessage" and set(batch.files or {}) == {"file0", "file1"} and batch.cost == 1
    assert payload["media"] == [
        {"id": "m0", "media": {"type": "photo", "media": "attach://file0"}},
        {"id": "m1", "media": {"type": "video", "media": "attach://file1"}},
    ]
    assert '<tg-collage><img src="tg://photo?id=m0"/><video src="tg://video?id=m1"></video></tg-collage>' in payload["html"]
    assert "cdn.discordapp.com/failed" in payload["html"] and "https://[" not in payload["html"]
    assert len(payload["html"]) <= 32768


@pytest.mark.asyncio
async def test_eleven_visual_media_use_one_rich_request_with_cost_one(tmp_path: Path):
    requests: list[httpx.Request] = []
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"ok": True})
    visual_media = [media(index) for index in range(11)]
    formatted = format_event(event(), extracted_media_count=len(visual_media))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        state = StateStore(tmp_path / "state", tmp_path / "dead")
        sender = TgSender("token", client, state, 1, 1, rich_messages_enabled=True)
        batch = sender._rich_request(Target("1"), formatted.rich_html or "", visual_media, [])
        assert batch.cost == 1
        await sender.send_event(Envelope("c", event()), [Target("1")], formatted, visual_media, [])
    assert [request.url.path for request in requests] == ["/bottoken/sendRichMessage"]


def test_rich_message_enforces_block_and_media_limits(tmp_path: Path):
    sender = TgSender("token", httpx.AsyncClient(), StateStore(tmp_path / "state", tmp_path / "dead"), rich_messages_enabled=True)
    dense = format_event(event("\n\n".join(f"line {index}" for index in range(501))))
    assert dense.rich_html is not None
    assert rich_block_count(dense.rich_html) <= 500
    assert set(rich_tags(dense.rich_html)) <= SUPPORTED_RICH_TAGS

    near_limit = format_event(event("\n\n".join(f"line {index}" for index in range(498))))
    assert near_limit.rich_html is not None
    batch = sender._rich_request(Target("1"), near_limit.rich_html, [media(0), media(1)], [])
    payload = json.loads(batch.data["rich_message"])
    assert rich_block_count(payload["html"]) <= 500

    fifty = [media(index) for index in range(50)]
    fifty_one = [media(index) for index in range(51)]
    assert sender._rich_eligible(near_limit, fifty)
    assert not sender._rich_eligible(near_limit, fifty_one)
    with pytest.raises(ValueError):
        sender._rich_request(Target("1"), near_limit.rich_html, fifty_one, [])


@pytest.mark.asyncio
async def test_config_gate_and_unsupported_document_stay_classic(tmp_path: Path):
    paths: list[str] = []
    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(200, json={"ok": True})
    formatted = format_event(rich_event())
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, StateStore(tmp_path / "off", tmp_path / "off-dead"), 1000, 1000, rich_messages_enabled=False).send_event(Envelope("off", rich_event()), [Target("1")], formatted, [], [])
        await TgSender("token", client, StateStore(tmp_path / "doc", tmp_path / "doc-dead"), 1000, 1000, rich_messages_enabled=True).send_event(Envelope("doc", rich_event()), [Target("1")], formatted, [media(0, "document")], [])
    assert paths == ["/bottoken/sendMessage", "/bottoken/sendDocument"]


@pytest.mark.asyncio
async def test_rich_success_one_call_and_definite_rejection_persists_media_before_classic(tmp_path: Path, monkeypatch):
    paths: list[str] = []
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    transitions: list[str] = []
    original = state.set_media
    async def set_media(index: int) -> None:
        await original(index)
        inflight = state.in_flight
        assert inflight is not None
        transitions.append(inflight["targets"][index]["phase"])
    monkeypatch.setattr(state, "set_media", set_media)
    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path.endswith("sendRichMessage"):
            return httpx.Response(400, json={"ok": False, "error_code": 400})
        assert transitions == ["media"]
        return httpx.Response(200, json={"ok": True})
    formatted = format_event(rich_event())
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000, rich_messages_enabled=True).send_event(Envelope("c", rich_event()), [Target("1")], formatted, [media(0), media(1)], [])
    assert paths == ["/bottoken/sendRichMessage", "/bottoken/sendMediaGroup"] and state.ack == "c"


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [httpx.Response(500), httpx.Response(200, text="not-json"), httpx.ConnectError("network", request=httpx.Request("POST", "https://api.telegram.org"))])
async def test_ambiguous_rich_errors_retry_only_rich_then_dead_letter(tmp_path: Path, response: httpx.Response | httpx.ConnectError):
    paths: list[str] = []
    async def no_sleep(_value: float) -> None: return None
    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if isinstance(response, httpx.ConnectError):
            raise response
        return response
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    formatted = format_event(rich_event())
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000, sleep=no_sleep, rich_messages_enabled=True).send_event(Envelope("c", rich_event()), [Target("1")], formatted, [], [])
    assert paths == ["/bottoken/sendRichMessage"] * 3
    assert state.ack == "c" and json.loads(state.dead_letter_path.read_text())["phase"] == "rich"


@pytest.mark.asyncio
async def test_rich_phase_crash_recovery_resumes_with_gate_off_and_legacy_state_loads(tmp_path: Path):
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    envelope = Envelope("c", rich_event())
    await state.begin(envelope, [Target("1")], "rich")
    await state.retry(0, rich=True)
    restarted = StateStore(state.path, state.dead_letter_path)
    paths: list[str] = []
    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(200, json={"ok": True})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, restarted, 1000, 1000, rich_messages_enabled=False).send_event(envelope, [Target("1")], format_event(rich_event()), [], [])
    assert paths == ["/bottoken/sendRichMessage"] and restarted.ack == "c"

    legacy = StateStore(tmp_path / "legacy", tmp_path / "legacy-dead")
    await legacy.begin(Envelope("legacy", event()), [Target("1")])
    raw = json.loads(legacy.path.read_text())
    raw["in_flight"]["targets"][0].pop("rich_retries")
    legacy.path.write_text(json.dumps(raw), encoding="utf-8")
    legacy_inflight = StateStore(legacy.path, legacy.dead_letter_path).in_flight
    assert legacy_inflight is not None and legacy_inflight["targets"][0]["phase"] == "media"


@pytest.mark.asyncio
async def test_rich_401_dead_letters_without_classic_and_logs_never_receive_body(tmp_path: Path, monkeypatch):
    logged: list[str] = []
    monkeypatch.setattr("src.tg_sender.log_error", logged.append)
    paths: list[str] = []
    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(401, json={"ok": False, "error_code": 401, "description": "secret body https://secret"})
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000, rich_messages_enabled=True).send_event(Envelope("c", rich_event()), [Target("1")], format_event(rich_event()), [], [])
    assert paths == ["/bottoken/sendRichMessage"] and logged == []
