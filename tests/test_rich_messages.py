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
            "guild_name": "Example Server",
            "content": content,
            "author": {"username": "Example User"},
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


def test_adaptive_compact_and_editorial_triggers():
    compact = format_event(event("short"), extracted_media_count=1)
    assert compact.style == "compact" and compact.rich_html is None
    assert compact.text.startswith("<b>Example User</b> in <b>#signals</b>")

    reply = event(); reply["message"]["referenced_message"] = {"content": "old"}
    embed = event(); embed["message"]["embeds"] = [{"title": "Card"}]
    edited = event("after"); edited["event_type"] = "EDITED"; edited["editHistory"] = [{"content": "before"}]
    triggers = [reply, embed, event("```\ncode\n```"), event("x" * 1200), edited]
    assert all(format_event(value).style == "editorial" for value in triggers)
    assert format_event(event(), extracted_media_count=2).style == "editorial"


def test_rich_html_is_escaped_and_bounded():
    formatted = format_event(rich_event())
    assert formatted.rich_html is not None
    rich = formatted.rich_html
    assert "<blockquote>quoted &lt;reply&gt;</blockquote>" in rich
    assert '<pre><code class="language-py">print(&#x27;&lt;x&gt;&#x27;)\n</code></pre>' in rich
    assert "Card &lt;title&gt;" in rich and "Embed &lt;body&gt;" in rich
    assert len(rich) <= 32768 and set(rich_tags(rich)) <= SUPPORTED_RICH_TAGS

    bounded = format_event(event("😀" * 21000)).rich_html
    assert bounded is not None and len(bounded) <= 32768
    assert len(bounded.encode("utf-16-le")) // 2 <= 32768
    assert html.unescape(re.sub(r"<[^>]+>", "", bounded))


def test_rich_request_multipart_mapping_and_limits(tmp_path: Path):
    sender = TgSender("token", httpx.AsyncClient(), StateStore(tmp_path / "state", tmp_path / "dead"), rich_messages_enabled=True)
    formatted = format_event(rich_event())
    assert formatted.rich_html is not None
    batch = sender._rich_request(Target("1", 7), formatted.rich_html, [media(0), media(1, "video")], ["https://cdn.discordapp.com/failed", "https://["])
    payload = json.loads(batch.data["rich_message"])
    assert batch.method == "sendRichMessage" and set(batch.files or {}) == {"file0", "file1"} and batch.cost == 1
    assert payload["media"] == [
        {"id": "m0", "media": {"type": "photo", "media": "attach://file0"}},
        {"id": "m1", "media": {"type": "video", "media": "attach://file1"}},
    ]
    assert '<tg-collage><img src="tg://photo?id=m0"/><video src="tg://video?id=m1"></video></tg-collage>' in payload["html"]
    assert "cdn.discordapp.com/failed" in payload["html"] and "https://[" not in payload["html"]
    assert len(payload["html"]) <= 32768 and rich_block_count(payload["html"]) <= 500

    fifty_one = [media(index) for index in range(51)]
    assert not sender._rich_eligible(formatted, fifty_one)
    with pytest.raises(ValueError):
        sender._rich_request(Target("1"), formatted.rich_html, fifty_one, [])


@pytest.mark.asyncio
async def test_config_gate_and_unsupported_document_stay_classic(tmp_path: Path):
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(200, json={"ok": True})

    formatted = format_event(rich_event())
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, StateStore(tmp_path / "off", tmp_path / "off-dead"), 1000, 1000).send_event(Envelope("off", rich_event()), [Target("1")], formatted, [], [])
        await TgSender("token", client, StateStore(tmp_path / "doc", tmp_path / "doc-dead"), 1000, 1000, rich_messages_enabled=True).send_event(Envelope("doc", rich_event()), [Target("1")], formatted, [media(0, "document")], [])
    assert paths == ["/bottoken/sendMessage", "/bottoken/sendDocument"]


@pytest.mark.asyncio
async def test_definite_rich_rejection_persists_media_before_classic(tmp_path: Path, monkeypatch):
    paths: list[str] = []
    transitions: list[str] = []
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    original = state.set_media

    async def set_media(index: int) -> None:
        await original(index)
        assert state.in_flight is not None
        transitions.append(state.in_flight["targets"][index]["phase"])

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

    async def no_sleep(_value: float) -> None:
        return None

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if isinstance(response, httpx.ConnectError):
            raise response
        return response

    state = StateStore(tmp_path / "state", tmp_path / "dead")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000, sleep=no_sleep, rich_messages_enabled=True).send_event(Envelope("c", rich_event()), [Target("1")], format_event(rich_event()), [], [])
    assert paths == ["/bottoken/sendRichMessage"] * 3
    assert state.ack == "c" and json.loads(state.dead_letter_path.read_text())["phase"] == "rich"


@pytest.mark.asyncio
async def test_persisted_rich_phase_resumes_when_gate_is_disabled(tmp_path: Path):
    envelope = Envelope("c", rich_event())
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    await state.begin(envelope, [Target("1")], "rich")
    restarted = StateStore(state.path, state.dead_letter_path)
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, restarted, 1000, 1000, rich_messages_enabled=False).send_event(envelope, [Target("1")], format_event(rich_event()), [], [])
    assert paths == ["/bottoken/sendRichMessage"] and restarted.ack == "c"
