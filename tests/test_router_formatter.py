import os
import html
import re
from importlib import import_module
from pathlib import Path

pytest = import_module("pytest")

from src.formatter import format_event, truncate_html
from src.LoggerManager import LoggerManager, logger
from src.router import Router, parse_rules


def event(content="hello", channel="c1", event_type="CREATED"):
    return {"event_type": event_type, "message": {"channel_id": channel, "guild_id": "g1", "channel_name": "<chan>", "guild_name": "A&B", "content": content, "author": {"id": "a1", "username": "<alice>"}}}


def test_routing_first_match_scalar_list_dedup_default_drop():
    router = parse_rules({"rules": [
        {"name": "drop", "match": {"channel_id": "noise"}, "action": "drop"},
        {"name": "send", "match": {"guild_id": ["g1"], "event_type": "CREATED", "author_id": "a1", "author_name": "<alice>", "keyword": "HEL"}, "forward_to": [{"chat_id": "1"}, {"chat_id": "1"}, {"chat_id": "2", "thread_id": 4}]},
    ], "default_action": "drop"})
    assert [target.key for target in router.route(event())] == ["1:", "2:4"]
    assert router.route(event(channel="noise")) == []
    assert router.route(event(event_type="DELETED")) == []


def test_router_invalid_hot_reload_keeps_old(tmp_path: Path):
    path = tmp_path / "rules.yaml"
    path.write_text("rules:\n- match: {channel_id: c1}\n  forward_to: {chat_id: '1'}\ndefault_action: drop\n")
    router = Router(path)
    assert router.route(event())[0].chat_id == "1"
    path.write_text("rules:\n- match: {keyword: '['}\n  forward_to: {chat_id: '2'}\n")
    os.utime(path, ns=(path.stat().st_atime_ns, path.stat().st_mtime_ns + 1_000_000))
    with pytest.raises(Exception):
        router.reload_if_changed()
    assert router.route(event())[0].chat_id == "1"


@pytest.mark.asyncio
async def test_router_watcher_reloads_without_message(tmp_path: Path):
    path = tmp_path / "rules.yaml"
    path.write_text("rules: []\ndefault_action: drop\n")
    router = Router(path)
    stop = __import__("asyncio").Event()
    task = __import__("asyncio").create_task(router.watch(stop))
    path.write_text("rules:\n- match: {channel_id: c1}\n  forward_to: {chat_id: '9'}\ndefault_action: drop\n")
    os.utime(path, ns=(path.stat().st_atime_ns, path.stat().st_mtime_ns + 1_000_000))
    await __import__("asyncio").sleep(1.05)
    stop.set()
    await task
    assert router.route(event())[0].chat_id == "9"


def test_formatter_escapes_edit_history_reply_and_bounds():
    value = event("new <b>", event_type="EDITED")
    value["editHistory"] = [{"content": "old & bad"}]
    value["message"]["referencedMessage"] = {"resolved": {"content": "quoted <x>"}}
    formatted = format_event(value)
    assert "#&lt;chan&gt;" in formatted.text
    assert "A&amp;B" in formatted.text
    assert "&lt;alice&gt;" in formatted.text
    assert "old &amp; bad" in formatted.text and "new &lt;b&gt;" in formatted.text
    assert "quoted &lt;x&gt;" in formatted.text
    assert len(formatted.text) <= 4096 and len(formatted.caption) <= 1024


def visible_utf16_units(value):
    visible = html.unescape(re.sub(r"<[^>]+>", "", value))
    return len(visible.encode("utf-16-le", errors="replace")) // 2


def test_realistic_upstream_shape_uses_id_fallbacks_and_renders_stickers():
    value = {
        "schema_version": 1,
        "event_type": "CREATED",
        "captured_at": "2026-07-19T12:00:00.000Z",
        "message": {
            "id": "message-1",
            "channel_id": "channel-123",
            "guildId": "guild-456",
            "content": "collector payload",
            "author": {"id": "author-1", "username": "collector-user"},
            "sticker_items": [
                {"id": "1", "name": "wave <hello>", "format_type": 1},
                {"id": "2", "name": "animated", "formatType": 2},
                {"id": "3", "name": "lottie", "format_type": 3},
                {"id": "4", "name": "gif", "format_type": 4},
            ],
            "attachments": [{"url": "https://cdn.discordapp.com/attachments/a/b.png", "filename": "b.png"}],
        },
    }
    text = format_event(value).text
    assert "#channel-123" in text and "@ guild-456" in text and "@ DM" not in text
    assert "collector-user" in text and "wave &lt;hello&gt;" in text
    assert "https://cdn.discordapp.com/stickers/1.png" in text
    assert "https://cdn.discordapp.com/stickers/2.png" in text
    assert "https://cdn.discordapp.com/stickers/3.json" in text
    assert "https://media.discordapp.net/stickers/4.gif" in text


def test_camel_case_sticker_items_and_unknown_format_have_safe_link_fallback():
    value = event("x")
    value["message"]["stickerItems"] = [{"id": "9", "name": "mystery", "formatType": 999}]
    assert 'href="https://cdn.discordapp.com/stickers/9.png"' in format_event(value).text


@pytest.mark.parametrize("alias", ["reference_snake", "reference_camel", "message_reference", "messageReference"])
def test_nested_reply_aliases(alias):
    value = event("current")
    if alias == "reference_snake": value["message"]["reference"] = {"referenced_message": {"content": "nested <reply>"}}
    if alias == "reference_camel": value["message"]["reference"] = {"referencedMessage": {"content": "nested <reply>"}}
    if alias == "message_reference": value["message"]["message_reference"] = {"resolved": {"content": "nested <reply>"}}
    if alias == "messageReference": value["message"]["messageReference"] = {"resolved": {"content": "nested <reply>"}}
    assert "nested &lt;reply&gt;" in format_event(value).text


def test_html_truncation_balances_tags_and_entities_at_all_risky_components():
    value = event("<&>" * 3000, event_type="EDITED")
    value["editHistory"] = [{"content": "old & <tag>" * 1000}]
    value["message"]["referenced_message"] = {"content": "reply <&>" * 1000}
    formatted = format_event(value)
    for text, limit in ((formatted.text, 4096), (formatted.caption, 1024)):
        assert limit - 1 <= visible_utf16_units(text) <= limit
        assert text.count("<b>") == text.count("</b>")
        assert text.count("<blockquote>") == text.count("</blockquote>")
        assert not text.endswith("&")


def test_truncate_html_counts_astral_characters_as_two_utf16_units():
    value = truncate_html("😀" * 6, 9)
    assert value == "😀" * 4 + "…"
    assert visible_utf16_units(value) == 9


def test_truncate_html_counts_entities_as_decoded_visible_characters():
    value = truncate_html("<b>" + "&amp;" * 20 + "</b>", 8)
    assert value == "<b>" + "&amp;" * 7 + "…</b>"
    assert html.unescape(re.sub(r"<[^>]+>", "", value)) == "&" * 7 + "…"
    assert visible_utf16_units(value) == 8


def test_long_anchor_source_length_consumes_no_visible_budget():
    href = "https://example.com/" + "path/" * 2000
    value = truncate_html(f'<a href="{href}">abcdefghijk</a>', 8)
    assert value == f'<a href="{href}">abcdefg…</a>'
    assert visible_utf16_units(value) == 8


def test_lone_surrogate_is_replaced_in_output_without_crash():
    value = truncate_html("a\ud800bcdef", 5)
    assert value == "a�bc…"
    assert visible_utf16_units(value) == 5
    assert value.encode("utf-8")


def test_valid_surrogate_pair_normalizes_to_astral_scalar():
    value = truncate_html("x\ud83d\ude00y", 4)
    assert value == "x😀y"
    assert visible_utf16_units(value) == 4
    assert value.encode("utf-8")


def test_caption_boundary_never_exceeds_telegram_utf16_limit_and_keeps_html_valid():
    value = event("😀&" * 800)
    caption = format_event(value).caption
    assert visible_utf16_units(caption) == 1024
    assert caption.count("<b>") == caption.count("</b>")
    assert not caption.endswith("&")


@pytest.mark.parametrize("event_type,icon", [("CREATED", "🆕"), ("EDITED", "✏️"), ("DELETED", "🗑️"), ("GHOST_PINGED", "👻")])
def test_independent_event_notifications(event_type, icon):
    assert format_event(event(event_type=event_type)).text.startswith(icon)


def test_logger_monitor_icons_exact():
    LoggerManager.configure()
    assert logger.level("DEBUG").icon == "*️⃣DDDEBUG"
    assert logger.level("INFO").icon == "ℹ️IIIINFO"
    assert logger.level("SUCCESS").icon == "✅SUCCESS"
    assert logger.level("WARNING").icon == "⚠️WARNING"
    assert logger.level("ERROR").icon == "⭕EEERROR"
