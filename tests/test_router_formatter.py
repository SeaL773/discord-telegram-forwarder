import os
import html
import re
from importlib import import_module
from pathlib import Path

pytest = import_module("pytest")

from src.formatter import add_fallbacks, format_event, truncate_html
from src.models import FormattedMessage
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


@pytest.mark.parametrize("forward_to", [
    {},
    {"thread_id": 1},
    {"chat_id": ""},
    {"chat_id": "   "},
    {"chat_id": " 1"},
    {"chat_id": "1 "},
    {"chat_id": True},
    {"chat_id": 1.5},
    {"chat_id": float("nan")},
    {"chat_id": []},
    {"chat_id": {}},
    {"chat_id": "1", "thread_id": True},
    {"chat_id": "1", "thread_id": "2"},
    {"chat_id": "1", "thread_id": -1},
    {"chat_id": "1", "thread_id": []},
])
def test_target_parser_rejects_missing_or_invalid_target_fields_as_value_error(forward_to):
    with pytest.raises(ValueError):
        parse_rules({"rules": [{"match": {}, "forward_to": forward_to}], "default_action": "drop"})


def test_target_parser_accepts_nonempty_scalar_chat_id_and_nonnegative_integer_thread():
    snapshot = parse_rules({"rules": [{"match": {}, "forward_to": [123, {"chat_id": "-100", "thread_id": 0}]}], "default_action": "drop"})
    assert [(target.chat_id, target.thread_id) for target in snapshot.route(event())] == [("123", None), ("-100", 0)]


@pytest.mark.parametrize("default_action", [
    {},
    {"forward_to": None},
    {"forward_to": []},
    {"forward_to": {}},
    {"unknown": "value"},
    {"forward_to": {"chat_id": "1"}, "unknown": "value"},
])
def test_default_forward_requires_exactly_one_nonempty_forward_to_field(default_action):
    with pytest.raises(ValueError):
        parse_rules({"rules": [], "default_action": default_action})


def test_default_forward_accepts_valid_targets():
    snapshot = parse_rules({"rules": [], "default_action": {"forward_to": [{"chat_id": "1"}, {"chat_id": "2", "thread_id": 0}]}})
    assert [target.key for target in snapshot.route(event())] == ["1:", "2:0"]


@pytest.mark.asyncio
async def test_router_watcher_survives_invalid_target_reload_then_accepts_valid_reload(tmp_path: Path):
    path = tmp_path / "rules.yaml"
    path.write_text("rules:\n- match: {channel_id: c1}\n  forward_to: {chat_id: '1'}\ndefault_action: drop\n")
    router = Router(path)
    stop = __import__("asyncio").Event()
    task = __import__("asyncio").create_task(router.watch(stop))
    path.write_text("rules:\n- match: {channel_id: c1}\n  forward_to: {thread_id: 2}\ndefault_action: drop\n")
    os.utime(path, ns=(path.stat().st_atime_ns, path.stat().st_mtime_ns + 1_000_000))
    await __import__("asyncio").sleep(1.05)
    assert not task.done()
    assert router.route(event())[0].chat_id == "1"
    path.write_text("rules:\n- match: {channel_id: c1}\n  forward_to: {chat_id: '9', thread_id: 0}\ndefault_action: drop\n")
    os.utime(path, ns=(path.stat().st_atime_ns, path.stat().st_mtime_ns + 1_000_000))
    await __import__("asyncio").sleep(1.05)
    stop.set()
    await task
    assert router.route(event())[0].key == "9:0"


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


def test_collector_top_level_names_override_message_id_fallbacks():
    value = event("collector payload")
    value["message"].pop("channel_name")
    value["message"].pop("guild_name")
    value["channel_name"] = "实时市场消息提醒"
    value["guild_name"] = "猫猫炒美股"
    text = format_event(value).text
    assert "#实时市场消息提醒" in text
    assert "@ 猫猫炒美股" in text
    assert "#channel" not in text


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
        assert text.count("<i>") == text.count("</i>")
        assert text.count("<blockquote>") == text.count("</blockquote>")
        assert not text.endswith("&")


def test_discord_markdown_headings_bold_italic_and_html_are_safe():
    value = event(
        "📈 ***Jen’s SPX Levels - Monday 7/20***\n"
        "### Market Context\n"
        "Market <structure> shifted.\n"
        "### 🔹Bias zone 7470-7480\n"
        "*Let the candles tell the story.*\n"
        "**Educational only.**"
    )
    formatted = format_event(value)
    assert "<b><i>Jen’s SPX Levels - Monday 7/20</i></b>" in formatted.text
    assert "<b>Market Context</b>" in formatted.text
    assert "<b>🔹Bias zone 7470-7480</b>" in formatted.text
    assert "<i>Let the candles tell the story.</i>" in formatted.text
    assert "<b>Educational only.</b>" in formatted.text
    assert "Market &lt;structure&gt; shifted." in formatted.text
    assert "###" not in formatted.text and "***" not in formatted.text


def test_markdown_tags_remain_balanced_at_utf16_truncation_boundary():
    value = event("### Heading\n***" + "😀" * 800 + "***")
    caption = format_event(value).caption
    assert visible_utf16_units(caption) <= 1024
    assert caption.count("<b>") == caption.count("</b>")
    assert caption.count("<i>") == caption.count("</i>")


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


def test_unhashable_sticker_format_is_safely_normalized():
    value = event("sticker")
    value["message"]["sticker_items"] = [{"id": "123", "name": "odd", "format_type": []}]
    assert "odd" in format_event(value).text


def test_caption_boundary_never_exceeds_telegram_utf16_limit_and_keeps_html_valid():
    value = event("😀&" * 800)
    caption = format_event(value).caption
    assert 1022 <= visible_utf16_units(caption) <= 1024
    assert caption.count("<b>") == caption.count("</b>")
    assert not caption.endswith("&")


def test_fallback_budget_preserves_complete_first_anchor_at_full_limits():
    urls = ["https://example.com/" + "a" * 5000, "https://example.com/second"]
    result = add_fallbacks(FormattedMessage("😀" * 3000, "😀" * 1000), urls)
    for value, limit in ((result.text, 4096), (result.caption, 1024)):
        assert visible_utf16_units(value) <= limit
        assert f'href="{urls[0]}"' in value
        assert value.count("<a href=") == value.count("</a>")
        assert not value.endswith("&")


def test_fallback_suffix_exactly_consuming_budget_does_not_add_ellipsis():
    urls = [f"https://example.com/{index}" for index in range(91)] + ["https://["]
    result = add_fallbacks(FormattedMessage("visible", "visible"), urls)
    assert visible_utf16_units(result.caption) == 1024
    assert not result.caption.startswith("…")
    assert "visible" not in result.caption
    assert result.caption.count("<a href=") == 91
    assert result.caption.endswith("Attachment unavailable")


@pytest.mark.parametrize("url", [
    "https://[",
    "ftp://example.com/file",
    "https://example.com/a b",
    "https://example.com/a\nnext",
    "https://example.com/a\x00b",
    "https://example.com/über",
])
def test_invalid_fallback_urls_degrade_to_fixed_plain_text_without_href(url):
    result = add_fallbacks(FormattedMessage("valid <b>message</b>", "caption"), [url])
    for value in (result.text, result.caption):
        assert value.endswith("\nAttachment unavailable")
        assert "<a href=" not in value
        assert "valid <b>message</b>" in result.text


def test_mixed_fallback_urls_keep_valid_links_dedupe_and_isolate_invalid_values():
    valid = "https://example.com/a?x=1&y=2"
    result = add_fallbacks(FormattedMessage("body", "caption"), ["https://[", valid, valid, "javascript:alert(1)"])
    assert result.text.count("Attachment unavailable") == 2
    assert result.text.count("<a href=") == 1
    assert f'href="{html.escape(valid, quote=True)}"' in result.text
    assert result.text.count("</a>") == 1


def test_verified_runtime_embed_shape_renders_text_without_duplicates():
    value = event("@everyone\n\n@所有人")
    value["message"]["embeds"] = [{
        "type": "rich",
        "author": {"name": "Posted"},
        "title": "Market update",
        "url": "https://example.com/post?a=1&b=2",
        "description": "双语 description <safe>",
        "fields": [{"name": "Label & key", "value": "Value <one>"}],
        "footer": {"text": "Footer > end"},
        "image": {"url": "https://pbs.twimg.com/media/example.jpg"},
    }]
    formatted = format_event(value)
    assert formatted.text.count("@everyone\n\n@所有人") == 1
    assert "Posted" in formatted.text
    assert '<a href="https://example.com/post?a=1&amp;b=2">Market update</a>' in formatted.text
    assert "双语 description &lt;safe&gt;" in formatted.text
    assert "<b>Label &amp; key</b>\nValue &lt;one&gt;" in formatted.text
    assert "Footer &gt; end" in formatted.text
    assert ">Source</a>" not in formatted.text
    assert "pbs.twimg.com" not in formatted.text


def test_embed_source_link_validation_escaping_and_repeated_components():
    value = event("same")
    value["message"]["embeds"] = [
        {"author": {"name": "same"}, "title": "same", "description": "unique", "url": "https://example.com/?q=\"<&"},
        {"description": "unique", "footer": {"text": "footer"}, "url": "https://user:pass@example.com/private"},
        {"description": "third", "url": "https://example.org/source"},
    ]
    text = format_event(value).text
    assert text.count("same") == 1
    assert text.count("unique") == 1
    assert "footer" in text and "third" in text
    assert 'href="https://example.com/?q=&quot;&lt;&amp;"' in text
    assert "user:pass" not in text
    assert '<a href="https://example.org/source">Source</a>' in text


def test_embed_dict_shape_caps_embeds_fields_and_keeps_caption_utf16_safe():
    capped_embeds = {}
    for index in range(12):
        capped_embeds[str(index)] = {
            "title": f"Title {index}",
            "fields": [{"name": f"Field {field}", "value": f"v{index}-{field}"} for field in range(30)],
        }
    value = event("")
    value["message"]["embeds"] = capped_embeds
    formatted = format_event(value)
    assert "Title 9" in formatted.text and "Title 10" not in formatted.text
    assert "Field 24" in formatted.text and "Field 25" not in formatted.text

    boundary = event("")
    boundary["message"]["embeds"] = [{"title": "Boundary", "description": "😀&" * 800}]
    caption = format_event(boundary).caption
    assert 1022 <= visible_utf16_units(caption) <= 1024
    assert caption.count("<b>") == caption.count("</b>")


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
