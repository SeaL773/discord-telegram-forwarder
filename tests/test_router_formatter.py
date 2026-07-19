import os
from importlib import import_module
from pathlib import Path

pytest = import_module("pytest")

from src.formatter import format_event
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
        assert len(text) <= limit
        assert text.count("<b>") == text.count("</b>")
        assert text.count("<blockquote>") == text.count("</blockquote>")
        assert not text.endswith("&")


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
