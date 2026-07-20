import importlib.util
import json
from importlib import import_module
from pathlib import Path

import httpx
import yaml


pytest = import_module("pytest")


SCRIPT_PATH = Path(__file__).parents[1] / ".local" / "sync_topics.py"


def load_script():
    spec = importlib.util.spec_from_file_location("sync_topics_under_test", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def configure_paths(module, tmp_path: Path) -> None:
    module.LOCAL_DIR = tmp_path
    module.MAP_PATH = tmp_path / "topic-map.json"
    module.RULES_PATH = tmp_path / "rules.yaml"
    module.PENDING_PATH = tmp_path / "topic-create-pending.json"


def channel(channel_id="100", name="Alerts"):
    return {"id": channel_id, "name": name, "parent_name": "Market", "status": "ok"}


def test_rule_preferences_preserve_legacy_true_manual_false_and_safe_new_default(tmp_path: Path):
    module = load_script()
    configure_paths(module, tmp_path)
    module.RULES_PATH.write_text(yaml.safe_dump({"rules": [
        {"match": {"channel_id": "100"}, "action": "forward", "forward_to": {"chat_id": "-1", "thread_id": 7}},
        {"enabled": False, "match": {"channel_id": "200"}, "action": "forward", "forward_to": {"chat_id": "-1", "thread_id": 8}},
    ]}), encoding="utf-8")

    existing = module.load_enabled()
    assert existing == {"100": True, "200": False}
    assert module.resolve_enabled({"100", "200", "300", "400"}, {"300": 9}, existing) == {
        "100": True,
        "200": False,
        "300": True,
        "400": False,
    }


def test_atomic_write_fsyncs_parent_after_replace(tmp_path: Path, monkeypatch):
    module = load_script()
    events = []
    original_fsync = module.os.fsync
    original_replace = module.os.replace

    def fsync(fd):
        events.append("fsync")
        return original_fsync(fd)

    def replace(source, target):
        events.append("replace")
        return original_replace(source, target)

    monkeypatch.setattr(module.os, "fsync", fsync)
    monkeypatch.setattr(module.os, "replace", replace)
    module.atomic_write(tmp_path / "state.json", "{}\n")

    assert events == ["fsync", "replace", "fsync"]


def test_save_rules_keeps_disabled_mapping_and_does_not_require_topic_for_new_disabled_channel(tmp_path: Path):
    module = load_script()
    configure_paths(module, tmp_path)
    channels = [channel("100"), channel("200", "New")]
    module.save_rules(channels, {"100": 7}, {"100": False, "200": False})

    rules = yaml.safe_load(module.RULES_PATH.read_text(encoding="utf-8"))["rules"]
    assert rules[0]["enabled"] is False
    assert rules[0]["forward_to"]["thread_id"] == 7
    assert rules[1]["enabled"] is False
    assert rules[1]["action"] == "drop"
    assert "forward_to" not in rules[1]
    assert rules[1]["channel_name"] == "Market／New"


@pytest.mark.parametrize("raw", [
    [],
    {"0100": 7},
    {"100": True},
    {"100": 1},
    {"100": 7, "200": 7},
])
def test_load_mapping_rejects_invalid_or_duplicate_topic_identifiers(tmp_path: Path, raw):
    module = load_script()
    configure_paths(module, tmp_path)
    module.MAP_PATH.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(RuntimeError, match="topic map"):
        module.load_mapping()


@pytest.mark.asyncio
async def test_new_unmapped_channel_stays_disabled_without_telegram_request(tmp_path: Path, monkeypatch):
    module = load_script()
    configure_paths(module, tmp_path)
    monkeypatch.setattr(module, "load_catalog", lambda: [channel()])

    calls = []

    async def no_telegram(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(module, "telegram_call", no_telegram)
    await module.main()

    rule = yaml.safe_load(module.RULES_PATH.read_text(encoding="utf-8"))["rules"][0]
    assert calls == []
    assert rule["enabled"] is False and rule["action"] == "drop"
    assert json.loads(module.MAP_PATH.read_text(encoding="utf-8")) == {}


@pytest.mark.asyncio
async def test_pending_create_blocks_duplicate_after_api_success_before_mapping_persist(tmp_path: Path, monkeypatch):
    module = load_script()
    configure_paths(module, tmp_path)
    item = channel()
    monkeypatch.setattr(module, "load_catalog", lambda: [item])
    module.RULES_PATH.write_text(yaml.safe_dump({"rules": [{
        "enabled": True,
        "match": {"channel_id": "100"},
        "action": "forward",
        "forward_to": {"chat_id": "-1", "thread_id": 7},
    }]}), encoding="utf-8")
    calls = 0

    async def created(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return {"message_thread_id": 9}

    monkeypatch.setattr(module, "telegram_call", created)
    monkeypatch.setattr(module, "save_mapping", lambda _mapping: (_ for _ in ()).throw(OSError("disk full")))

    with pytest.raises(OSError, match="disk full"):
        await module.main()
    assert calls == 1 and module.PENDING_PATH.exists()

    with pytest.raises(RuntimeError, match="reconcile Telegram"):
        await module.main()
    assert calls == 1

    module.MAP_PATH.write_text(json.dumps({"100": 9}), encoding="utf-8")
    module.recover_pending(module.load_mapping())
    assert not module.PENDING_PATH.exists()


@pytest.mark.asyncio
async def test_local_telegram_retry_is_finite_and_caps_retry_after(monkeypatch):
    module = load_script()
    waits = []
    attempts = 0

    class Client:
        async def post(self, *_args, **_kwargs):
            nonlocal attempts
            attempts += 1
            return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 10**9}})

    async def sleep(value):
        waits.append(value)

    monkeypatch.setenv("TG_BOT_TOKEN", "synthetic")
    monkeypatch.setattr(module.asyncio, "sleep", sleep)
    with pytest.raises(RuntimeError):
        await module.telegram_call(Client(), "editForumTopic", {})
    assert attempts == module.MAX_ATTEMPTS
    assert waits == [module.MAX_RETRY_AFTER_S, module.MAX_RETRY_AFTER_S]
