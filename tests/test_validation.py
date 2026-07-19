from pathlib import Path
from importlib import import_module

pytest = import_module("pytest")

from src.config import load_config
from src.models import Envelope
from src.router import parse_rules


def valid_frame():
    return {"type": "event", "cursor": "c", "event": {"schema_version": 1, "event_type": "CREATED", "captured_at": "now", "message": {"id": "m", "channel_id": "ch"}}}


@pytest.mark.parametrize("mutation", [
    lambda f: f["event"].update(schema_version=2),
    lambda f: f["event"].update(event_type="OTHER"),
    lambda f: f["event"].update(captured_at=""),
    lambda f: f["event"]["message"].update(id=""),
    lambda f: f["event"]["message"].update(channel_id=""),
])
def test_strict_envelope_validation(mutation):
    frame = valid_frame()
    mutation(frame)
    with pytest.raises(ValueError):
        Envelope.from_frame(frame)


def test_config_range_validation(tmp_path: Path):
    path = tmp_path / "config.yaml"
    path.write_text("telegram: {rate_limit_global_per_s: 31}\n")
    with pytest.raises(ValueError):
        load_config(path)


@pytest.mark.parametrize("raw", [
    {"rules": [{"match": {"unknown": "x"}, "action": "drop"}]},
    {"rules": [{"match": {"is_dm": "yes"}, "action": "drop"}]},
    {"rules": [{"match": {}, "action": "drop", "forward_to": {"chat_id": "1"}}]},
])
def test_rule_validation(raw):
    with pytest.raises(ValueError):
        parse_rules(raw)


def test_global_name_author_alias():
    rules = parse_rules({"rules": [{"match": {"author_name": "Global"}, "forward_to": {"chat_id": "1"}}], "default_action": "drop"})
    assert rules.route({"event_type": "CREATED", "message": {"author": {"globalName": "Global"}}})


def test_docs_and_config_consistency():
    root = Path(__file__).parents[1]
    config = (root / "config.yaml").read_text()
    readme = (root / "README.md").read_text()
    architecture = (root / "ARCHITECTURE.md").read_text()
    env_example = (root / ".env.example").read_text()
    assert "host: 127.0.0.1" in config
    assert "media_max_attachments: 20" in config and "prepared_queue_size: 4" in config
    assert "host.docker.internal" in readme and "mode-0600" in readme
    assert "M0-M4 已实现" in architecture and "GET /v1/events?after=" in architecture
    assert "do not double" in env_example and "docker compose up -d" in env_example
