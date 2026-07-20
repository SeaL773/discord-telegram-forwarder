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


def test_config_requires_explicit_admin_chat_id(tmp_path: Path):
    path = tmp_path / "config.yaml"
    path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="admin_chat_id"):
        load_config(path)


def test_dead_letter_retention_config_defaults_and_values(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("ADMIN_CHAT_ID", "-1001")
    default_path = tmp_path / "default.yaml"
    default_path.write_text('admin_chat_id: "${ADMIN_CHAT_ID}"\n', encoding="utf-8")
    default = load_config(default_path)
    assert default.dead_letter_max_bytes == 32 * 1024 * 1024
    assert default.dead_letter_backup_count == 2

    configured_path = tmp_path / "configured.yaml"
    configured_path.write_text("admin_chat_id: '-1001'\nstate: {dead_letter_max_bytes: 16777216, dead_letter_backup_count: 4}\n", encoding="utf-8")
    configured = load_config(configured_path)
    assert configured.dead_letter_max_bytes == 16 * 1024 * 1024
    assert configured.dead_letter_backup_count == 4


@pytest.mark.parametrize("name,value", [
    ("dead_letter_max_bytes", "true"),
    ("dead_letter_max_bytes", "1048575"),
    ("dead_letter_max_bytes", "1073741825"),
    ("dead_letter_max_bytes", "1.5"),
    ("dead_letter_backup_count", "true"),
    ("dead_letter_backup_count", "0"),
    ("dead_letter_backup_count", "-1"),
    ("dead_letter_backup_count", "101"),
    ("dead_letter_backup_count", "2.5"),
])
def test_dead_letter_retention_config_rejects_invalid_values(tmp_path: Path, name: str, value: str):
    path = tmp_path / "config.yaml"
    path.write_text(f"admin_chat_id: '-1001'\nstate:\n  {name}: {value}\n", encoding="utf-8")
    with pytest.raises(ValueError, match=name):
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
    assert "dead_letter_max_bytes: 33554432" in config and "dead_letter_backup_count: 2" in config
    assert "host.docker.internal" in readme and "mode-0600" in readme
    assert "approximately 96 MiB total" in readme and "no age-based deletion" in readme
    assert "`.pending` file" in readme and "interrupted `.pending` rotation" in readme
    assert "M0-M4 已实现" in architecture and "GET /v1/events?after=" in architecture
    assert "do not double" in env_example and "docker compose up -d" in env_example

    release_workflow = (root / "docs" / "safe-public-release-workflow.md").read_text()
    assert "ls-files -z --cached --others --exclude-standard" in release_workflow
    assert "git archive HEAD" not in release_workflow
    assert "switch --orphan" not in release_workflow


def test_tracked_docs_contain_no_deployment_id_shapes():
    import re
    root = Path(__file__).parents[1]
    snowflake_re = re.compile(r'(?<!\w)\d{17,20}(?!\w)')
    tg_supergroup_re = re.compile(r'-100\d{9,}')
    doc_dirs = [root, root / "docs"]
    violations: list[str] = []
    for doc_dir in doc_dirs:
        if not doc_dir.is_dir():
            continue
        for md_file in sorted(doc_dir.glob("*.md")):
            text = md_file.read_text(encoding="utf-8")
            for lineno, line in enumerate(text.splitlines(), start=1):
                if snowflake_re.search(line) or tg_supergroup_re.search(line):
                    violations.append(f"{md_file.relative_to(root)}:{lineno}: {line.strip()}")
    assert not violations, (
        "Tracked documentation contains deployment-ID-shaped numeric values.\n"
        "Replace with angle-bracket placeholders such as <DISCORD_GUILD_ID>.\n"
        "Violations:\n" + "\n".join(violations)
    )
