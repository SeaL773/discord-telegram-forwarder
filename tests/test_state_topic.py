import json
from importlib import import_module
from pathlib import Path

pytest = import_module("pytest")

from src.models import Target
from src.state import StateStore


def old_state_payload():
    return {
        "version": 1,
        "last_acked_cursor": None,
        "stats": {"dead_lettered": 0, "dropped": 0, "forwarded": 0, "gaps": 0},
        "in_flight": None,
        "bootstrap": None,
    }


def test_old_state_file_gains_empty_topic_states_backward_compatibly(tmp_path: Path):
    path = tmp_path / "state.json"
    path.write_text(json.dumps(old_state_payload()), encoding="utf-8")

    state = StateStore(path, tmp_path / "dead.ndjson")

    assert state.topic_states == {}
    assert state.data["topic_states"] == {}


@pytest.mark.asyncio
async def test_topic_state_persists_only_when_explicitly_marked(tmp_path: Path):
    path = tmp_path / "state.json"
    state = StateStore(path, tmp_path / "dead.ndjson")
    topic = Target("-1001", 7)

    assert state.topic_states == {}
    assert not path.exists()

    await state.mark_topic_state(topic, False)

    assert state.topic_states == {topic.key: False}
    assert StateStore(path, state.dead_letter_path).topic_states == {topic.key: False}


@pytest.mark.asyncio
async def test_topic_state_view_is_a_copy_and_marker_requires_boolean(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    topic = Target("-1001", 7)
    view = state.topic_states
    view[topic.key] = False

    assert state.topic_states == {}
    with pytest.raises(ValueError, match="boolean"):
        await state.mark_topic_state(topic, 1)
    assert state.topic_states == {}


@pytest.mark.asyncio
async def test_prune_topic_states_removes_deleted_topics_and_persists(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    current = Target("-1001", 7)
    deleted = Target("-1001", 8)
    await state.mark_topic_state(current, True)
    await state.mark_topic_state(deleted, False)

    await state.prune_topic_states({current.key})

    assert state.topic_states == {current.key: True}
    assert StateStore(state.path, state.dead_letter_path).topic_states == {current.key: True}


@pytest.mark.asyncio
async def test_prune_topic_states_noop_skips_persist(tmp_path: Path, monkeypatch):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    current = Target("-1001", 7)
    await state.mark_topic_state(current, True)

    async def unexpected_persist():
        raise AssertionError("no-op prune must not persist")

    monkeypatch.setattr(state, "_persist", unexpected_persist)
    await state.prune_topic_states({current.key})
    assert state.topic_states == {current.key: True}


@pytest.mark.asyncio
async def test_prune_topic_states_rolls_back_memory_on_persist_failure(tmp_path: Path, monkeypatch):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    current = Target("-1001", 7)
    deleted = Target("-1001", 8)
    await state.mark_topic_state(current, True)
    await state.mark_topic_state(deleted, False)
    original = state.topic_states

    async def fail_persist():
        raise OSError("disk full")

    monkeypatch.setattr(state, "_persist", fail_persist)
    with pytest.raises(OSError, match="disk full"):
        await state.prune_topic_states({current.key})
    assert state.topic_states == original


@pytest.mark.parametrize("topic_states", [
    None,
    [],
    {"": True},
    {"-1001:": True},
    {"-1001:1": True},
    {"-1001:7": 1},
    {"-1001:7": "true"},
])
def test_state_validation_rejects_malformed_topic_states(tmp_path: Path, topic_states):
    path = tmp_path / "state.json"
    payload = old_state_payload()
    payload["topic_states"] = topic_states
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError):
        StateStore(path, tmp_path / "dead.ndjson")
    assert path.with_suffix(".json.corrupt").exists()
