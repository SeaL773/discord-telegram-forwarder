import json
from importlib import import_module
from pathlib import Path

pytest = import_module("pytest")

from src.models import Target
from src.state import StateStore


def stored_topic_states(state):
    return getattr(state, "topic_states")


async def mark_topic_state(state, target: Target, enabled: bool):
    await getattr(state, "mark_topic_state")(target, enabled)


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

    assert stored_topic_states(state) == {}
    assert state.data["topic_states"] == {}


@pytest.mark.asyncio
async def test_topic_state_persists_only_when_explicitly_marked(tmp_path: Path):
    path = tmp_path / "state.json"
    state = StateStore(path, tmp_path / "dead.ndjson")
    topic = Target("-1001", 7)

    assert stored_topic_states(state) == {}
    assert not path.exists()

    await mark_topic_state(state, topic, False)

    assert stored_topic_states(state) == {topic.key: False}
    assert stored_topic_states(StateStore(path, state.dead_letter_path)) == {topic.key: False}


@pytest.mark.asyncio
async def test_topic_state_view_is_a_copy_and_marker_requires_boolean(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    topic = Target("-1001", 7)
    view = stored_topic_states(state)
    view[topic.key] = False

    assert stored_topic_states(state) == {}
    with pytest.raises(ValueError, match="boolean"):
        await state.mark_topic_state(topic, 1)
    assert stored_topic_states(state) == {}


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
