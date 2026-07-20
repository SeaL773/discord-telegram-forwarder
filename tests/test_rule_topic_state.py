from importlib import import_module

pytest = import_module("pytest")

from src.models import Target
from src.router import parse_rules


def rule_enabled(rule):
    return getattr(rule, "enabled")


def rule_channel_name(rule):
    return getattr(rule, "channel_name")


def snapshot_topic_states(snapshot):
    return getattr(snapshot, "topic_states")()


def test_rule_defaults_enabled_and_retains_channel_name():
    snapshot = parse_rules({
        "rules": [{
            "name": "market",
            "channel_name": "Market Alerts",
            "match": {"channel_id": "channel-1"},
            "forward_to": {"chat_id": "-1001", "thread_id": 7},
        }],
        "default_action": "drop",
    })

    rule = snapshot.rules[0]
    assert rule_enabled(rule) is True
    assert rule_channel_name(rule) == "Market Alerts"


def test_disabled_matching_rule_is_first_match_terminal_drop():
    snapshot = parse_rules({
        "rules": [
            {
                "name": "disabled",
                "enabled": False,
                "match": {"channel_id": "channel-1"},
                "forward_to": {"chat_id": "-1001", "thread_id": 7},
            },
            {
                "name": "later",
                "match": {"channel_id": "channel-1"},
                "forward_to": {"chat_id": "-1002", "thread_id": 8},
            },
        ],
        "default_action": "drop",
    })

    assert snapshot.route({"message": {"channel_id": "channel-1"}}) == []


@pytest.mark.parametrize("enabled", [0, 1, "false", None, [], {}])
def test_rule_enabled_rejects_non_boolean_values(enabled):
    with pytest.raises(ValueError, match="enabled"):
        parse_rules({
            "rules": [{
                "enabled": enabled,
                "match": {},
                "forward_to": {"chat_id": "-1001", "thread_id": 7},
            }],
            "default_action": "drop",
        })


@pytest.mark.parametrize("channel_name", [True, 1, 1.5, None, [], {}])
def test_rule_channel_name_rejects_non_string_values(channel_name):
    with pytest.raises(ValueError, match="channel_name"):
        parse_rules({
            "rules": [{
                "channel_name": channel_name,
                "match": {},
                "forward_to": {"chat_id": "-1001", "thread_id": 7},
            }],
            "default_action": "drop",
        })


def test_rule_snapshot_topic_states_aggregate_shared_targets_with_or_semantics():
    shared = Target("-1001", 7)
    enabled_only = Target("-1001", 8)
    disabled_only = Target("-1002", 9)
    snapshot = parse_rules({
        "rules": [
            {"enabled": False, "match": {"channel_id": "a"}, "forward_to": {"chat_id": shared.chat_id, "thread_id": shared.thread_id}},
            {"enabled": True, "match": {"channel_id": "b"}, "forward_to": {"chat_id": shared.chat_id, "thread_id": shared.thread_id}},
            {"match": {"channel_id": "c"}, "forward_to": {"chat_id": enabled_only.chat_id, "thread_id": enabled_only.thread_id}},
            {"enabled": False, "match": {"channel_id": "d"}, "forward_to": {"chat_id": disabled_only.chat_id, "thread_id": disabled_only.thread_id}},
            {"enabled": False, "match": {"channel_id": "e"}, "forward_to": {"chat_id": "-1003"}},
            {"enabled": False, "match": {"channel_id": "f"}, "forward_to": {"chat_id": "-1003", "thread_id": 1}},
            {"enabled": False, "match": {"channel_id": "g"}, "forward_to": {"chat_id": "-1003", "thread_id": 0}},
        ],
        "default_action": "drop",
    })

    assert snapshot_topic_states(snapshot) == {
        shared.key: (shared, True),
        enabled_only.key: (enabled_only, True),
        disabled_only.key: (disabled_only, False),
    }


def test_default_forward_target_reopens_topic_closed_by_removed_rule():
    topic = Target("-1001", 7)
    snapshot = parse_rules({
        "rules": [],
        "default_action": {"forward_to": {"chat_id": topic.chat_id, "thread_id": topic.thread_id}},
    })

    assert snapshot_topic_states(snapshot) == {topic.key: (topic, True)}
