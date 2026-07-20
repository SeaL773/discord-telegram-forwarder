from importlib import import_module
from pathlib import Path
from urllib.parse import parse_qs

import httpx

pytest = import_module("pytest")

from src.models import Target
from src.state import StateStore
from src.tg_sender import MAX_RETRY_AFTER_S, TgSender


def request_form(request: httpx.Request) -> dict[str, list[str]]:
    return parse_qs(request.content.decode("ascii"))


@pytest.mark.asyncio
async def test_sync_records_initially_enabled_topic_without_telegram_api_call(tmp_path: Path):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True})

    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    topic = Target("-1001", 7)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000).sync_forum_topics({topic.key: (topic, True)})

    assert requests == []
    assert state.topic_states == {topic.key: True}
    assert StateStore(state.path, state.dead_letter_path).topic_states == {topic.key: True}


@pytest.mark.asyncio
async def test_sync_closes_initially_disabled_topic_and_persists_success(tmp_path: Path):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True})

    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    topic = Target("-1001", 7)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000).sync_forum_topics({topic.key: (topic, False)})

    assert [request.url.path for request in requests] == ["/bottoken/closeForumTopic"]
    assert request_form(requests[0]) == {"chat_id": [topic.chat_id], "message_thread_id": [str(topic.thread_id)]}
    assert all("deleteForumTopic" not in request.url.path for request in requests)
    assert state.topic_states == {topic.key: False}


@pytest.mark.asyncio
async def test_sync_skips_unchanged_topic_states(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    enabled = Target("-1001", 7)
    disabled = Target("-1001", 8)
    await state.mark_topic_state(enabled, True)
    await state.mark_topic_state(disabled, False)
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True})

    desired = {enabled.key: (enabled, True), disabled.key: (disabled, False)}
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000).sync_forum_topics(desired)

    assert requests == []
    assert state.topic_states == {enabled.key: True, disabled.key: False}


@pytest.mark.asyncio
async def test_sync_prunes_states_for_topics_no_longer_in_rules(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    current = Target("-1001", 7)
    deleted = Target("-1001", 8)
    await state.mark_topic_state(current, True)
    await state.mark_topic_state(deleted, False)
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000).sync_forum_topics({current.key: (current, True)})

    assert requests == []
    assert state.topic_states == {current.key: True}


@pytest.mark.asyncio
async def test_sync_reopens_false_to_true_and_persists_success(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    topic = Target("-1001", 7)
    await state.mark_topic_state(topic, False)
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000).sync_forum_topics({topic.key: (topic, True)})

    assert [request.url.path for request in requests] == ["/bottoken/reopenForumTopic"]
    assert request_form(requests[0]) == {"chat_id": [topic.chat_id], "message_thread_id": [str(topic.thread_id)]}
    assert all("deleteForumTopic" not in request.url.path for request in requests)
    assert state.topic_states == {topic.key: True}


@pytest.mark.asyncio
async def test_sync_persists_only_successful_api_outcomes(tmp_path: Path):
    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    closes = Target("-1001", 7)
    fails = Target("-1001", 8)
    requests = []

    def handler(request):
        requests.append(request)
        form = request_form(request)
        if form["message_thread_id"] == [str(closes.thread_id)]:
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(400, json={"ok": False, "error_code": 400})

    desired = {closes.key: (closes, False), fails.key: (fails, False)}
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000).sync_forum_topics(desired)

    assert [request.url.path for request in requests] == ["/bottoken/closeForumTopic", "/bottoken/closeForumTopic"]
    assert state.topic_states == {closes.key: False}
    assert StateStore(state.path, state.dead_letter_path).topic_states == {closes.key: False}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response,expected_waits",
    [
        (lambda: httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 0.25}}), [0.25, 0.25]),
        (lambda: httpx.Response(500, json={"ok": False, "error_code": 500}), [1, 2]),
    ],
)
async def test_topic_api_retryable_failures_are_bounded_and_do_not_persist(tmp_path: Path, response, expected_waits):
    attempts = 0
    waits = []

    async def sleep(value):
        waits.append(value)

    def handler(_request):
        nonlocal attempts
        attempts += 1
        return response()

    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    topic = Target("-1001", 7)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000, sleep=sleep).sync_forum_topics({topic.key: (topic, False)})

    assert attempts == 3
    assert waits == expected_waits
    assert state.topic_states == {}


@pytest.mark.asyncio
async def test_topic_api_nonretryable_4xx_is_single_attempt_and_does_not_persist(tmp_path: Path):
    requests = []
    waits = []

    async def sleep(value):
        waits.append(value)

    def handler(request):
        requests.append(request)
        return httpx.Response(403, json={"ok": False, "error_code": 403})

    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    topic = Target("-1001", 7)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await TgSender("token", client, state, 1000, 1000, sleep=sleep).sync_forum_topics({topic.key: (topic, False)})

    assert [request.url.path for request in requests] == ["/bottoken/closeForumTopic"]
    assert waits == []
    assert state.topic_states == {}


@pytest.mark.asyncio
async def test_topic_sync_caps_retry_after_and_total_elapsed_time(tmp_path: Path):
    waits = []

    async def no_wait(value):
        waits.append(value)

    state = StateStore(tmp_path / "state.json", tmp_path / "dead.ndjson")
    topic = Target("-1001", 7)
    responses = [
        httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 10**9}}),
        httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 10**9}}),
        httpx.Response(500, json={"ok": False, "error_code": 500}),
    ]
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: responses.pop(0))) as client:
        await TgSender("token", client, state, 1000, 1000, sleep=no_wait).sync_forum_topics({topic.key: (topic, False)})

    assert waits == [MAX_RETRY_AFTER_S, MAX_RETRY_AFTER_S]
    assert state.topic_states == {}

    request_started = __import__("asyncio").Event()

    async def hangs(_request: httpx.Request) -> httpx.Response:
        request_started.set()
        await __import__("asyncio").Event().wait()
        raise AssertionError("unreachable")

    async with httpx.AsyncClient(transport=httpx.MockTransport(hangs)) as client:
        sender = TgSender("token", client, state, 1000, 1000)
        await sender.sync_forum_topics({topic.key: (topic, False)}, timeout_s=0.01)

    assert request_started.is_set()
    assert state.topic_states == {}
