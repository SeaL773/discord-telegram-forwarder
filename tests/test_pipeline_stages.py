import asyncio
from importlib import import_module
from typing import Any

import httpx
pytest = import_module("pytest")

from src.main import bootstrap_work_item, persist_gap_and_alert, prepare_work
from src.models import Attachment, DownloadedMedia, Envelope, EventPreparationError, FormattedMessage, PreparedEvent, RejectedEvent, Target, WorkItem
from src.state import StateStore


@pytest.mark.asyncio
async def test_prepare_can_download_b_while_a_send_blocked_and_commit_order_holds():
    source = asyncio.Queue()
    prepared = asyncio.Queue(maxsize=4)
    downloaded = []
    committed = []
    release_a = asyncio.Event()

    async def prepare():
        for _ in range(2):
            item = await source.get()
            downloaded.append(item)
            await prepared.put(item)
            source.task_done()

    async def send():
        for _ in range(2):
            item = await prepared.get()
            if item == "A": await release_a.wait()
            committed.append(item)
            prepared.task_done()

    await source.put("A"); await source.put("B")
    prepare_task = asyncio.create_task(prepare())
    send_task = asyncio.create_task(send())
    await source.join()
    assert downloaded == ["A", "B"] and committed == []
    release_a.set()
    await asyncio.gather(prepare_task, send_task)
    assert committed == ["A", "B"]


@pytest.mark.asyncio
async def test_prepare_only_converts_explicit_deterministic_rejection():
    class Router:
        def route(self, event: dict[str, Any]) -> list[Target]:
            del event
            return []
    class Media:
        async def download_all(self, event: dict[str, Any], attachments: list[Attachment] | None = None) -> tuple[list[DownloadedMedia], list[str]]:
            del event, attachments
            return [], []
    rejected = RejectedEvent("bad", {"schema_version": 2}, "invalid event schema")
    prepared = await prepare_work(WorkItem(rejected), Router(), Media(), None)
    assert prepared is rejected

    class BrokenRouter:
        def route(self, event: dict[str, Any]) -> list[Target]:
            del event
            raise RuntimeError("bug")
    with pytest.raises(RuntimeError, match="bug"):
        await prepare_work(WorkItem(Envelope("c", {"event_type": "CREATED", "message": {}})), BrokenRouter(), Media(), None)

    class DeterministicRouter:
        def route(self, event: dict[str, Any]) -> list[Target]:
            del event
            raise EventPreparationError("known bad event")
    deterministic = await prepare_work(WorkItem(Envelope("c", {"event_type": "CREATED", "message": {}})), DeterministicRouter(), Media(), None)
    assert isinstance(deterministic, RejectedEvent)
    assert deterministic.reason == "known bad event"


@pytest.mark.asyncio
async def test_prepare_drop_skips_formatting_attachment_extraction_and_download(monkeypatch):
    class Router:
        def route(self, event: dict[str, Any]) -> list[Target]:
            del event
            return []

    class Media:
        async def download_all(self, event: dict[str, Any], attachments: list[Attachment] | None = None) -> tuple[list[DownloadedMedia], list[str]]:
            del event, attachments
            raise AssertionError("drop event must not download media")

    monkeypatch.setattr("src.main.format_event", lambda _event: (_ for _ in ()).throw(AssertionError("drop event must not be formatted")))
    monkeypatch.setattr("src.main.extract_attachments", lambda _event: (_ for _ in ()).throw(AssertionError("drop event must not extract attachments")))

    envelope = Envelope("drop", {"event_type": "CREATED", "message": {}})
    prepared = await prepare_work(WorkItem(envelope), Router(), Media(), None)

    assert isinstance(prepared, PreparedEvent)
    assert prepared.envelope is envelope
    assert prepared.targets == []
    assert prepared.formatted == FormattedMessage("", "")
    assert prepared.media == []
    assert prepared.fallback_urls == []
    assert prepared.attachment_urls == []


def test_bootstrap_work_item_recovers_cursor_only_drop_and_payload_items():
    dropped = bootstrap_work_item({"cursor": "drop", "action": "drop"})
    assert dropped == WorkItem(Envelope("drop", {}), ())

    forwarded_event = {"message": {"content": "keep"}}
    forwarded = bootstrap_work_item({"cursor": "forward", "event": forwarded_event, "targets": [{"chat_id": "1", "thread_id": 7}]})
    assert forwarded == WorkItem(Envelope("forward", forwarded_event), (Target("1", 7),))

    rejected_event = {"schema_version": 2}
    rejected = bootstrap_work_item({"cursor": "reject", "event": rejected_event, "targets": [], "rejection_reason": "invalid schema"})
    assert rejected == WorkItem(RejectedEvent("reject", rejected_event, "invalid schema"), ())


@pytest.mark.asyncio
async def test_prepare_reuses_extracted_attachments_for_media_download(monkeypatch):
    attachments = [Attachment("https://cdn.discordapp.com/file", "file")]

    class Router:
        def route(self, event: dict[str, Any]) -> list[Target]:
            del event
            return [Target("1")]

    class Media:
        async def download_all(self, event: dict[str, Any], attachments: list[Attachment] | None = None) -> tuple[list[DownloadedMedia], list[str]]:
            del event
            assert attachments is not None and attachments[0].url == "https://cdn.discordapp.com/file"
            return [], []

    monkeypatch.setattr("src.main.extract_attachments", lambda _event: attachments)
    prepared = await prepare_work(WorkItem(Envelope("cursor", {"event_type": "CREATED", "message": {}})), Router(), Media(), None)
    assert isinstance(prepared, PreparedEvent)
    assert prepared.attachment_urls == [attachments[0].url]


@pytest.mark.asyncio
async def test_prepare_converts_explicit_formatter_rejection_without_swallowing_other_errors(monkeypatch):
    class Router:
        def route(self, event: dict[str, Any]) -> list[Target]:
            del event
            return [Target("1")]
    class Media:
        async def download_all(self, event: dict[str, Any], attachments: list[Attachment] | None = None) -> tuple[list[DownloadedMedia], list[str]]:
            del event, attachments
            return [], []
    envelope = Envelope("c", {"event_type": "CREATED", "message": {}})
    monkeypatch.setattr("src.main.format_event", lambda _event, _media_count=0: (_ for _ in ()).throw(EventPreparationError("bad format")))
    rejected = await prepare_work(WorkItem(envelope), Router(), Media(), None)
    assert isinstance(rejected, RejectedEvent)
    assert rejected.reason == "bad format"

    monkeypatch.setattr("src.main.format_event", lambda _event, _media_count=0: (_ for _ in ()).throw(RuntimeError("formatter bug")))
    with pytest.raises(RuntimeError, match="formatter bug"):
        await prepare_work(WorkItem(envelope), Router(), Media(), None)


@pytest.mark.asyncio
async def test_prepare_extract_exception_classification_and_cancellation(monkeypatch):
    class Router:
        def route(self, event: dict[str, Any]) -> list[Target]:
            del event
            return [Target("1")]
    class Media:
        async def download_all(self, event: dict[str, Any], attachments: list[Attachment] | None = None) -> tuple[list[DownloadedMedia], list[str]]:
            del event, attachments
            return [], []
    envelope = Envelope("c", {"event_type": "CREATED", "message": {}})

    monkeypatch.setattr("src.main.extract_attachments", lambda _event: (_ for _ in ()).throw(EventPreparationError("bad attachment")))
    rejected = await prepare_work(WorkItem(envelope), Router(), Media(), None)
    assert isinstance(rejected, RejectedEvent)
    assert rejected.reason == "bad attachment"

    monkeypatch.setattr("src.main.extract_attachments", lambda _event: (_ for _ in ()).throw(RuntimeError("extract bug")))
    with pytest.raises(RuntimeError, match="extract bug"):
        await prepare_work(WorkItem(envelope), Router(), Media(), None)

    monkeypatch.setattr("src.main.extract_attachments", lambda _event: (_ for _ in ()).throw(asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        await prepare_work(WorkItem(envelope), Router(), Media(), None)


@pytest.mark.asyncio
async def test_prepare_does_not_dead_letter_transient_media_failure(monkeypatch):
    class Router:
        def route(self, event: dict[str, Any]) -> list[Target]:
            del event
            return [Target("1")]
    class Media:
        async def download_all(self, event: dict[str, Any], attachments: list[Attachment] | None = None) -> tuple[list[DownloadedMedia], list[str]]:
            del event, attachments
            request = httpx.Request("GET", "https://cdn.discordapp.com/file")
            raise httpx.ConnectError("network unavailable", request=request)
    monkeypatch.setattr("src.main.extract_attachments", lambda _event: [])
    envelope = Envelope("c", {"event_type": "CREATED", "message": {}})
    with pytest.raises(httpx.ConnectError, match="network unavailable"):
        await prepare_work(WorkItem(envelope), Router(), Media(), None)


@pytest.mark.asyncio
async def test_gap_is_durable_before_best_effort_alert(tmp_path, monkeypatch):
    state = StateStore(tmp_path / "state", tmp_path / "dead")
    order = []
    original_gap = state.gap_to
    async def gap_to(cursor):
        await original_gap(cursor)
        order.append(("persisted", state.ack))
    monkeypatch.setattr(state, "gap_to", gap_to)
    class Sender:
        async def send_alert(self, chat_id: str, text: str) -> bool:
            order.append(("alert", state.ack, chat_id, text))
            return False
    await persist_gap_and_alert(state, Sender(), "admin", "ready")
    assert order[0] == ("persisted", "ready")
    assert order[1][:3] == ("alert", "ready", "admin")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("alert bug"), None])
async def test_gap_alert_failure_or_timeout_cannot_block_durable_advance(tmp_path, failure):
    state = StateStore(tmp_path / "state", tmp_path / "dead")

    class Sender:
        async def send_alert(self, chat_id: str, text: str) -> bool:
            del chat_id, text
            if failure is not None:
                raise failure
            await asyncio.Future()
            return True

    await persist_gap_and_alert(state, Sender(), "admin", "ready", alert_timeout_s=0.01)
    assert state.ack == "ready" and state.data["stats"]["gaps"] == 1
