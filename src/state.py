from __future__ import annotations

import asyncio
import json
import os
import stat
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any

from .models import Envelope, Target


STAT_NAMES = {"forwarded", "dropped", "dead_lettered", "gaps"}
DEFAULT_STATE: dict[str, Any] = {
    "version": 1,
    "last_acked_cursor": None,
    "stats": {name: 0 for name in sorted(STAT_NAMES)},
    "in_flight": None,
    "bootstrap": None,
}


class StateStore:
    def __init__(self, path: Path, dead_letter_path: Path) -> None:
        self.path = path
        self.dead_letter_path = dead_letter_path
        self._lock = asyncio.Lock()
        self.data = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return deepcopy(DEFAULT_STATE)
        if self.path.is_symlink():
            raise RuntimeError("state path must not be a symlink")
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
            if "bootstrap" not in value:
                value["bootstrap"] = None
            self._validate(value)
            os.chmod(self.path, 0o600)
            return value
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            corrupt = self.path.with_suffix(self.path.suffix + ".corrupt")
            try:
                os.replace(self.path, corrupt)
            except OSError:
                pass
            raise RuntimeError(f"invalid state preserved at {corrupt}") from exc

    @staticmethod
    def _validate_target(target: Any) -> None:
        if not isinstance(target, dict) or not isinstance(target.get("chat_id"), str) or not target["chat_id"]:
            raise ValueError("invalid target")
        if target.get("status") not in {"pending", "sent", "dead_lettered"}:
            raise ValueError("invalid target status")
        if target.get("phase", "media") not in {"media", "fallback"}:
            raise ValueError("invalid target phase")
        for name in ("retries", "fallback_retries"):
            value = target.get(name, 0)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError("invalid retry count")

    @classmethod
    def _validate(cls, value: Any) -> None:
        if not isinstance(value, dict) or value.get("version") != 1:
            raise ValueError("unsupported state version")
        ack = value.get("last_acked_cursor")
        if ack is not None and (not isinstance(ack, str) or not ack):
            raise ValueError("invalid ack cursor")
        stats = value.get("stats")
        if not isinstance(stats, dict) or set(stats) != STAT_NAMES or any(not isinstance(v, int) or isinstance(v, bool) or v < 0 for v in stats.values()):
            raise ValueError("invalid stats")
        inflight = value.get("in_flight")
        if inflight is not None:
            if not isinstance(inflight, dict) or not isinstance(inflight.get("cursor"), str) or not inflight["cursor"] or not isinstance(inflight.get("event"), dict):
                raise ValueError("invalid in-flight")
            targets = inflight.get("targets")
            if not isinstance(targets, list) or not targets:
                raise ValueError("invalid in-flight targets")
            for target in targets:
                cls._validate_target(target)
        bootstrap = value.get("bootstrap")
        if bootstrap is not None:
            if not isinstance(bootstrap, dict) or not isinstance(bootstrap.get("ready_cursor"), str) or not bootstrap["ready_cursor"]:
                raise ValueError("invalid bootstrap boundary")
            items = bootstrap.get("items")
            index = bootstrap.get("next_index")
            if not isinstance(items, list) or not isinstance(index, int) or isinstance(index, bool) or not 0 <= index <= len(items):
                raise ValueError("invalid bootstrap plan")
            for item in items:
                if not isinstance(item, dict) or not isinstance(item.get("cursor"), str) or not isinstance(item.get("event"), dict) or not isinstance(item.get("targets"), list):
                    raise ValueError("invalid bootstrap item")

    async def _persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.path.parent, 0o700)
        fd, name = tempfile.mkstemp(prefix=".state-", suffix=".json", dir=self.path.parent)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self.data, handle, ensure_ascii=True, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(name, self.path)
            os.chmod(self.path, 0o600)
            self._fsync_parent(self.path.parent)
        finally:
            if os.path.exists(name):
                os.unlink(name)

    @staticmethod
    def _fsync_parent(path: Path) -> None:
        try:
            directory_fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            pass

    @property
    def ack(self) -> str | None:
        value = self.data.get("last_acked_cursor")
        return value if isinstance(value, str) else None

    @property
    def in_flight(self) -> dict[str, Any] | None:
        value = self.data.get("in_flight")
        return value if isinstance(value, dict) else None

    @property
    def bootstrap(self) -> dict[str, Any] | None:
        value = self.data.get("bootstrap")
        return value if isinstance(value, dict) else None

    async def save_bootstrap(self, ready_cursor: str, items: list[dict[str, Any]]) -> None:
        async with self._lock:
            if self.data["bootstrap"] is not None or self.data["in_flight"] is not None:
                raise RuntimeError("cannot overwrite recovery state")
            self.data["bootstrap"] = {"ready_cursor": ready_cursor, "next_index": 0, "items": items}
            await self._persist()

    async def begin(self, envelope: Envelope, targets: list[Target]) -> None:
        async with self._lock:
            if self.data["in_flight"] is not None:
                raise RuntimeError("in-flight event already exists")
            self.data["in_flight"] = {
                "cursor": envelope.cursor,
                "event": envelope.event,
                "targets": [{"chat_id": t.chat_id, "thread_id": t.thread_id, "status": "pending", "phase": "media", "retries": 0, "fallback_retries": 0} for t in targets],
            }
            await self._persist()

    def _target(self, target_index: int) -> dict[str, Any]:
        inflight = self.in_flight
        if inflight is None:
            raise RuntimeError("no in-flight event")
        targets = inflight["targets"]
        if not 0 <= target_index < len(targets):
            raise IndexError("target index out of bounds")
        return targets[target_index]

    async def retry(self, target_index: int, *, fallback: bool = False) -> None:
        async with self._lock:
            target = self._target(target_index)
            key = "fallback_retries" if fallback else "retries"
            target[key] = int(target.get(key, 0)) + 1
            await self._persist()

    async def set_fallback(self, target_index: int) -> None:
        async with self._lock:
            target = self._target(target_index)
            if target["status"] != "pending":
                raise RuntimeError("terminal target cannot enter fallback")
            target["phase"] = "fallback"
            await self._persist()

    async def terminal(self, target_index: int, status: str) -> None:
        if status not in {"sent", "dead_lettered"}:
            raise ValueError("invalid terminal status")
        async with self._lock:
            target = self._target(target_index)
            if target["status"] != "pending":
                raise RuntimeError("target already terminal")
            target["status"] = status
            await self._persist()

    async def finish(self, cursor: str, stat_name: str) -> None:
        if stat_name not in STAT_NAMES:
            raise ValueError("invalid stat name")
        async with self._lock:
            inflight = self.in_flight
            if inflight is not None and inflight["cursor"] != cursor:
                raise RuntimeError("finish cursor does not match in-flight")
            self.data["last_acked_cursor"] = cursor
            self.data["in_flight"] = None
            self.data["stats"][stat_name] += 1
            bootstrap = self.bootstrap
            if bootstrap is not None:
                index = bootstrap["next_index"]
                if index >= len(bootstrap["items"]) or bootstrap["items"][index]["cursor"] != cursor:
                    raise RuntimeError("bootstrap cursor order violation")
                bootstrap["next_index"] = index + 1
                if bootstrap["next_index"] == len(bootstrap["items"]):
                    if cursor != bootstrap["ready_cursor"]:
                        raise RuntimeError("bootstrap did not finish at ready boundary")
                    self.data["bootstrap"] = None
            await self._persist()

    async def gap_to(self, cursor: str | None) -> None:
        async with self._lock:
            self.data["in_flight"] = None
            self.data["bootstrap"] = None
            self.data["last_acked_cursor"] = cursor
            self.data["stats"]["gaps"] += 1
            await self._persist()

    async def dead_letter(self, record: dict[str, Any]) -> None:
        async with self._lock:
            self.dead_letter_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(self.dead_letter_path.parent, 0o700)
            flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            fd = os.open(self.dead_letter_path, flags, 0o600)
            try:
                file_stat = os.fstat(fd)
                if not stat.S_ISREG(file_stat.st_mode):
                    raise RuntimeError("dead-letter path is not a regular file")
                os.fchmod(fd, 0o600)
                payload = (json.dumps(record, ensure_ascii=True, separators=(",", ":")) + "\n").encode("ascii")
                os.write(fd, payload)
                os.fsync(fd)
            finally:
                os.close(fd)
            self._fsync_parent(self.dead_letter_path.parent)
            self.data["stats"]["dead_lettered"] += 1
            await self._persist()
