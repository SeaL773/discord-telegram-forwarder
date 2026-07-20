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
DLQ_SCAN_CHUNK_SIZE = 64 * 1024
DLQ_MAX_RECOVERY_RECORD_BYTES = 16 * 1024 * 1024
DEFAULT_DEAD_LETTER_MAX_BYTES = 32 * 1024 * 1024
DEFAULT_DEAD_LETTER_BACKUP_COUNT = 2
DEFAULT_STATE: dict[str, Any] = {
    "version": 1,
    "last_acked_cursor": None,
    "stats": {name: 0 for name in sorted(STAT_NAMES)},
    "in_flight": None,
    "bootstrap": None,
    "topic_states": {},
}


class StateStore:
    def __init__(
        self,
        path: Path,
        dead_letter_path: Path,
        dead_letter_max_bytes: int = DEFAULT_DEAD_LETTER_MAX_BYTES,
        dead_letter_backup_count: int = DEFAULT_DEAD_LETTER_BACKUP_COUNT,
    ) -> None:
        if not isinstance(dead_letter_max_bytes, int) or isinstance(dead_letter_max_bytes, bool) or dead_letter_max_bytes <= 0:
            raise ValueError("dead_letter_max_bytes must be a positive integer")
        if not isinstance(dead_letter_backup_count, int) or isinstance(dead_letter_backup_count, bool) or dead_letter_backup_count < 1:
            raise ValueError("dead_letter_backup_count must be a positive integer")
        self.path = path
        self.dead_letter_path = dead_letter_path
        self.dead_letter_max_bytes = dead_letter_max_bytes
        self.dead_letter_backup_count = dead_letter_backup_count
        self._lock = asyncio.Lock()
        self._dead_letter_checked_ids: set[str] = set()
        self._dead_letter_found_ids: set[str] = set()
        self._dead_letter_scan_complete = False
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
            if "topic_states" not in value:
                value["topic_states"] = {}
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
        topic_states = value.get("topic_states")
        if not isinstance(topic_states, dict):
            raise ValueError("invalid topic states")
        for key, enabled in topic_states.items():
            if not isinstance(key, str) or not isinstance(enabled, bool):
                raise ValueError("invalid topic state")
            chat_id, separator, raw_thread_id = key.rpartition(":")
            if not separator or not chat_id.strip() or chat_id != chat_id.strip() or not raw_thread_id.isdecimal():
                raise ValueError("invalid topic state key")
            thread_id = int(raw_thread_id)
            if thread_id <= 1 or str(thread_id) != raw_thread_id:
                raise ValueError("invalid topic state key")

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

    @property
    def topic_states(self) -> dict[str, bool]:
        return dict(self.data["topic_states"])

    async def mark_topic_state(self, target: Target, enabled: object) -> None:
        if not isinstance(enabled, bool):
            raise ValueError("topic state must be boolean")
        if not target.chat_id.strip() or target.chat_id != target.chat_id.strip():
            raise ValueError("topic target must have a nonempty chat ID")
        if target.thread_id is None or target.thread_id <= 1:
            raise ValueError("topic target must have a non-General thread")
        async with self._lock:
            self.data["topic_states"][target.key] = enabled
            await self._persist()

    async def prune_topic_states(self, keep_keys: set[str]) -> None:
        if any(not isinstance(key, str) for key in keep_keys):
            raise ValueError("topic state keys must be strings")
        async with self._lock:
            current = self.data["topic_states"]
            retained = {key: enabled for key, enabled in current.items() if key in keep_keys}
            if retained == current:
                return
            self.data["topic_states"] = retained
            try:
                await self._persist()
            except BaseException:
                self.data["topic_states"] = current
                raise

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
            self._advance(cursor, stat_name)
            await self._persist()

    def _advance(self, cursor: str, stat_name: str) -> None:
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

    def _append_dead_letter(self, record: dict[str, Any]) -> None:
        self.dead_letter_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.dead_letter_path.parent, 0o700)
        payload = (json.dumps(record, ensure_ascii=True, separators=(",", ":")) + "\n").encode("ascii")
        self._complete_pending_dead_letter_rotation()
        if self._dead_letter_rotation_required(len(payload)):
            self._write_pending_dead_letter(payload)
            self._complete_pending_dead_letter_rotation()
            return
        flags = os.O_APPEND | os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(self.dead_letter_path, flags, 0o600)
        try:
            file_stat = os.fstat(fd)
            if not stat.S_ISREG(file_stat.st_mode):
                raise RuntimeError("dead-letter path is not a regular file")
            os.fchmod(fd, 0o600)
            if file_stat.st_size and os.pread(fd, 1, file_stat.st_size - 1) != b"\n":
                self._write_all(fd, b"\n")
            self._write_all(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
        self._fsync_parent(self.dead_letter_path.parent)

    def _rotated_dead_letter_path(self, index: int) -> Path:
        return Path(f"{self.dead_letter_path}.{index}")

    def _pending_dead_letter_path(self) -> Path:
        return Path(f"{self.dead_letter_path}.pending")

    @staticmethod
    def _validate_regular_path(path: Path) -> bool:
        try:
            path_stat = path.lstat()
        except FileNotFoundError:
            return False
        if stat.S_ISLNK(path_stat.st_mode):
            raise OSError(f"dead-letter path must not be a symlink: {path}")
        if not stat.S_ISREG(path_stat.st_mode):
            raise RuntimeError(f"dead-letter path is not a regular file: {path}")
        return True

    def _dead_letter_rotation_required(self, payload_size: int) -> bool:
        if not self._validate_regular_path(self.dead_letter_path):
            return False
        active_size = self.dead_letter_path.stat().st_size
        if active_size == 0:
            return False
        separator_size = 1 if active_size and self._last_byte_is_not_newline(self.dead_letter_path) else 0
        return active_size + separator_size + payload_size > self.dead_letter_max_bytes

    def _write_pending_dead_letter(self, payload: bytes) -> None:
        pending = self._pending_dead_letter_path()
        self._validate_regular_path(pending)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(pending, flags, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise RuntimeError("pending dead-letter path is not a regular file")
            os.fchmod(fd, 0o600)
            self._write_all(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
        self._fsync_parent(self.dead_letter_path.parent)

    def _complete_pending_dead_letter_rotation(self) -> None:
        pending = self._pending_dead_letter_path()
        if not self._validate_regular_path(pending):
            return
        os.chmod(pending, 0o600)
        for index in range(1, self.dead_letter_backup_count + 1):
            self._validate_regular_path(self._rotated_dead_letter_path(index))
        for index in range(self.dead_letter_backup_count - 1, 0, -1):
            source = self._rotated_dead_letter_path(index)
            if self._validate_regular_path(source):
                os.replace(source, self._rotated_dead_letter_path(index + 1))
                self._fsync_parent(self.dead_letter_path.parent)
        if self._validate_regular_path(self.dead_letter_path):
            os.replace(self.dead_letter_path, self._rotated_dead_letter_path(1))
            os.chmod(self._rotated_dead_letter_path(1), 0o600)
            self._fsync_parent(self.dead_letter_path.parent)
        os.replace(pending, self.dead_letter_path)
        os.chmod(self.dead_letter_path, 0o600)
        self._remove_excess_dead_letter_backups()
        self._fsync_parent(self.dead_letter_path.parent)

    def _remove_excess_dead_letter_backups(self) -> None:
        prefix = f"{self.dead_letter_path.name}."
        for path in self.dead_letter_path.parent.iterdir():
            suffix = path.name.removeprefix(prefix)
            if path.name.startswith(prefix) and suffix.isdecimal() and int(suffix) > self.dead_letter_backup_count:
                self._validate_regular_path(path)
                path.unlink()

    @staticmethod
    def _last_byte_is_not_newline(path: Path) -> bool:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags)
        try:
            file_stat = os.fstat(fd)
            if not stat.S_ISREG(file_stat.st_mode):
                raise RuntimeError("dead-letter path is not a regular file")
            return bool(file_stat.st_size and os.pread(fd, 1, file_stat.st_size - 1) != b"\n")
        finally:
            os.close(fd)

    @staticmethod
    def _write_all(fd: int, payload: bytes) -> None:
        remaining = memoryview(payload)
        while remaining:
            written = os.write(fd, remaining)
            if written <= 0:
                raise OSError("dead-letter write made no progress")
            remaining = remaining[written:]

    def _dead_letter_ids(self, requested: set[str]) -> set[str]:
        unchecked = requested - self._dead_letter_checked_ids
        if not unchecked:
            return requested & self._dead_letter_found_ids
        if self._dead_letter_scan_complete:
            self._dead_letter_checked_ids.update(unchecked)
            return requested & self._dead_letter_found_ids
        for path in self._dead_letter_paths():
            self._scan_dead_letter_ids(path, unchecked)
        self._dead_letter_checked_ids.update(unchecked)
        self._dead_letter_scan_complete = True
        return requested & self._dead_letter_found_ids

    def _dead_letter_paths(self) -> list[Path]:
        return [
            self.dead_letter_path,
            *(self._rotated_dead_letter_path(index) for index in range(1, self.dead_letter_backup_count + 1)),
            self._pending_dead_letter_path(),
        ]

    def _scan_dead_letter_ids(self, path: Path, requested: set[str]) -> None:
        if not self._validate_regular_path(path):
            return
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise RuntimeError("dead-letter path is not a regular file")
            os.fchmod(fd, 0o600)
            record = bytearray()
            oversized = False
            oversized_identity: str | None = None
            identity_probe_limit = max(
                (
                    len(b'"dead_letter_id":') + len(json.dumps(identity, ensure_ascii=True).encode("ascii"))
                    for identity in requested
                ),
                default=0,
            )
            while chunk := os.read(fd, DLQ_SCAN_CHUNK_SIZE):
                segments = chunk.split(b"\n")
                for index, segment in enumerate(segments):
                    if not oversized:
                        probe_limit = max(DLQ_MAX_RECOVERY_RECORD_BYTES, identity_probe_limit)
                        if len(record) + len(segment) <= probe_limit:
                            record.extend(segment)
                        else:
                            needed = max(0, probe_limit - len(record))
                            if needed:
                                record.extend(segment[:needed])
                            oversized_identity = self._dead_letter_identity_prefix(record, requested)
                            record.clear()
                            oversized = True
                    if index == len(segments) - 1:
                        continue
                    if not oversized:
                        try:
                            value = json.loads(record)
                        except (json.JSONDecodeError, UnicodeDecodeError, RecursionError):
                            pass
                        else:
                            identity = value.get("dead_letter_id") if isinstance(value, dict) else None
                            if identity in requested:
                                self._dead_letter_found_ids.add(identity)
                    elif oversized_identity is not None:
                        self._dead_letter_found_ids.add(oversized_identity)
                    record.clear()
                    oversized = False
                    oversized_identity = None
        finally:
            os.close(fd)

    @staticmethod
    def _dead_letter_identity_prefix(record: bytearray, requested: set[str]) -> str | None:
        for identity in requested:
            field = b'"dead_letter_id":' + json.dumps(identity, ensure_ascii=True).encode("ascii")
            if field in record:
                return identity
        return None

    def _target_dead_letter_id(self, target_index: int) -> str:
        inflight = self.in_flight
        if inflight is None:
            raise RuntimeError("no in-flight event")
        target = self._target(target_index)
        thread_id = target.get("thread_id")
        return f"target:{inflight['cursor']}:{target_index}:{target['chat_id']}:{'' if thread_id is None else thread_id}"

    async def dead_letter_target(self, target_index: int, record: dict[str, Any]) -> None:
        async with self._lock:
            target = self._target(target_index)
            if target["status"] != "pending":
                raise RuntimeError("target already terminal")
            identity = self._target_dead_letter_id(target_index)
            before = deepcopy(self.data)
            if identity not in self._dead_letter_ids({identity}):
                self._append_dead_letter({"dead_letter_id": identity, **{key: value for key, value in record.items() if key != "dead_letter_id"}})
                self._dead_letter_checked_ids.add(identity)
                self._dead_letter_found_ids.add(identity)
            try:
                target["status"] = "dead_lettered"
                self.data["stats"]["dead_lettered"] += 1
                await self._persist()
            except BaseException:
                self.data = before
                raise

    async def recover_target_dead_letters(self) -> None:
        async with self._lock:
            inflight = self.in_flight
            if inflight is None:
                return
            requested = {
                self._target_dead_letter_id(index)
                for index, target in enumerate(inflight["targets"])
                if target["status"] == "pending"
            }
            identities = self._dead_letter_ids(requested)
            before = deepcopy(self.data)
            changed = False
            for index, target in enumerate(inflight["targets"]):
                if target["status"] == "pending" and self._target_dead_letter_id(index) in identities:
                    target["status"] = "dead_lettered"
                    self.data["stats"]["dead_lettered"] += 1
                    changed = True
            if not changed:
                return
            try:
                await self._persist()
            except BaseException:
                self.data = before
                raise

    async def reject_event(self, cursor: str, event: dict[str, Any], reason: str) -> None:
        async with self._lock:
            inflight = self.in_flight
            if inflight is not None and inflight["cursor"] != cursor:
                raise RuntimeError("reject cursor does not match in-flight")
            bootstrap = self.bootstrap
            if bootstrap is not None:
                index = bootstrap["next_index"]
                if index >= len(bootstrap["items"]) or bootstrap["items"][index]["cursor"] != cursor:
                    raise RuntimeError("bootstrap cursor order violation")
            before = deepcopy(self.data)
            self._append_dead_letter({"cursor": cursor, "event": event, "reason": reason, "phase": "prepare"})
            try:
                self._advance(cursor, "dead_lettered")
                await self._persist()
            except BaseException:
                self.data = before
                raise

    async def gap_to(self, cursor: str | None) -> None:
        async with self._lock:
            self.data["in_flight"] = None
            self.data["bootstrap"] = None
            self.data["last_acked_cursor"] = cursor
            self.data["stats"]["gaps"] += 1
            await self._persist()

    async def dead_letter(self, record: dict[str, Any]) -> None:
        async with self._lock:
            self._append_dead_letter(record)
            self.data["stats"]["dead_lettered"] += 1
            await self._persist()
