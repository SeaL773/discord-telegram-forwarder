"""
Tests for scripts/audit_history.py using synthetic Git repositories.

All tests use temporary directories with synthetic identifiers.
No production repository history is inspected.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from importlib import import_module
from pathlib import Path

pytest = import_module("pytest")

SCRIPT = Path(__file__).parent.parent / "scripts" / "audit_history.py"
PYTHON = sys.executable

try:
    _GIT_AVAILABLE = subprocess.run(["git", "--version"], capture_output=True).returncode == 0
except FileNotFoundError:
    _GIT_AVAILABLE = False


@pytest.fixture(autouse=True)
def require_git():
    if not _GIT_AVAILABLE:
        pytest.skip("git not available in this environment")

_GIT_ENV = {
    "GIT_AUTHOR_NAME": "Test",
    "GIT_AUTHOR_EMAIL": "test@example.com",
    "GIT_COMMITTER_NAME": "Test",
    "GIT_COMMITTER_EMAIL": "test@example.com",
    "GIT_CONFIG_NOSYSTEM": "1",
    "HOME": "/tmp",
}


def _git(args: list[str], cwd: Path) -> subprocess.CompletedProcess[bytes]:
    merged = {**os.environ, **_GIT_ENV}
    return subprocess.run(
        ["git"] + args,
        cwd=str(cwd),
        capture_output=True,
        env=merged,
        check=True,
    )


def _make_repo(tmp_path: Path, name: str = "repo") -> Path:
    repo = tmp_path / name
    repo.mkdir()
    _git(["init", "-b", "main"], repo)
    _git(["config", "user.email", "test@example.com"], repo)
    _git(["config", "user.name", "Test"], repo)
    return repo


def _commit(
    repo: Path,
    filename: str,
    content: str,
    message: str = "add file",
) -> str:
    target = repo / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    _git(["add", filename], repo)
    _git(["commit", "-m", message], repo)
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(repo),
        capture_output=True,
        env={**os.environ, **_GIT_ENV},
    )
    return result.stdout.decode().strip()


def _tag(repo: Path, name: str) -> None:
    _git(["tag", name], repo)


def _branch(repo: Path, name: str) -> None:
    _git(["branch", name], repo)


def _run_audit(repo: Path, denylist_path: Path) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [PYTHON, str(SCRIPT), "--repo", str(repo), "--denylist", str(denylist_path)],
        capture_output=True,
    )


def _write_denylist(tmp_path: Path, entries: list[str], name: str = "denylist.txt") -> Path:
    p = tmp_path / name
    p.write_text("\n".join(entries) + "\n", encoding="utf-8")
    return p


class TestCurrentTreeFindings:
    def test_blob_in_current_tree_detected(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "config.txt", "SYNTHETIC_ID_111111\nother content\n")
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_ID_111111"])

        result = _run_audit(repo, denylist)

        assert result.returncode == 1
        stderr = result.stderr.decode(errors="replace")
        assert "finding" in stderr.lower()

    def test_no_match_exits_zero(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "readme.txt", "This file has no private identifiers.\n")
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_ID_999999"])

        result = _run_audit(repo, denylist)

        assert result.returncode == 0
        assert "CLEAN" in result.stderr.decode(errors="replace")

    def test_multiple_occurrences_counted(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "data.txt", "SYNTHETIC_ID_111111\nSYNTHETIC_ID_111111\nSYNTHETIC_ID_111111\n")
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_ID_111111"])

        result = _run_audit(repo, denylist)

        assert result.returncode == 1
        assert b"3" in result.stderr


class TestHistoricalFindings:
    def test_removed_blob_still_detected(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "secret.txt", "SYNTHETIC_ID_222222\n", "add private id")
        _commit(repo, "secret.txt", "replaced content\n", "remove private id")
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_ID_222222"])

        result = _run_audit(repo, denylist)

        assert result.returncode == 1

    def test_clean_history_passes(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "a.txt", "public content only\n", "initial")
        _commit(repo, "b.txt", "more public content\n", "second")
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_ID_333333"])

        result = _run_audit(repo, denylist)

        assert result.returncode == 0

    def test_commit_message_finding(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "clean content\n", "fix: remove SYNTHETIC_COMMIT_MSG_ID_888888 from config")
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_COMMIT_MSG_ID_888888"])

        result = _run_audit(repo, denylist)

        assert result.returncode == 1
        stderr = result.stderr.decode(errors="replace")
        assert "commit" in stderr.lower()


class TestPathAndRefAttribution:
    def test_blob_path_reported(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "subdir/config.yaml", "SYNTHETIC_PATH_ID_444444: true\n")
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_PATH_ID_444444"])

        result = _run_audit(repo, denylist)

        assert result.returncode == 1
        stderr = result.stderr.decode(errors="replace")
        assert "subdir/config.yaml" in stderr

    def test_branch_ref_reported(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "SYNTHETIC_BRANCH_ID_555555\n")
        _branch(repo, "feature/test-branch")
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_BRANCH_ID_555555"])

        result = _run_audit(repo, denylist)

        assert result.returncode == 1
        stderr = result.stderr.decode(errors="replace")
        assert "feature/test-branch" in stderr or "main" in stderr

    def test_tag_ref_reported(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "SYNTHETIC_TAG_ID_666666\n")
        _tag(repo, "v0.1.0")
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_TAG_ID_666666"])

        result = _run_audit(repo, denylist)

        assert result.returncode == 1
        stderr = result.stderr.decode(errors="replace")
        assert "v0.1.0" in stderr

    def test_annotated_tag_message_is_scanned(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "clean\n")
        _git(["tag", "-a", "private-note", "-m", "SYNTHETIC_ANNOTATED_TAG_ID_121212"], repo)
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_ANNOTATED_TAG_ID_121212"])

        result = _run_audit(repo, denylist)

        assert result.returncode == 1
        stderr = result.stderr.decode(errors="replace")
        assert "tag" in stderr
        assert "private-note" in stderr

    def test_historical_blob_ref_reported(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "old.txt", "SYNTHETIC_HIST_ID_777777\n", "add old")
        _commit(repo, "old.txt", "clean\n", "sanitize")
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_HIST_ID_777777"])

        result = _run_audit(repo, denylist)

        assert result.returncode == 1
        stderr = result.stderr.decode(errors="replace")
        assert "main" in stderr or "refs/" in stderr


class TestOutputRedaction:
    def test_matched_value_absent_from_stdout(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        secret = "SYNTHETIC_SECRET_XYZABC"
        _commit(repo, "file.txt", f"{secret}\n")
        denylist = _write_denylist(tmp_path, [secret])

        result = _run_audit(repo, denylist)

        assert result.returncode == 1
        assert secret.encode() not in result.stdout
        assert secret.encode() not in result.stderr

    def test_matched_value_absent_from_stderr(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        secret = "SYNTHETIC_PRIVATE_NUMID_404040"
        _commit(repo, "cfg.txt", f"id={secret}\n")
        denylist = _write_denylist(tmp_path, [secret])

        result = _run_audit(repo, denylist)

        assert result.returncode == 1
        assert secret.encode() not in result.stderr

    def test_term_index_reported_not_value(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        secret = "SYNTHETIC_INDEXED_SECRET_12345"
        _commit(repo, "f.txt", f"{secret}\n")
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_DECOY_AAAAA", secret])

        result = _run_audit(repo, denylist)

        assert result.returncode == 1
        stderr = result.stderr.decode(errors="replace")
        assert secret not in stderr
        assert "1" in stderr


class TestDenylistValidation:
    def test_empty_denylist_rejected(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "content\n")
        denylist = _write_denylist(tmp_path, [])

        result = _run_audit(repo, denylist)

        assert result.returncode == 2
        stderr = result.stderr.decode(errors="replace")
        assert "no valid entries" in stderr.lower() or "empty" in stderr.lower()

    def test_comment_only_denylist_rejected(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "content\n")
        denylist = _write_denylist(tmp_path, ["# this is a comment", "# another comment"])

        result = _run_audit(repo, denylist)

        assert result.returncode == 2

    def test_entry_too_long_rejected(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "content\n")
        long_entry = "A" * 201
        denylist = _write_denylist(tmp_path, [long_entry])

        result = _run_audit(repo, denylist)

        assert result.returncode == 2
        stderr = result.stderr.decode(errors="replace")
        assert "200" in stderr or "exceed" in stderr.lower()
        assert long_entry not in stderr

    def test_too_many_entries_rejected(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "content\n")
        entries = [f"ENTRY_{i:06d}" for i in range(501)]
        denylist = _write_denylist(tmp_path, entries)

        result = _run_audit(repo, denylist)

        assert result.returncode == 2
        stderr = result.stderr.decode(errors="replace")
        assert "500" in stderr or "maximum" in stderr.lower()

    def test_duplicate_entry_rejected(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "content\n")
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_DUP_ENTRY", "SYNTHETIC_DUP_ENTRY"])

        result = _run_audit(repo, denylist)

        assert result.returncode == 2
        stderr = result.stderr.decode(errors="replace")
        assert "duplicate" in stderr.lower()
        assert "SYNTHETIC_DUP_ENTRY" not in stderr

    def test_nonexistent_denylist_rejected(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "content\n")
        missing = tmp_path / "does_not_exist.txt"

        result = _run_audit(repo, missing)

        assert result.returncode == 2

    def test_symlink_denylist_rejected(self, tmp_path: Path) -> None:
        if os.name == "nt":
            pytest.skip("Symlink test requires POSIX or elevated Windows privilege")
        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "content\n")
        real = _write_denylist(tmp_path, ["SYNTHETIC_ID_555555"], "real.txt")
        link = tmp_path / "link.txt"
        link.symlink_to(real)

        result = _run_audit(repo, link)

        assert result.returncode == 2
        assert "symlink" in result.stderr.decode(errors="replace").lower()

    def test_blank_lines_and_comments_ignored(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "SYNTHETIC_ID_666666\n")
        content = textwrap.dedent("""\
            # This is a comment

            SYNTHETIC_ID_666666

            # Another comment
        """)
        denylist = tmp_path / "mixed.txt"
        denylist.write_text(content, encoding="utf-8")

        result = _run_audit(repo, denylist)

        assert result.returncode == 1

    def test_validation_error_does_not_echo_entry_value(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "content\n")
        long_entry = "SYNTHETIC_LONG_" + "X" * 190
        denylist = _write_denylist(tmp_path, [long_entry])

        result = _run_audit(repo, denylist)

        assert result.returncode == 2
        assert long_entry.encode() not in result.stderr


class TestOversizedObject:
    def test_oversized_blob_fails_closed(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        large_content = "SYNTHETIC_LARGE_BLOB_CONTENT\n" + ("x" * 1024)
        _commit(repo, "large.bin", large_content)
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_LARGE_BLOB_CONTENT"])

        result = subprocess.run(
            [
                PYTHON, str(SCRIPT),
                "--repo", str(repo),
                "--denylist", str(denylist),
                "--max-object-bytes", "512",
            ],
            capture_output=True,
        )

        assert result.returncode == 2
        stderr = result.stderr.decode(errors="replace")
        assert "exceed" in stderr.lower() or "limit" in stderr.lower() or "bytes" in stderr.lower()

    def test_invalid_object_limit_rejected(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "clean\n")
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_UNUSED"])

        result = subprocess.run(
            [PYTHON, str(SCRIPT), "--repo", str(repo), "--denylist", str(denylist), "--max-object-bytes", "0"],
            capture_output=True,
        )

        assert result.returncode == 2


class TestNonProductionRepo:
    def test_does_not_scan_production_repo(self, tmp_path: Path) -> None:
        production_repo = Path(__file__).parent.parent
        assert (production_repo / ".git").exists(), "sanity: production repo has .git"

        repo = _make_repo(tmp_path)
        _commit(repo, "f.txt", "SYNTHETIC_ID_777777\n")
        denylist = _write_denylist(tmp_path, ["SYNTHETIC_ID_777777"])

        result = _run_audit(repo, denylist)

        assert result.returncode == 1
        assert str(production_repo) not in result.stderr.decode(errors="replace")
