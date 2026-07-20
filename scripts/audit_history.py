#!/usr/bin/env python3
"""
audit_history.py - Git history audit for private identifier strings.

Scans every locally reachable Git object (all refs, including branches and
tags) for exact string matches supplied through a caller-provided UTF-8
denylist file.  Reports only ref/object/path/count metadata; never prints
matched values.  Exits nonzero when findings are present or on any error.

Usage:
    python scripts/audit_history.py --repo /path/to/repo --denylist /path/to/denylist.txt

Denylist file format:
    One entry per line, UTF-8, no BOM.
    Blank lines and lines starting with '#' are ignored.
    Each entry must be 1-200 bytes (UTF-8 encoded), no NUL or newline characters.
    No duplicate entries.
    Maximum 500 entries.

Object size limit:
    Blobs, commit objects, and annotated tag objects larger than
    MAX_OBJECT_BYTES (default 64 MiB) are rejected with exit code 2
    (fail-closed). This prevents memory exhaustion from malicious or
    pathological repositories.

Security constraints:
    - Denylist path must be a regular file (not a symlink).
    - All subprocess calls use argument lists (no shell=True).
    - Matched values are never printed or logged.
    - Validation error messages never echo denylist entry values.
    - Exits 1 on any finding; exits 2 on usage/validation/size error.
"""

from __future__ import annotations

import argparse
import json
import stat
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import NoReturn

_MAX_ENTRY_BYTES = 200
_MAX_ENTRY_COUNT = 500
_MAX_OBJECT_BYTES = 64 * 1024 * 1024


def _die(msg: str, code: int = 2) -> NoReturn:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def _load_denylist(path: Path) -> list[bytes]:
    try:
        st = path.lstat()
    except OSError as exc:
        _die(f"Cannot stat denylist file: {exc}")

    if stat.S_ISLNK(st.st_mode):
        _die("Denylist path must be a regular file, not a symlink.")

    if not stat.S_ISREG(st.st_mode):
        _die("Denylist path must be a regular file.")

    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        _die(f"Cannot read denylist file: {exc}")
    except UnicodeDecodeError as exc:
        _die(f"Denylist file is not valid UTF-8: {exc}")

    entries: list[bytes] = []
    seen: set[bytes] = set()
    for lineno, line in enumerate(raw.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        encoded = stripped.encode("utf-8")
        if b"\x00" in encoded or b"\n" in encoded or b"\r" in encoded:
            _die(f"Denylist line {lineno}: entry contains NUL or newline character.")
        if len(encoded) > _MAX_ENTRY_BYTES:
            _die(
                f"Denylist line {lineno}: entry exceeds {_MAX_ENTRY_BYTES} bytes "
                f"(got {len(encoded)})."
            )
        if encoded in seen:
            _die(f"Denylist line {lineno}: duplicate entry (index {len(entries)}).")
        seen.add(encoded)
        entries.append(encoded)

    if not entries:
        _die("Denylist file contains no valid entries (all blank or comments).")

    if len(entries) > _MAX_ENTRY_COUNT:
        _die(
            f"Denylist has {len(entries)} entries; maximum is {_MAX_ENTRY_COUNT}."
        )

    return entries


def _git_bytes(args: list[str], cwd: Path) -> bytes:
    try:
        result = subprocess.run(
            ["git"] + args,
            cwd=str(cwd),
            capture_output=True,
            check=True,
        )
    except FileNotFoundError:
        _die("'git' executable not found in PATH.")
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode(errors="replace").strip() if exc.stderr else ""
        _die(f"git {args[0]} failed (exit {exc.returncode}): {stderr}")
    return result.stdout


def _git_run(args: list[str], cwd: Path) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            ["git"] + args,
            cwd=str(cwd),
            capture_output=True,
        )
    except FileNotFoundError:
        _die("'git' executable not found in PATH.")


@dataclass
class ObjectInfo:
    sha: str
    obj_type: str
    paths: set[str] = field(default_factory=set)
    refs: set[str] = field(default_factory=set)


def _build_object_index(repo: Path) -> dict[str, ObjectInfo]:
    """
    Build a mapping of object SHA -> ObjectInfo covering all reachable objects.

    Uses git for-each-ref to enumerate all local refs, then rev-list --objects
    per ref to collect the objects and paths reachable from each ref.  The path
    field in rev-list --objects output is the blob/tree path within the tree;
    commit objects have no path.
    """
    index: dict[str, ObjectInfo] = {}

    ref_out = _git_bytes(
        ["for-each-ref", "--format=%(refname) %(objectname)", "refs/"],
        repo,
    )
    refs: list[tuple[str, str]] = []
    for line in ref_out.decode(errors="replace").splitlines():
        parts = line.split(None, 1)
        if len(parts) == 2:
            refs.append((parts[0].strip(), parts[1].strip()))

    head_result = _git_run(["rev-parse", "--verify", "HEAD"], repo)
    if head_result.returncode == 0:
        head_sha = head_result.stdout.decode(errors="replace").strip()
        if head_sha:
            refs.append(("HEAD", head_sha))

    for ref_name, _ref_sha in refs:
        if _ref_sha not in index:
            index[_ref_sha] = ObjectInfo(sha=_ref_sha, obj_type="")
        index[_ref_sha].refs.add(ref_name)
        obj_out = _git_bytes(
            ["rev-list", "--objects", ref_name],
            repo,
        )
        for line in obj_out.decode(errors="replace").splitlines():
            parts = line.split(None, 1)
            if not parts:
                continue
            sha = parts[0].strip()
            path = parts[1] if len(parts) == 2 else ""
            if sha not in index:
                index[sha] = ObjectInfo(sha=sha, obj_type="")
            if path:
                index[sha].paths.add(path)
            index[sha].refs.add(ref_name)

    if index:
        sha_input = ("\n".join(index.keys()) + "\n").encode()
        try:
            type_result = subprocess.run(
                ["git", "cat-file", "--batch-check=%(objectname) %(objecttype)"],
                cwd=str(repo),
                input=sha_input,
                capture_output=True,
            )
        except FileNotFoundError:
            _die("'git' executable not found in PATH.")
        if type_result.returncode == 0:
            for line in type_result.stdout.decode(errors="replace").splitlines():
                parts = line.split(None, 1)
                if len(parts) == 2 and parts[0] in index:
                    index[parts[0]].obj_type = parts[1].strip()
        else:
            _die("git cat-file batch type query failed")

        missing_types = [sha for sha, info in index.items() if not info.obj_type]
        if missing_types:
            _die(f"git did not return object types for {len(missing_types)} reachable objects")

    return index


def _object_size(sha: str, repo: Path) -> int:
    raw = _git_bytes(["cat-file", "-s", sha], repo)
    try:
        return int(raw.decode(errors="strict").strip())
    except ValueError:
        _die(f"git returned an invalid size for object {sha}")


def _cat_object(sha: str, repo: Path) -> bytes:
    return _git_bytes(["cat-file", "-p", sha], repo)


@dataclass
class Finding:
    object_sha: str
    obj_type: str
    paths: set[str]
    refs: set[str]
    term_index: int
    match_count: int


def audit(repo: Path, denylist: list[bytes], max_object_bytes: int = _MAX_OBJECT_BYTES) -> int:
    """
    Scan all reachable git objects for denylist entries.

    Returns the number of findings (0 = clean).
    Prints only metadata (object SHA, type, paths, refs, match count).
    Matched values are never printed.
    """
    print(f"Auditing repository: {repo}", file=sys.stderr)
    print(f"Denylist entries: {len(denylist)}", file=sys.stderr)

    print("Building object index...", file=sys.stderr)
    index = _build_object_index(repo)

    scannable = [
        info for info in index.values()
        if info.obj_type in ("blob", "commit", "tag")
    ]
    print(f"Objects to scan: {len(scannable)}", file=sys.stderr)

    findings: list[Finding] = []

    for info in scannable:
        size = _object_size(info.sha, repo)
        if size > max_object_bytes:
            _die(
                f"Object {info.sha} ({info.obj_type}) is {size} bytes, "
                f"which exceeds the {max_object_bytes}-byte scan limit. "
                f"Refusing to load into memory (fail-closed)."
            )

        content = _cat_object(info.sha, repo)
        if not content:
            continue

        for idx, term in enumerate(denylist):
            count = content.count(term)
            if count > 0:
                findings.append(Finding(
                    object_sha=info.sha,
                    obj_type=info.obj_type,
                    paths=set(info.paths),
                    refs=set(info.refs),
                    term_index=idx,
                    match_count=count,
                ))

    if not findings:
        print(
            "\nAUDIT RESULT: CLEAN -- no denylist entries found in any reachable object.",
            file=sys.stderr,
        )
        return 0

    print(
        f"\nAUDIT RESULT: {len(findings)} finding(s) -- denylist entries present in history.",
        file=sys.stderr,
    )
    print("\nFindings (values redacted):", file=sys.stderr)
    print(
        f"{'OBJECT SHA':<42} {'TYPE':<8} {'TERM#':>5} {'MATCHES':>7}  PATHS  REFS",
        file=sys.stderr,
    )
    print("-" * 120, file=sys.stderr)

    for f in findings:
        paths_str = ", ".join(json.dumps(path, ensure_ascii=True) for path in sorted(f.paths)) if f.paths else "(non-blob object)"
        refs_str = ", ".join(json.dumps(ref_name, ensure_ascii=True) for ref_name in sorted(f.refs)) if f.refs else "(no ref)"
        print(
            f"{f.object_sha:<42} {f.obj_type:<8} {f.term_index:>5} {f.match_count:>7}"
            f"  {paths_str}  [{refs_str}]",
            file=sys.stderr,
        )

    print(
        "\nNOTE: Matched values are intentionally not shown. "
        "See docs/safe-public-release-workflow.md for remediation steps.",
        file=sys.stderr,
    )
    return len(findings)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit all reachable Git objects for private identifier strings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--repo",
        type=Path,
        default=Path("."),
        help="Path to the Git repository root (default: current directory).",
    )
    parser.add_argument(
        "--denylist",
        type=Path,
        required=True,
        help="Path to a UTF-8 text file with one search term per line.",
    )
    parser.add_argument(
        "--max-object-bytes",
        type=int,
        default=_MAX_OBJECT_BYTES,
        metavar="BYTES",
        help=(
            f"Maximum blob/commit size to scan in bytes (default: {_MAX_OBJECT_BYTES}). "
            "Objects exceeding this limit cause exit 2 (fail-closed)."
        ),
    )
    args = parser.parse_args()

    if args.max_object_bytes < 1 or args.max_object_bytes > 1024 * 1024 * 1024:
        _die("--max-object-bytes must be between 1 and 1073741824")

    repo = args.repo.resolve()
    if not repo.is_dir():
        _die(f"Repository path is not a directory: {repo}")
    repository_check = _git_run(["rev-parse", "--git-dir"], repo)
    if repository_check.returncode != 0:
        _die(f"Not a git repository: {repo}")

    denylist = _load_denylist(args.denylist)

    finding_count = audit(repo, denylist, max_object_bytes=args.max_object_bytes)
    if finding_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
