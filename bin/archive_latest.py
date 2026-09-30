#!/usr/bin/env python3
"""Manually copy append-only latest.jsonl into month archives.

The source file is never truncated, rewritten, or replaced.  Archives are
rebuilt per month from the current source snapshot so rerunning this command
is idempotent for each month.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

from common import ROOT, TZ

DEFAULT_THRESHOLD_BYTES = 50 * 1024 * 1024


def _month_from_timestamp(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("ts must be a non-empty ISO-8601 string")
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=TZ)
    else:
        parsed = parsed.astimezone(TZ)
    return parsed.strftime("%Y%m")


def _snapshot_lines(path: Path) -> tuple[list[bytes], int]:
    """Read one size-bounded snapshot, excluding a concurrent partial tail."""
    size = path.stat().st_size
    with path.open("rb") as source:
        data = source.read(size)
    lines = data.splitlines(keepends=True)
    if lines and not lines[-1].endswith(b"\n"):
        lines.pop()
    return lines, size


def _atomic_replace(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def archive_latest(
    latest: Path,
    archive_dir: Path,
    *,
    threshold_bytes: int = DEFAULT_THRESHOLD_BYTES,
    force: bool = False,
    dry_run: bool = False,
) -> dict:
    """Archive a stable snapshot of *latest* without changing the source."""
    latest = Path(latest)
    archive_dir = Path(archive_dir)
    if threshold_bytes < 0:
        raise ValueError("threshold_bytes must be non-negative")
    if not latest.exists():
        return {"ok": False, "archived": False, "reason": "missing_latest", "latest": str(latest)}

    size_bytes = latest.stat().st_size
    result = {
        "ok": True,
        "archived": False,
        "latest": str(latest),
        "archive_dir": str(archive_dir),
        "size_bytes": size_bytes,
        "threshold_bytes": threshold_bytes,
        "months": {},
        "skipped_rows": 0,
        "skipped_tail": False,
    }
    if size_bytes <= threshold_bytes and not force:
        result["reason"] = "below_threshold"
        return result

    lines, snapshot_size = _snapshot_lines(latest)
    result["snapshot_size_bytes"] = snapshot_size

    grouped: defaultdict[str, list[bytes]] = defaultdict(list)
    skipped = 0
    skipped_tail = snapshot_size > 0 and snapshot_size != sum(len(line) for line in lines)
    for line_number, raw_line in enumerate(lines, 1):
        if not raw_line.strip():
            continue
        try:
            row = json.loads(raw_line.decode("utf-8"))
            if not isinstance(row, dict):
                raise ValueError("row is not an object")
            month = _month_from_timestamp(row.get("ts"))
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
            skipped += 1
            continue
        grouped[month].append(raw_line)

    result["skipped_rows"] = skipped
    result["skipped_tail"] = skipped_tail
    result["months"] = {
        month: {"rows": len(rows), "bytes": sum(map(len, rows))}
        for month, rows in sorted(grouped.items())
    }
    if not grouped:
        result["reason"] = "no_archivable_rows"
        return result

    if not dry_run:
        for month, rows in sorted(grouped.items()):
            _atomic_replace(archive_dir / f"latest_{month}.jsonl", b"".join(rows))
        result["archived"] = True
    result["reason"] = "dry_run" if dry_run else "archived"
    return result


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="手动按 ts 月份复制归档 latest.jsonl（不会修改当前文件）"
    )
    parser.add_argument("--root", type=Path, default=ROOT,
                        help="信号根目录，默认 SIGNALS_ROOT")
    parser.add_argument("--latest", type=Path, default=None,
                        help="latest.jsonl 路径，默认 ROOT/latest.jsonl")
    parser.add_argument("--archive-dir", type=Path, default=None,
                        help="归档目录，默认 ROOT/archive")
    parser.add_argument("--threshold-mb", type=float, default=50.0,
                        help="触发阈值（MiB），默认 50；严格大于才归档")
    parser.add_argument("--force", action="store_true",
                        help="忽略大小阈值（仅手动验证/补归档使用）")
    parser.add_argument("--dry-run", action="store_true",
                        help="只扫描并输出计划，不写归档文件")
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.threshold_mb < 0:
        print("threshold must be non-negative", file=sys.stderr)
        return 2
    root = args.root.resolve()
    latest_arg = args.latest if args.latest is not None else Path("latest.jsonl")
    archive_arg = args.archive_dir if args.archive_dir is not None else Path("archive")
    latest = (latest_arg if latest_arg.is_absolute() else root / latest_arg).resolve()
    archive_dir = (archive_arg if archive_arg.is_absolute() else root / archive_arg).resolve()
    threshold_bytes = int(args.threshold_mb * 1024 * 1024)
    try:
        result = archive_latest(
            latest,
            archive_dir,
            threshold_bytes=threshold_bytes,
            force=args.force,
            dry_run=args.dry_run,
        )
    except (OSError, ValueError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
