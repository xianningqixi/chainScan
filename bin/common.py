"""Shared paths, configuration and file primitives for the file pipeline."""
from __future__ import annotations

import fcntl
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import tempfile
import tomllib
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(os.environ.get("SIGNALS_ROOT", "/workspace/signals")).resolve()
TZ = timezone(timedelta(hours=8))
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config/pipeline.toml"
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 5


def load_config() -> dict:
    def merge(target, supplied):
        for key, value in supplied.items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                merge(target[key], value)
            else:
                target[key] = value

    with DEFAULT_CONFIG_PATH.open("rb") as f:
        config = tomllib.load(f)
    path = Path(os.environ.get("SIGNALS_CONFIG", ROOT / "config/pipeline.toml"))
    if path.exists() and path.resolve() != DEFAULT_CONFIG_PATH:
        with path.open("rb") as f:
            merge(config, tomllib.load(f))
    return config


def atomic_write(path: Path, data: str | bytes) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data.encode("utf-8") if isinstance(data, str) else data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def append_jsonl(path: Path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as f:
        # A malformed or interrupted historical tail must not absorb a new row.
        # This only appends a separator; existing bytes are never rewritten.
        f.seek(0, os.SEEK_END)
        needs_separator = False
        if f.tell():
            f.seek(-1, os.SEEK_END)
            needs_separator = f.read(1) != b"\n"
        for row in rows:
            data = (json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8")
            if needs_separator:
                f.write(b"\n")
                needs_separator = False
            f.write(data)
        f.flush()
        os.fsync(f.fileno())


@contextmanager
def file_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def log_event(event: str, *, log_name=None, log_root=None, **fields) -> None:
    """Best-effort telemetry; never change signal delivery on a logging failure.

    Callers supply metadata only, never raw responses, credentials or payloads.
    Keep stdout available for the existing CLI JSON reports. Operational messages
    select log_name to use a rotating JSONL .log instead of the event journal.
    """
    try:
        root = Path(log_root) if log_root is not None else ROOT
        row = {**fields, "ts": datetime.now(TZ).isoformat(), "event": event}
        if log_name is not None:
            if not log_name or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789_" for c in log_name):
                raise ValueError("invalid log name")
            # Reopen under the lock so even separate processes see the current
            # file after rotation. No cached descriptor points at an old backup.
            with file_lock(root / f"state/{log_name}.log.lock"):
                handler = RotatingFileHandler(root / f"state/{log_name}.log",
                                              maxBytes=LOG_MAX_BYTES,
                                              backupCount=LOG_BACKUP_COUNT,
                                              encoding="utf-8", delay=True)
                try:
                    handler.emit(logging.LogRecord(log_name, logging.INFO, "", 0,
                                                   json.dumps(row, ensure_ascii=False), (), None))
                finally:
                    handler.close()
        else:
            with file_lock(root / "state/events.lock"):
                append_jsonl(root / "state/events.jsonl", [row])
    except (OSError, TypeError, ValueError):
        print(f"{log_name or 'events.jsonl'}: unable to record event", file=sys.stderr)
