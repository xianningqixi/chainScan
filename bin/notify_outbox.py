#!/usr/bin/env python3
"""Format outbox/NEW signals into a short Chinese push text.

Reads batch paths from outbox/NEW and appends formatted sections to PENDING_CHAT.
Successfully handled paths are archived; unreadable batches remain queued.
NOTIFY_WEBHOOK_URL optionally receives the same text as a best-effort JSON POST.
"""
from __future__ import annotations
import json, os, sys, time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

from common import ROOT, TZ, atomic_write, file_lock, log_event
OUTBOX = ROOT / "outbox"
NEW = OUTBOX / "NEW"
LAST = OUTBOX / "last_notify.txt"
PENDING = OUTBOX / "PENDING_CHAT"
OFFSET = OUTBOX / "PENDING_CHAT.offset"
DONE_DIR = OUTBOX / "notified"


class FileSink:
    def __init__(self, path: Path):
        self.path = path

    def send(self, body: str) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(body)
            f.flush()
            os.fsync(f.fileno())


class WebhookSink:
    """Best-effort informational text only; responses are never consumed."""

    def __init__(self, url: str):
        self.url = url

    def send(self, body: str) -> None:
        try:
            request = urllib.request.Request(
                self.url,
                data=json.dumps({"text": body}, ensure_ascii=False).encode("utf-8"),
                headers={"Content-Type": "application/json; charset=utf-8"},
                method="POST",
            )
            if request.type not in ("http", "https"):
                raise ValueError("webhook requires HTTP or HTTPS")
            with urllib.request.urlopen(request, timeout=3) as response:
                if not 200 <= response.status < 300:
                    raise urllib.error.HTTPError(self.url, response.status, "", None, None)
        except Exception as exc:
            # URLs and exception messages may contain credentials. Log metadata only.
            fields = {"sink": "webhook", "error": type(exc).__name__}
            if isinstance(exc, urllib.error.HTTPError):
                fields["status"] = exc.code
            log_event("notify_error", **fields)


def _fomo_summary(sig: dict) -> str:
    fv = sig.get("fomo_verify")
    if isinstance(fv, dict):
        s = fv.get("summary")
        if s:
            hits = fv.get("hits")
            if hits is not None and s in ("matched", "matched_name_mismatch"):
                return f"{s}(hits={hits})"
            return str(s)
    notes = str(sig.get("notes") or "")
    for part in notes.split(";"):
        part = part.strip()
        if part.startswith("fomo_verify="):
            return part.split("=", 1)[1]
    return "n/a"


def format_signal(sig: dict) -> str:
    name = sig.get("token_name") or "?"
    chain = sig.get("chain") or "?"
    ca = sig.get("ca") or "?"
    smart = sig.get("smart_money_count")
    smart_s = str(smart) if smart is not None else "?"
    src = sig.get("source_url") or ""
    fomo = _fomo_summary(sig)
    direction = sig.get("direction") or ""
    lines = [
        f"🔔 新信号 {name} ({chain})",
        f"CA: {ca}",
        f"聪明钱: {smart_s}" + (f" | 方向: {direction}" if direction else ""),
        f"FOMO核验: {fomo}",
    ]
    if src:
        lines.append(f"链接: {src}")
    return "\n".join(lines)


def format_update(sig: dict, changes) -> str:
    """Format informational state changes; never includes execution fields."""
    name = sig.get("token_name") or "?"
    chain = sig.get("chain") or "?"
    sid = sig.get("signal_id") or "?"
    ca = sig.get("ca") or "?"
    details = []
    for change in changes:
        if change.get("kind") == "status":
            details.append(f"状态→{change.get('status')}")
        elif change.get("kind") == "maxGain":
            details.append(f"maxGain≥{float(change.get('threshold')):g}%（当前 {float(change.get('value')):.2f}%）")
    if not details:
        return ""
    return (f"🔄 状态更新 {name} ({chain})\n"
            f"signal_id: {sid}\nCA: {ca}\n"
            f"{'; '.join(details)}\n\n")


def append_update(sig: dict, changes, *, root=None) -> str:
    """Append an update only to PENDING_CHAT, under the producer/consumer lock."""
    explicit_root = root is not None
    root = Path(root) if explicit_root else ROOT
    body = format_update(sig, changes)
    if not body:
        return ""
    pending = root / "outbox/PENDING_CHAT" if explicit_root else PENDING
    lock_path = root / "state/outbox.lock" if explicit_root else ROOT / "state/outbox.lock"
    with file_lock(lock_path):
        pending.parent.mkdir(parents=True, exist_ok=True)
        with pending.open("a+b") as stream:
            stream.seek(0, os.SEEK_END)
            if stream.tell():
                stream.seek(-1, os.SEEK_END)
                if stream.read(1) != b"\n":
                    stream.write(b"\n")
            stream.write(body.encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
    log_event("notify_update", signal_id=sig.get("signal_id"), changes=len(changes))
    return body


def _run_locked() -> dict:
    OUTBOX.mkdir(parents=True, exist_ok=True)
    if not NEW.exists():
        return {"ok": True, "signals": 0, "reason": "no_NEW"}
    sinks = [FileSink(PENDING)]
    url = os.environ.get("NOTIFY_WEBHOOK_URL")
    if url:
        sinks.append(WebhookSink(url))
    paths = [p.strip() for p in NEW.read_text(encoding="utf-8").splitlines() if p.strip()]
    remaining, completed, bodies = [], [], []
    n = 0
    for bp in dict.fromkeys(paths):
        p = Path(bp)
        if not p.is_absolute():
            p = OUTBOX / bp
        try:
            sigs = [json.loads(ln) for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
            texts = [format_signal(sig) for sig in sigs]
        except (OSError, ValueError, TypeError, AttributeError):
            remaining.append(bp)
            continue
        if texts:
            body = (f"### batch {p.name} {datetime.now(TZ).isoformat()}\n"
                    + "\n\n---\n\n".join(texts) + "\n\n")
            for sink in sinks:
                sink.send(body)
            bodies.append(body)
            n += len(sigs)
        completed.append(bp)
    if bodies:
        atomic_write(LAST, "".join(bodies))
        # Consumer-owned optional byte cursor. Never reset an existing cursor.
        if not OFFSET.exists():
            atomic_write(OFFSET, "0\n")
    if completed:
        DONE_DIR.mkdir(parents=True, exist_ok=True)
        atomic_write(DONE_DIR / f"NEW_{time.time_ns()}", "\n".join(completed) + "\n")
    if remaining:
        atomic_write(NEW, "\n".join(remaining) + "\n")
    else:
        NEW.unlink(missing_ok=True)
    log_event("notify", n=n, queued_batches=len(remaining))
    return {"ok": not remaining, "signals": n, "last_notify": str(LAST),
            "pending": bool(bodies), "queued_batches": len(remaining)}


def run() -> dict:
    # Same lock as NEW producers, so consuming cannot remove newly queued work.
    with file_lock(ROOT / "state/outbox.lock"):
        return _run_locked()


def main():
    print(json.dumps(run(), ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(json.dumps({"ok": False, "signals": 0, "reason": f"fatal:{type(e).__name__}"}))
        sys.exit(0)  # never fail hard for best-effort helper
