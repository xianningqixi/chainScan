#!/usr/bin/env python3
"""Minimal, local-only webhook receiver.

POSTed JSON objects/arrays are queued in ``inbox/``; ingest runs separately.
The receiver is deliberately disabled by default in ``config/services.toml``.
When an operator chooses to run it, ``WEBHOOK_TOKEN`` (or the more explicit
``SIGNAL_WEBHOOK_TOKEN``) can require an ``X-Signal-Token`` header.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import hmac
import json
import os
import signal
import time
from uuid import uuid4

from common import ROOT, atomic_write, log_event
INBOX = ROOT / "inbox"
HOST, PORT = "127.0.0.1", 8765
MAX_BODY_BYTES = 1024 * 1024


def _configured_token():
    """Return the optional shared token without exposing it in logs."""
    return os.environ.get("SIGNAL_WEBHOOK_TOKEN") or os.environ.get("WEBHOOK_TOKEN")


class H(BaseHTTPRequestHandler):
    # Bound stalled clients so an incomplete request cannot hold a worker forever.
    timeout = 10

    def log_message(self, fmt, *args):
        # Request paths can contain credentials; log only the request outcome.
        log_event("http_request", log_name="webhook_server",
                  method=self.command, status=str(args[1]) if len(args) > 1 else None)

    def do_GET(self):
        if self.path in ("/", "/health"):
            self._json_response(200, {"ok": True, "service": "signal-inbox"})
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        if self.path not in ("/ingest", "/hook", "/signal"):
            self.send_response(404)
            self.end_headers()
            return

        expected_token = _configured_token()
        if expected_token:
            supplied_token = self.headers.get("X-Signal-Token", "")
            if not isinstance(supplied_token, str) or not hmac.compare_digest(
                    supplied_token.encode("utf-8"), expected_token.encode("utf-8")):
                self._json_response(401, {"ok": False, "error": "unauthorized"})
                return

        raw_length = self.headers.get("Content-Length")
        try:
            n = int(raw_length) if raw_length is not None else 0
        except (TypeError, ValueError):
            self._json_response(400, {"ok": False, "error": "invalid_content_length"})
            return
        if n < 0:
            self._json_response(400, {"ok": False, "error": "invalid_content_length"})
            return
        if n > MAX_BODY_BYTES:
            self._json_response(413, {"ok": False, "error": "body_too_large"})
            return

        body = self.rfile.read(n)
        try:
            obj = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            obj = None
        if not isinstance(obj, (dict, list)):
            self._json_response(400, {"ok": False, "error": "expected JSON object or array"})
            return
        INBOX.mkdir(parents=True, exist_ok=True)
        path = INBOX / f"hook_{time.time_ns()}_{uuid4().hex}.jsonl"
        # normalize to jsonl
        items = obj if isinstance(obj, list) else [obj]
        atomic_write(path, "\n".join(json.dumps(x, ensure_ascii=False) for x in items) + "\n")
        log_event("sig_queued", source="webhook", n=len(items))
        self._json_response(200, {"ok": True, "queued": str(path)})

    def _json_response(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class WebhookServer(ThreadingHTTPServer):
    """Serve independent webhook requests concurrently."""

    daemon_threads = True


def main():
    INBOX.mkdir(parents=True, exist_ok=True)

    def stop(*_):
        raise SystemExit(0)

    previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        with WebhookServer((HOST, PORT), H) as server:
            log_event("service_started", log_name="webhook_server", host=HOST, port=PORT)
            server.serve_forever()
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    main()
