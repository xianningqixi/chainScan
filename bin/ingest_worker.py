#!/usr/bin/env python3
"""Consume inbox in a resident process, polling every 0.5 seconds."""
from __future__ import annotations

import signal
from threading import Event

from common import ROOT, log_event
import ingest
from store import Store

POLL_SECONDS = 0.5


def run(stop: Event):
    store = None
    while not stop.is_set():
        try:
            if store is None:
                store = Store(ROOT)
            report = ingest.process_batch(store)
            if report["files"] or report["errors"]:
                log_event("ingest_batch", files=report["files"], accepted=report["accepted"],
                          rejected=report["rejected"], errors=len(report["errors"]))
        except Exception as exc:
            # Claims and delivery journal survive; retry on the next poll.
            log_event("ingest_error", source="worker", error=type(exc).__name__)
        stop.wait(POLL_SECONDS)


def main():
    stop = Event()
    previous = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        previous[sig] = signal.signal(sig, lambda *_: stop.set())
    try:
        run(stop)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    main()
