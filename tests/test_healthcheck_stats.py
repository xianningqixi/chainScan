"""Addon stats count eval events, independently of delivery and old schemas."""
import json
import os
import sqlite3
import subprocess
import sys

import pytest

import common
import healthcheck
from conftest import REPO
from test_monitoring import event


def legacy_root(root, now, rows):
    (root / "state").mkdir()
    # Deliberately no tokens/evals tables or Store migration.
    with sqlite3.connect(root / "state/pipeline.db") as db:
        db.execute("CREATE TABLE breaker(name TEXT, open_until REAL, reason TEXT)")
    common.append_jsonl(root / "state/events.jsonl", [event("ws_message", now), *rows])


def snapshot(root):
    return {str(p.relative_to(root)): (p.read_bytes(), p.stat().st_mtime_ns)
            for p in root.rglob("*") if p.is_file()}


def test_complete_distributions_do_not_double_count(tmp_path):
    now = 1800000000
    rows = [
        event("eval", now - 86400, signal_id="same", verdict="pending", layer="L0", reason="mcap_pending",
              flags=["high_tax"], would_reject=[]),
        event("eval", now - 30, signal_id="same", verdict="accept", layer="L0", reason="smart>=1",
              flags=["high_tax", "high_tax", "dev_rm_liq"], would_reject=["dev_rm_liq", "dev_rm_liq"]),
        event("eval", now - 20, verdict="reject", layer="L1", reason="smart_remove",
              flags=["smart_remove", "dev_rm_liq"], would_reject=["dev_rm_liq"]),
        event("eval", now, verdict="reject", layer="L2", reason="score_below_min", flags=[], would_reject=[]),
        # Outcome annotations must not be counted a second time.
        event("rejected", now - 30, reason="mcap_pending", flags=["high_tax"]),
        event("rejected", now - 20, reason="smart_remove", would_reject=["dev_rm_liq"]),
        event("accepted", now - 10, pipeline_latency_ms=100, flags=["high_tax"]),
        event("accepted", now - 5, pipeline_latency_ms=300),
        event("accepted", now - 1, pipeline_latency_ms=None),
        event("eval", now - 86401, verdict="reject", layer="old", flags=["old"], would_reject=["old"]),
        event("eval", now + 1, verdict="reject", layer="future", flags=["future"], would_reject=["future"]),
        event("accepted", now + 1, pipeline_latency_ms=10000),
        event("rejected", now - 86401, reason="old"),
    ]
    legacy_root(tmp_path, now, rows)
    before = snapshot(tmp_path)
    result = healthcheck.inspect(tmp_path, now=now, stats="24h")
    assert result["exit_code"] == 0
    assert result["stats"] == {
        "window": "24h", "rejected_by_reason": {"mcap_pending": 1, "smart_remove": 1},
        "evals": 4, "evals_by_layer": {"L0": 2, "L1": 1, "L2": 1},
        "evals_by_layer_reason": {"L0": {"mcap_pending": 1, "smart>=1": 1},
                                "L1": {"smart_remove": 1}, "L2": {"score_below_min": 1}},
        "evals_by_verdict": {"accept": 1, "pending": 1, "reject": 2},
        "flags": {"dev_rm_liq": 2, "high_tax": 2, "smart_remove": 1},
        "would_reject": {"dev_rm_liq": 2},
        "would_reject_by_verdict": {"accept": {"dev_rm_liq": 1}, "pending": {}, "reject": {"dev_rm_liq": 1}},
        "accepted": 3, "latency_samples": 2, "missing_latency": 1,
        "pipeline_latency_ms": {"p50": 200, "p95": 290},
    }
    assert snapshot(tmp_path) == before
    assert "stats" not in healthcheck.inspect(tmp_path, now=now)


def test_legacy_and_malformed_optional_fields_are_tolerated(tmp_path):
    now = 1800000000
    rows = [event("eval", now, useful=False, reason="mcap_pending"),
            event("eval", now, layer=None, verdict=[], reason={}, flags="high_tax", would_reject={"dev_rm_liq": 2}),
            event("eval", now, layer="L1", verdict="accept", reason="smart>=1",
                  flags=["", " ", None, 4, [], {}, "late:16.0", "late:16.0"],
                  would_reject=[False, {}, "stale"]),
            event("eval", now, layer="L1", verdict="accept", reason="smart>=1", flags=None, would_reject=None),
            event("rejected", now, reason="duplicate")]
    legacy_root(tmp_path, now, rows)
    report = healthcheck.inspect(tmp_path, now=now, stats="1h")
    assert report["exit_code"] == 0 and report["malformed_event_rows"] == 0
    stats = report["stats"]
    assert stats["evals"] == 4
    assert stats["evals_by_layer"] == {"L1": 2, "unknown": 2}
    assert stats["evals_by_verdict"] == {"accept": 2, "unknown": 2}
    assert stats["evals_by_layer_reason"]["unknown"] == {"mcap_pending": 1, "unknown": 1}
    assert stats["flags"] == {"late:16.0": 1} and stats["would_reject"] == {"stale": 1}
    assert stats["would_reject_by_verdict"] == {"accept": {"stale": 1}, "unknown": {}}
    assert stats["rejected_by_reason"] == {"duplicate": 1}


def test_empty_stats_are_additive_without_creating_state(tmp_path):
    root = tmp_path / "missing"
    stats = healthcheck.inspect(root, now=1800000000, stats="30m")["stats"]
    assert stats["evals"] == stats["accepted"] == stats["latency_samples"] == 0
    for key in ("flags", "would_reject", "evals_by_layer", "evals_by_layer_reason", "evals_by_verdict", "would_reject_by_verdict"):
        assert stats[key] == {}
    assert not root.exists()


def test_cli_reports_addon_stats_on_unmigrated_database(tmp_path):
    now = healthcheck.time.time()
    legacy_root(tmp_path, now, [event("eval", now, verdict="accept", layer="L0", reason="smart>=1",
                                   flags=["dev_rm_liq"], would_reject=["dev_rm_liq"])])
    before = snapshot(tmp_path)
    completed = subprocess.run([sys.executable, str(REPO / "bin/healthcheck.py"), "--stats", "24h"],
                               env={**os.environ, "SIGNALS_ROOT": str(tmp_path)}, capture_output=True, text=True, check=True)
    stats = json.loads(completed.stdout)["stats"]
    assert stats["flags"] == stats["would_reject"] == {"dev_rm_liq": 1}
    assert stats["evals_by_layer"] == {"L0": 1}
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize("window", ["0h", "-1h", "24", "1w", "1.5h"])
def test_invalid_stats_window_still_rejected(tmp_path, window):
    with pytest.raises(ValueError, match="positive duration"):
        healthcheck.inspect(tmp_path, stats=window)
