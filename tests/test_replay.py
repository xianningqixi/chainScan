import json
import os
import subprocess
import sys

import pytest

from conftest import REPO


def test_offline_fixture_replay(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    for p in (REPO / "tests/fixtures").glob("binance-*.jsonl"):
        (source / p.name).write_bytes(p.read_bytes())
    out = tmp_path / "out"
    command = [sys.executable, str(REPO / "bin/replay.py"), "--from", str(source), "--out", str(out)]
    result = subprocess.run(command, capture_output=True, text=True, env=os.environ, timeout=30)
    assert result.returncode == 0, result.stderr
    report = json.loads((out / "report.json").read_text())
    assert report["accepted"] > 0
    assert report["accepted"] == report["unique_accepted"]
    assert "binance-57937" in report["recovered_after_rejection"]
    assert report["pending_resolved"] > 0
    assert sum(v["total"] for v in report["reject_by_reason"].values()) == report["rejected"]
    rows = [json.loads(s) for s in (out / "latest.jsonl").read_text().splitlines()]
    assert all(r["schema_version"] == 1 for r in rows)
    assert all(r["fomo_verify"]["summary"] == "dry_run" for r in rows)
    # Refuse overwriting any previous replay or production root.
    assert subprocess.run(command, capture_output=True).returncode != 0


def run_replay(source, out, *options, env=None):
    return subprocess.run(
        [sys.executable, str(REPO / "bin/replay.py"), "--from", str(source),
         "--out", str(out), *options], capture_output=True, text=True,
        env=env or os.environ, timeout=30)


def signal(sid, **fields):
    return {"signal_id": sid, "source": "binance_smart_money", "chain": "bsc",
            "ca": "0x" + "1" * 40, "ts": "2026-09-30T00:00:00+08:00",
            "direction": "buy", "status": "active", "smart_money_count": 3,
            "price": 2, "mcap": 9000000, "price_at_fetch": 1, **fields}


def test_fixed_config_comparison_and_report_only_labels(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    rows = [signal("winner", notes="maxGain=0%; tag={'x': [{'tagName': 'DEV Close Position'}]}"),
            signal("winner", notes="maxGain=120%; tag={'x': [{'tagName': 'DEV Close Position'}]}"),
            signal("fifty", smart_money_count=6, maxGain=60),
            signal("unlabeled", ca="0x" + "2" * 40),
            signal("loser", ca="0x" + "3" * 40, maxGain=20)]
    rows += [signal(f"sell-{i}", direction="sell") for i in range(25)]
    payload = "\n".join(json.dumps(row) for row in rows) + "\ninvalid json\n"
    (source / "signals.jsonl").write_text(payload)
    config = tmp_path / "candidate.toml"
    config.write_text('[filter]\nbaseline_dev_close_gate = false\n[filter.l1]\ndev_close_policy = "shadow"\n')
    out = tmp_path / "out"
    # Inherited production overrides and webhook settings cannot escape replay.
    env = {**os.environ, "SIGNALS_CONFIG": "/does/not/exist", "ENRICH_OFFLINE": "0",
           "ENRICH_FIXTURE": "/does/not/exist", "NOTIFY_WEBHOOK_URL": "https://invalid.example"}
    result = run_replay(source, out, "--config", str(config), "--mcap-mode", "fixed:200000",
                        "--compare", "--label", "maxgain", env=env)
    assert result.returncode == 0, result.stderr
    report = json.loads((out / "report.json").read_text())
    assert report["accepted"] == report["pushes"] == 4
    assert report["tokens_pushed"] == 3
    assert report["dup_push_rate"] == .25
    assert report["rejection_reasons"] == {"bad_input": 1, "duplicate": 1, "sell_skip": 25}
    assert report["reject_by_reason"]["sell_skip"] == {"total": 25, "terminal": 25, "non_terminal": 0}
    assert sum(v["total"] for v in report["reject_by_reason"].values()) == report["rejected"]
    assert report["n"] == 3
    assert report["unlabeled_pushes"] == 1
    assert report["winner_recall"] == {"numerator": 1, "denominator": 1, "rate": 1}
    assert report["precision_50"] == {"numerator": 2, "denominator": 3, "rate": 2 / 3}
    assert report["would_reject"]["dev_close"]["winners"] == 1
    assert report["comparison"]["baseline"]["accepted"] == 3
    assert report["comparison"]["delta"]["pushes"] == 1
    assert report["comparison"]["baseline"]["input_sha256"] == report["input_sha256"]
    accepted = [json.loads(line) for line in (out / "latest.jsonl").read_text().splitlines()]
    assert all(row["mcap"] == row["mcap_eval"] == 200000 for row in accepted)
    assert all("winner" not in row and "label" not in row for row in accepted)
    assert '"sink": "webhook"' not in (out / "state/events.jsonl").read_text()
    # Label collection neither changes decisions nor adds flags/output labels.
    plain = tmp_path / "plain"
    result = run_replay(source, plain, "--config", str(config), "--mcap-mode", "fixed:200000")
    assert result.returncode == 0, result.stderr
    plain_report = json.loads((plain / "report.json").read_text())
    assert "winner_recall" not in plain_report
    assert plain_report["accepted"] == report["accepted"]
    assert plain_report["rejection_reasons"] == report["rejection_reasons"]
    plain_rows = [json.loads(line) for line in (plain / "latest.jsonl").read_text().splitlines()]
    def decisions(rows):
        return [(r["signal_id"], [f for f in r["filter_flags"] if not f.startswith("late:")]) for r in rows]
    assert decisions(plain_rows) == decisions(accepted)
    assert (source / "signals.jsonl").read_text() == payload
    assert list(source.iterdir()) == [source / "signals.jsonl"]
    assert config.read_text().startswith('[filter]\nbaseline_dev_close_gate = false')


def test_explicit_comparison_config_and_legacy_reason_mapping(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "signals.json").write_text(json.dumps([signal("large", maxGain=200), signal("small", mcap=10000)]))
    config = tmp_path / "baseline.toml"
    config.write_text('[filter]\nmcap_scale_policy = "off"\n')
    out = tmp_path / "out"
    result = run_replay(source, out, "--mcap-mode", "fixture", "--compare", str(config), "--label", "maxgain")
    assert result.returncode == 0, result.stderr
    report = json.loads((out / "report.json").read_text())
    assert report["accepted"] == 0
    assert report["rejection_reasons"] == {"mcap_out_of_range": 2}
    assert report["reject_by_reason"]["mcap_above_max"]["terminal"] == 1
    assert report["reject_by_reason"]["mcap_below_min"]["non_terminal"] == 1
    assert report["comparison"]["baseline"]["config"] == str(config)
    assert report["winner_recall"]["rate"] == 0
    assert report["precision_50"]["rate"] is None
    assert report["first_seen_to_push"]["p50"] is None


@pytest.mark.parametrize("mode", ["fixed:0", "fixed:-1", "fixed:nan", "fixed:inf", "fixed:bad", "200000"])
def test_invalid_mcap_mode_does_not_create_output(tmp_path, mode):
    source = tmp_path / "source"
    source.mkdir()
    out = tmp_path / "out"
    result = run_replay(source, out, "--mcap-mode", mode)
    assert result.returncode != 0
    assert "positive finite" in result.stderr
    assert not out.exists()


def test_replay_refuses_unsafe_paths_and_bad_config(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    nested = source / "nested"
    link = tmp_path / "link"
    link.symlink_to(source, target_is_directory=True)
    for out in (source, nested, link, REPO / "state" / "replay-must-not-exist"):
        assert run_replay(source, out).returncode != 0
    assert not nested.exists()
    out = tmp_path / "out"
    missing = tmp_path / "missing.toml"
    assert run_replay(source, out, "--config", str(missing)).returncode != 0
    missing.write_text("invalid toml [")
    assert run_replay(source, out, "--compare", str(missing)).returncode != 0
    assert not out.exists()


def test_empty_labeled_replay(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    out = tmp_path / "out"
    result = run_replay(source, out, "--label", "maxgain")
    assert result.returncode == 0, result.stderr
    report = json.loads((out / "report.json").read_text())
    assert report["n"] == report["pushes"] == report["tokens_pushed"] == 0
    assert report["winner_recall"]["rate"] is None
    assert report["reject_by_reason"] == {}
