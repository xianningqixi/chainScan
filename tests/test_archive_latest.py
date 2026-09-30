import json

from archive_latest import archive_latest


def _row(ts, signal_id):
    return {"schema_version": 1, "signal_id": signal_id, "ts": ts}


def test_archive_months_is_idempotent_and_preserves_latest(tmp_path):
    latest = tmp_path / "latest.jsonl"
    lines = [
        json.dumps(_row("2026-01-31T23:30:00+08:00", "jan"), separators=(",", ":")) + "\n",
        json.dumps(_row("2026-02-01T00:01:00+08:00", "feb"), separators=(",", ":")) + "\n",
    ]
    latest.write_text("".join(lines), encoding="utf-8")
    before = latest.read_bytes()

    result = archive_latest(latest, tmp_path / "archive", threshold_bytes=1)

    assert result["archived"] is True
    assert result["months"]["202601"]["rows"] == 1
    assert result["months"]["202602"]["rows"] == 1
    assert (tmp_path / "archive/latest_202601.jsonl").read_text() == lines[0]
    assert (tmp_path / "archive/latest_202602.jsonl").read_text() == lines[1]
    assert latest.read_bytes() == before

    first_archives = {p.name: p.read_bytes() for p in (tmp_path / "archive").iterdir()}
    second = archive_latest(latest, tmp_path / "archive", threshold_bytes=1)
    assert second["archived"] is True
    assert {p.name: p.read_bytes() for p in (tmp_path / "archive").iterdir()} == first_archives
    assert latest.read_bytes() == before


def test_archive_skips_below_threshold_and_does_not_create_archive(tmp_path):
    latest = tmp_path / "latest.jsonl"
    latest.write_text(json.dumps(_row("2026-09-30T00:00:00+08:00", "one")) + "\n")

    result = archive_latest(latest, tmp_path / "archive", threshold_bytes=latest.stat().st_size)

    assert result["reason"] == "below_threshold"
    assert result["archived"] is False
    assert not (tmp_path / "archive").exists()


def test_archive_reports_unparseable_rows_without_rewriting_source(tmp_path):
    latest = tmp_path / "latest.jsonl"
    data = b'{"ts":"not-a-date","signal_id":"bad"}\n{"ts":"2026-03-01T00:00:00+08:00","signal_id":"ok"}\n'
    latest.write_bytes(data)

    result = archive_latest(latest, tmp_path / "archive", threshold_bytes=0)

    assert result["archived"] is True
    assert result["skipped_rows"] == 1
    assert (tmp_path / "archive/latest_202603.jsonl").read_bytes().endswith(b'"ok"}\n')
    assert latest.read_bytes() == data
