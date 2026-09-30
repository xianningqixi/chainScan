import io
import json
import os
import subprocess
import sys

import pytest

from conftest import REPO
from store import Store
from test_filter_signal import signal


@pytest.mark.parametrize("suffix", [".jsonl", ".jsonl.processing"])
@pytest.mark.parametrize("bad,error", [
    ("not json", "JSONDecodeError"),
    (json.dumps(signal(ca=["invalid"])), "BadInput"),
])
def test_bad_file_does_not_block_good_file(tmp_path, suffix, bad, error):
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / ("a_bad" + suffix)).write_text(bad)
    (inbox / ("b_good" + suffix)).write_text(json.dumps(signal()))
    env = {**os.environ, "SIGNALS_ROOT": str(tmp_path)}

    def run():
        result = subprocess.run(
            [sys.executable, str(REPO / "bin/ingest.py")],
            env=env, check=True, capture_output=True, text=True, timeout=20,
        )
        return json.loads(result.stdout)

    report = run()
    assert report["accepted"] == report["rejected"] == 1
    assert report["files"] == 2
    rejection = report["rejected_detail"][0]
    assert rejection["reason"] == "bad_input"
    assert rejection["error"].startswith(error + ":")
    failed = list((inbox / "failed").glob("*_a_bad.jsonl"))
    assert len(failed) == 1
    assert failed[0].read_text() == bad
    reason = failed[0].with_name(failed[0].name + ".error.json")
    assert json.loads(reason.read_text()) == rejection
    assert len(list((inbox / "done").glob("*_b_good.jsonl"))) == 1
    assert not list(inbox.glob("*.processing"))
    assert not list(inbox.glob("*.jsonl"))

    latest = tmp_path / "latest.jsonl"
    batches = list((tmp_path / "outbox").glob("push_*.jsonl"))
    assert len(batches) == 1
    rows = [json.loads(line) for line in latest.read_text().splitlines()]
    assert [row["signal_id"] for row in rows] == [signal()["signal_id"]]
    assert batches[0].read_bytes() == latest.read_bytes()
    pending = tmp_path / "outbox/PENDING_CHAT"
    assert "ALPHA" in pending.read_text()
    snapshots = {p: p.read_bytes() for p in [latest, batches[0], pending, failed[0], reason]}

    second = run()
    assert second["accepted"] == second["rejected"] == second["files"] == 0
    assert second["rejected_detail"] == []
    assert list((tmp_path / "outbox").glob("push_*.jsonl")) == batches
    assert all(p.read_bytes() == content for p, content in snapshots.items())


@pytest.mark.parametrize("format", ["jsonl", "array"])
def test_non_object_entries_are_bad_input(tmp_path, format):
    import ingest

    items = ["just a string", None, 1, True, [], [["ca", "PublicAddresspump"]], signal()]
    path = tmp_path / "mixed.jsonl"
    path.write_text(json.dumps(items) if format == "array" else
                    "\n".join(json.dumps(item) for item in items))
    accepted, rejected = ingest.process_file(path, Store(tmp_path))
    assert [s["signal_id"] for s in accepted] == [signal()["signal_id"]]
    assert len(rejected) == 6
    assert all(r["reason"] == "bad_input" and r["signal_id"] is None for r in rejected)
    location = "entry" if format == "array" else "line"
    assert [r[location] for r in rejected] == list(range(1, 7))


@pytest.mark.parametrize("body,status", [
    (b"not json", 400), (b"", 400), (b'{"incomplete":', 400),
    (b'"just a string"', 400), (b"null", 400), (b"123", 400),
    (b"true", 400), (b'{"invalid":"\xff"}', 400),
    (b'{"signal_id":"queued"}', 200),
    (b'[{"signal_id":"first"},{"signal_id":"second"}]', 200),
])
def test_webhook_validates_before_enqueue(tmp_path, monkeypatch, body, status):
    import webhook_server

    inbox = tmp_path / "inbox"
    monkeypatch.setattr(webhook_server, "INBOX", inbox)
    handler = object.__new__(webhook_server.H)
    handler.path = "/ingest"
    handler.headers = {"Content-Length": str(len(body))}
    handler.rfile = io.BytesIO(body)
    handler.wfile = io.BytesIO()
    responses = []
    monkeypatch.setattr(handler, "send_response", responses.append)
    monkeypatch.setattr(handler, "send_header", lambda *args: None)
    monkeypatch.setattr(handler, "end_headers", lambda: None)

    handler.do_POST()

    assert responses == [status]
    response = json.loads(handler.wfile.getvalue())
    assert response["ok"] is (status == 200)
    if status == 400:
        assert "queued" not in response
        assert not inbox.exists()
    else:
        queued = list(inbox.glob("*.jsonl"))
        assert len(queued) == 1
        expected = json.loads(body)
        assert [json.loads(line) for line in queued[0].read_text().splitlines()] == (
            expected if isinstance(expected, list) else [expected]
        )


def test_webhook_rejects_oversized_body(tmp_path, monkeypatch):
    import webhook_server

    inbox = tmp_path / "inbox"
    monkeypatch.setattr(webhook_server, "INBOX", inbox)
    handler = object.__new__(webhook_server.H)
    handler.path = "/ingest"
    handler.headers = {"Content-Length": str(webhook_server.MAX_BODY_BYTES + 1)}
    handler.rfile = io.BytesIO()
    handler.wfile = io.BytesIO()
    responses = []
    monkeypatch.setattr(handler, "send_response", responses.append)
    monkeypatch.setattr(handler, "send_header", lambda *args: None)
    monkeypatch.setattr(handler, "end_headers", lambda: None)

    handler.do_POST()

    assert responses == [413]
    assert json.loads(handler.wfile.getvalue()) == {
        "ok": False, "error": "body_too_large"
    }
    assert not inbox.exists()


def test_webhook_rejects_non_ascii_token_as_unauthorized(tmp_path, monkeypatch):
    import webhook_server

    monkeypatch.setenv("SIGNAL_WEBHOOK_TOKEN", "expected-token")
    monkeypatch.delenv("WEBHOOK_TOKEN", raising=False)
    monkeypatch.setattr(webhook_server, "INBOX", tmp_path / "inbox")
    handler = object.__new__(webhook_server.H)
    handler.path = "/ingest"
    handler.headers = {"Content-Length": "0", "X-Signal-Token": "é"}
    handler.rfile = io.BytesIO()
    handler.wfile = io.BytesIO()
    responses = []
    monkeypatch.setattr(handler, "send_response", responses.append)
    monkeypatch.setattr(handler, "send_header", lambda *args: None)
    monkeypatch.setattr(handler, "end_headers", lambda: None)

    handler.do_POST()

    assert responses == [401]
    assert json.loads(handler.wfile.getvalue()) == {
        "ok": False, "error": "unauthorized"
    }


@pytest.mark.parametrize("format", ["jsonl", "array"])
def test_good_entries_survive_bad_middle_entry(tmp_path, format):
    import ingest
    path = tmp_path / "mixed.jsonl"
    good = [signal(signal_id="before"), signal(signal_id="after")]
    if format == "array":
        path.write_text(json.dumps([good[0], signal(ca=["invalid"]), good[1]]))
    else:
        path.write_bytes(json.dumps(good[0]).encode() + b"\n\ngarbage\n\xff\n" + json.dumps(good[1]).encode())
    accepted, rejected = ingest.process_file(path, Store(tmp_path))
    assert [s["signal_id"] for s in accepted] == ["before", "after"]
    assert [r.get("entry", r.get("line")) for r in rejected] == ([2] if format == "array" else [3, 4])
    assert all(r["reason"] == "bad_input" for r in rejected)
    assert len(list((tmp_path / "failed").glob("*.error.json"))) == len(rejected)


@pytest.mark.parametrize("error", [OSError("disk full"), __import__('sqlite3').OperationalError("database locked"), RuntimeError("unexpected")])
def test_infrastructure_error_retains_claim_for_retry(tmp_path, monkeypatch, error):
    import ingest
    for name, value in {"ROOT": tmp_path, "INBOX": tmp_path / "inbox", "OUTBOX": tmp_path / "outbox",
                        "STATE": tmp_path / "state", "LATEST": tmp_path / "latest.jsonl"}.items():
        monkeypatch.setattr(ingest, name, value)
    monkeypatch.setattr(ingest, "notify_outbox_best_effort", lambda: None)
    ingest.INBOX.mkdir()
    rows = [signal(signal_id=sid) for sid in ("before", "retry", "after")]
    original = "\n".join(json.dumps(s) for s in rows)
    (ingest.INBOX / "mixed.jsonl").write_text(original)
    process = ingest.process_signal

    def failing(raw, store):
        if raw["signal_id"] == "retry":
            raise error
        return process(raw, store)

    monkeypatch.setattr(ingest, "process_signal", failing)
    report = ingest.main()
    assert report["errors"] and not report["rejected_detail"]
    assert (ingest.INBOX / "mixed.jsonl.processing").read_text() == original
    assert not (ingest.INBOX / "failed").exists()
    monkeypatch.setattr(ingest, "process_signal", process)
    assert not ingest.main()["errors"]
    published = [json.loads(line)["signal_id"] for line in ingest.LATEST.read_text().splitlines()]
    assert published == ["before", "retry", "after"]
    assert not list(ingest.INBOX.glob("*.processing"))
