import json
import os
from pathlib import Path
import subprocess
import sys

from conftest import REPO


def test_two_producers_and_ingesters(tmp_path):
    env = {**os.environ, "SIGNALS_ROOT": str(tmp_path), "PYTHONPATH": str(REPO / "bin")}
    producer = '''
import json, sys
from pathlib import Path
from common import ROOT, atomic_write
import ingest
for i in range(100):
    sid = sys.argv[1] + '-' + str(i)
    sig = dict(schema_version=1, signal_id=sid, ca='PublicAddresspump', chain='sol',
               token_name='ALPHA', direction='buy', mcap=100000, smart_money_count=1)
    atomic_write(ROOT / 'inbox' / (sid + '.jsonl'), json.dumps(sig) + '\\n')
    if i % 20 == 0:
        ingest.main()
ingest.main()
'''
    processes = [subprocess.Popen([sys.executable, "-c", producer, str(i)], env=env,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for i in range(2)]
    for process in processes:
        stdout, stderr = process.communicate(timeout=40)
        assert process.returncode == 0, (stdout, stderr)
    subprocess.run([sys.executable, str(REPO / "bin/ingest.py")], env=env, check=True, capture_output=True)
    latest = [json.loads(s) for s in (tmp_path / "latest.jsonl").read_text().splitlines()]
    pushed = [json.loads(s) for p in (tmp_path / "outbox").glob("push_*.jsonl") for s in p.read_text().splitlines()]
    expected = {f"{i}-{j}" for i in range(2) for j in range(100)}
    assert len(latest) == len(pushed) == 200
    assert {s["signal_id"] for s in latest} == {s["signal_id"] for s in pushed} == expected
    assert len(list((tmp_path / "inbox/done").iterdir())) == 200
    assert not list((tmp_path / "inbox").glob("*.processing"))


def test_webhook_only_queues(tmp_path, monkeypatch):
    import io
    import webhook_server
    monkeypatch.setattr(webhook_server, "INBOX", tmp_path)
    handler = object.__new__(webhook_server.H)
    raw = b'{"signal_id":"queued"}'
    handler.path = "/ingest"
    handler.headers = {"Content-Length": str(len(raw))}
    handler.rfile = io.BytesIO(raw)
    handler.wfile = io.BytesIO()
    monkeypatch.setattr(handler, "send_response", lambda *a: None)
    monkeypatch.setattr(handler, "send_header", lambda *a: None)
    monkeypatch.setattr(handler, "end_headers", lambda: None)
    handler.do_POST()
    response = json.loads(handler.wfile.getvalue())
    assert response["ok"] is True
    assert json.loads(Path(response["queued"]).read_text()) == {"signal_id": "queued"}
    assert not (tmp_path / "latest.jsonl").exists()
