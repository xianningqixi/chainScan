import json
from unittest.mock import MagicMock
from urllib.error import HTTPError, URLError

import pytest
import common
import notify_outbox as notify
from test_filter_signal import signal


@pytest.fixture
def outbox(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "ROOT", tmp_path)
    monkeypatch.setattr(notify, "ROOT", tmp_path)
    for key, path in {"OUTBOX": "outbox", "NEW": "outbox/NEW", "LAST": "outbox/last_notify.txt",
                      "PENDING": "outbox/PENDING_CHAT", "OFFSET": "outbox/PENDING_CHAT.offset",
                      "DONE_DIR": "outbox/notified"}.items():
        monkeypatch.setattr(notify, key, tmp_path / path)
    notify.OUTBOX.mkdir()
    return notify.OUTBOX


def test_format_signal():
    text = notify.format_signal(signal(source_url="https://example.org/token",
                                      fomo_verify={"summary":"matched", "hits":2}))
    assert text == ("🔔 新信号 ALPHA (sol)\nCA: PublicAddresspump\n聪明钱: 1 | 方向: buy\n"
                    "FOMO核验: matched(hits=2)\n链接: https://example.org/token")
    assert "FOMO核验: quota_exceeded" in notify.format_signal(signal(notes="fomo_verify=quota_exceeded"))


@pytest.mark.parametrize("notify_each", [False, True])
def test_three_batches_survive(outbox, notify_each):
    for i in range(3):
        batch = outbox / f"push_20260930_00000{i}.jsonl"
        batch.write_text(json.dumps(signal(signal_id=str(i))) + "\n")
        with notify.NEW.open("a") as f:
            f.write(str(batch) + "\n")
        if notify_each:
            assert notify.run()["signals"] == 1
    if not notify_each:
        assert notify.run()["signals"] == 3
    assert notify.PENDING.read_text().count("### batch push_") == 3
    assert notify.PENDING.read_text().count("🔔 新信号 ALPHA") == 3
    assert notify.OFFSET.read_text() == "0\n"
    notify.OFFSET.write_text("123\n")
    assert notify.run()["signals"] == 0
    assert notify.OFFSET.read_text() == "123\n"
    assert not notify.NEW.exists()


def test_failed_batch_stays_queued(outbox):
    notify.NEW.write_text("missing.jsonl\n")
    assert not notify.run()["ok"]
    assert notify.NEW.read_text() == "missing.jsonl\n"


def queue_batch(outbox, name="push_20260930_000001.jsonl", rows=None):
    batch = outbox / name
    batch.write_text("".join(json.dumps(row) + "\n" for row in
                             (rows if rows is not None else [signal()])), encoding="utf-8")
    with notify.NEW.open("a", encoding="utf-8") as f:
        f.write(name + "\n")
    return batch


def events():
    return [json.loads(line) for line in
            (notify.ROOT / "state/events.jsonl").read_text().splitlines()]


@pytest.mark.parametrize("url", [None, ""])
def test_webhook_default_off_exact_file_behavior(outbox, monkeypatch, url):
    if url is None:
        monkeypatch.delenv("NOTIFY_WEBHOOK_URL", raising=False)
    else:
        monkeypatch.setenv("NOTIFY_WEBHOOK_URL", url)
    webhook = MagicMock(side_effect=AssertionError("webhook must remain disabled"))
    monkeypatch.setattr(notify, "WebhookSink", webhook)
    http = MagicMock(side_effect=AssertionError("unexpected HTTP"))
    monkeypatch.setattr(notify.urllib.request, "urlopen", http)
    clock = MagicMock()
    clock.now.return_value.isoformat.return_value = "2026-09-30T12:00:00+08:00"
    monkeypatch.setattr(notify, "datetime", clock)
    batch = queue_batch(outbox)
    notify.PENDING.write_text("previous batch\n", encoding="utf-8")
    notify.OFFSET.write_text("17\n")

    assert notify.run() == {"ok": True, "signals": 1, "last_notify": str(notify.LAST),
                            "pending": True, "queued_batches": 0}
    body = (f"### batch {batch.name} 2026-09-30T12:00:00+08:00\n"
            "🔔 新信号 ALPHA (sol)\nCA: PublicAddresspump\n聪明钱: 1 | 方向: buy\n"
            "FOMO核验: n/a\n\n")
    assert notify.PENDING.read_bytes() == ("previous batch\n" + body).encode("utf-8")
    assert notify.LAST.read_bytes() == body.encode("utf-8")
    assert notify.OFFSET.read_text() == "17\n"
    assert not notify.NEW.exists()
    assert [p.read_text() for p in notify.DONE_DIR.iterdir()] == [batch.name + "\n"]
    assert [row["event"] for row in events()] == ["notify"]
    webhook.assert_not_called()
    http.assert_not_called()


@pytest.mark.parametrize("status", [200, 204])
def test_webhook_success_sends_only_identical_notification_text(outbox, monkeypatch, status):
    url = "https://example.invalid/notify"
    monkeypatch.setenv("NOTIFY_WEBHOOK_URL", url)
    response = MagicMock()
    response.__enter__.return_value.status = status
    http = MagicMock(return_value=response)
    monkeypatch.setattr(notify.urllib.request, "urlopen", http)
    queue_batch(outbox, rows=[signal(source_url="https://example.org/token",
                                    fomo_verify={"summary": "matched", "hits": 2},
                                    execution={"action": "ignored"}, order={"amount": 42}),
                              signal(token_name="BETA", direction="watch")])

    assert notify.run()["signals"] == 2
    http.assert_called_once()
    request = http.call_args.args[0]
    assert http.call_args.kwargs == {"timeout": 3}
    assert request.full_url == url
    assert request.get_method() == "POST"
    assert request.get_header("Content-type") == "application/json; charset=utf-8"
    body = notify.PENDING.read_text(encoding="utf-8")
    assert json.loads(request.data.decode("utf-8")) == {"text": body}
    assert "\n\n---\n\n" in body
    assert "FOMO核验: matched(hits=2)" in body
    assert "链接: https://example.org/token" in body
    assert "execution" not in body and "order" not in body and "ignored" not in body
    assert notify.LAST.read_text(encoding="utf-8") == body
    assert not notify.NEW.exists()
    response.__exit__.assert_called_once()
    assert [row["event"] for row in events()] == ["notify"]
    assert notify.run()["reason"] == "no_NEW"
    http.assert_called_once()


@pytest.mark.parametrize("error", [
    TimeoutError("sensitive endpoint details"),
    URLError(TimeoutError("sensitive endpoint details")),
    URLError("sensitive endpoint details"),
    HTTPError("https://example.invalid/secret", 503, "sensitive endpoint details", None, None),
    ValueError("sensitive endpoint details"),
])
def test_webhook_failure_keeps_files_and_queue_semantics(outbox, monkeypatch, error):
    monkeypatch.setenv("NOTIFY_WEBHOOK_URL", "https://example.invalid/secret")
    calls = []

    def fail(request, timeout):
        # File delivery must already be durable when HTTP starts.
        body = json.loads(request.data)["text"]
        assert notify.PENDING.read_text(encoding="utf-8").endswith(body)
        assert timeout == 3
        calls.append(body)
        raise error

    monkeypatch.setattr(notify.urllib.request, "urlopen", fail)
    first = queue_batch(outbox)
    second = queue_batch(outbox, "push_20260930_000002.jsonl")
    with notify.NEW.open("a") as f:
        f.write(first.name + "\nmissing.jsonl\n")
    notify.OFFSET.write_text("23\n")

    result = notify.run()
    assert result["signals"] == 2 and result["queued_batches"] == 1 and not result["ok"]
    assert len(calls) == 2
    assert notify.PENDING.read_text(encoding="utf-8") == "".join(calls)
    assert notify.LAST.read_text(encoding="utf-8") == "".join(calls)
    assert notify.OFFSET.read_text() == "23\n"
    assert notify.NEW.read_text() == "missing.jsonl\n"
    assert [p.read_text() for p in notify.DONE_DIR.iterdir()] == [first.name + "\n" + second.name + "\n"]
    failures = [row for row in events() if row["event"] == "notify_error"]
    assert len(failures) == 2
    assert all(row["sink"] == "webhook" and row["error"] == type(error).__name__ for row in failures)
    if isinstance(error, HTTPError):
        assert all(row["status"] == 503 for row in failures)
    assert "secret" not in json.dumps(events())
    assert "sensitive" not in json.dumps(events())
    assert events()[-1]["event"] == "notify" and events()[-1]["n"] == 2
    notify.run()
    assert len(calls) == 2  # Failed webhooks do not requeue delivered batches.


def test_webhook_skips_empty_and_unreadable_batches(outbox, monkeypatch):
    monkeypatch.setenv("NOTIFY_WEBHOOK_URL", "https://example.invalid/notify")
    http = MagicMock(side_effect=AssertionError("unexpected HTTP"))
    monkeypatch.setattr(notify.urllib.request, "urlopen", http)
    empty = queue_batch(outbox, rows=[])
    bad = queue_batch(outbox, "bad.jsonl")
    bad.write_text("{broken\n")
    assert notify.run()["signals"] == 0
    assert notify.NEW.read_text() == bad.name + "\n"
    assert [p.read_text() for p in notify.DONE_DIR.iterdir()] == [empty.name + "\n"]
    assert not notify.PENDING.exists()
    http.assert_not_called()


def test_file_failure_preserves_new_and_does_not_send_webhook(outbox, monkeypatch):
    monkeypatch.setenv("NOTIFY_WEBHOOK_URL", "https://example.invalid/notify")
    http = MagicMock()
    monkeypatch.setattr(notify.urllib.request, "urlopen", http)
    queue_batch(outbox)
    original_new = notify.NEW.read_bytes()
    notify.PENDING.mkdir()  # Force a real file-open failure.
    with pytest.raises(OSError):
        notify.run()
    assert notify.NEW.read_bytes() == original_new
    assert not notify.DONE_DIR.exists()
    http.assert_not_called()
