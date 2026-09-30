import os
import sys
import tempfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "bin"))
# Set before importing pipeline modules; tests must never use production state.
_root = tempfile.TemporaryDirectory(prefix="signals-tests-")
os.environ["SIGNALS_ROOT"] = _root.name
os.environ["ENRICH_OFFLINE"] = "1"
os.environ["FOMO_VERIFY_DRY"] = "1"
os.environ.pop("NOTIFY_WEBHOOK_URL", None)


@pytest.fixture
def addon_signal():
    """A valid, timeless signal with no incidental format/quality flags."""
    return {"signal_id": "addon-1", "token_name": "ALPHA", "ca": "0x" + "a1" * 20,
            "chain": "eth", "status": "active", "direction": "buy", "mcap": 200000,
            "smart_money_count": 3, "buy_usd": None, "price": 1, "tags": []}


@pytest.fixture(autouse=True)
def no_network_or_secrets(monkeypatch):
    import socket

    def blocked(*args, **kwargs):
        raise AssertionError("network disabled in tests")

    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket.socket, "connect_ex", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    original = Path.open

    def safe_open(path, *args, **kwargs):
        if path.suffix == ".env" or path.name == "cookies.json":
            raise AssertionError("secret files disabled in tests")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", safe_open)
