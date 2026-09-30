"""Guard against trading integrations while permitting SQLite execute calls."""
import re
import pytest
from conftest import REPO

PROHIBITED = re.compile(r"\b(?:swap|sign_?transaction|private_?key|place_?order|create_?order|send_?transaction|send_?raw_?transaction|auto_trade|mnemonic|keypair|secret_?key)\b", re.I)


def test_no_trading_calls_or_secrets():
    for path in (REPO / "bin").rglob("*.py"):
        assert not PROHIBITED.search(path.read_text()), path.name


@pytest.mark.parametrize("name", ["signTransaction", "sign_transaction", "mnemonic", "keyPair", "secretKey", "secret_key"])
def test_guard_detects_camel_and_snake_spellings(name):
    assert PROHIBITED.search(f"client.{name}()")
