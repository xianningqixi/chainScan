"""SQLite evaluation state and a small durable journal for file delivery."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time

from common import ROOT, TZ, load_config
from filter_signal import EvalContext, FilterResult, parse_num


_MAX_GAIN_RE = re.compile(r"(?:^|;)\s*maxGain\s*=\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+))\s*%", re.I)


def signal_max_gain_percent(sig):
    """Return a post-hoc maxGain label in percent, if present."""
    match = _MAX_GAIN_RE.search(str(sig.get("notes") or ""))
    if match:
        try:
            value = float(match.group(1))
            return value if math.isfinite(value) else None
        except (TypeError, ValueError, OverflowError):
            return None
    for key in ("maxGain", "maxgain", "max_gain"):
        value = parse_num(sig.get(key))
        if value is not None and math.isfinite(value):
            return value
    return None


def token_key(chain, ca):
    chain = str(chain or "").lower().strip()
    chain = {"501": "sol", "ct_501": "sol", "solana": "sol", "56": "bsc",
             "1": "eth", "ethereum": "eth", "8453": "base", "42161": "arb",
             "10": "op", "polygon": "matic"}.get(chain, chain)
    ca = str(ca or "").strip()
    return chain, ca if chain == "sol" else ca.lower()


def _different_known_source(previous, current):
    """Return true only when both source values are known and differ.

    ``source`` was added after the first rows were written, so historical
    pushed rows legitimately contain NULL. Unknown source must not become a
    cross-source claim during the upgrade; otherwise a same-source replay can
    be rejected as a false duplicate.
    """
    previous = str(previous).strip() if previous is not None else ""
    current = str(current).strip() if current is not None else ""
    return bool(previous and current and previous != current)


class Store:
    def __init__(self, root: Path = ROOT, cfg=None, clock=None):
        self.root = Path(root)
        self.cfg = load_config() if cfg is None else cfg
        self.clock = clock or time.time
        self.path = self.root / "state/pipeline.db"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS signals (
                    signal_id TEXT PRIMARY KEY, first_seen_ts REAL NOT NULL,
                    last_fingerprint TEXT, eval_count INTEGER NOT NULL DEFAULT 0,
                    last_reason TEXT, pushed_at REAL);
                CREATE TABLE IF NOT EXISTS mcap_cache (
                    key TEXT PRIMARY KEY, mcap REAL, source TEXT, ts REAL, error TEXT);
                CREATE TABLE IF NOT EXISTS breaker (
                    name TEXT PRIMARY KEY, open_until REAL, reason TEXT);
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE IF NOT EXISTS pushes (
                    signal_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
                    batch TEXT, delivered INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS output_ids (
                    path TEXT, signal_id TEXT, PRIMARY KEY(path, signal_id));
                CREATE TABLE IF NOT EXISTS ws_fingerprints (
                    signal_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS tokens (
                    chain TEXT, ca_norm TEXT, first_seen_ts REAL NOT NULL,
                    last_push_ts REAL, last_push_smart REAL, last_push_sid TEXT,
                    PRIMARY KEY(chain, ca_norm));
                CREATE TABLE IF NOT EXISTS token_reservations (
                    chain TEXT, ca_norm TEXT, reserved_at REAL NOT NULL,
                    signal_id TEXT NOT NULL, source TEXT,
                    PRIMARY KEY(chain, ca_norm));
                CREATE TABLE IF NOT EXISTS evals (
                    signal_id TEXT, ts REAL, fp TEXT, verdict TEXT, reason TEXT,
                    layer TEXT, terminal INTEGER, flags_json TEXT,
                    would_reject_json TEXT, score REAL, mcap_eval REAL, mcap_source TEXT);
                CREATE INDEX IF NOT EXISTS evals_ts ON evals(ts);
            """)
            db.execute("BEGIN IMMEDIATE")
            for table, columns in {
                "signals": {"first_price": "REAL", "last_verdict": "TEXT", "last_terminal": "INTEGER",
                            "last_eval_ts": "REAL", "chain": "TEXT", "ca_norm": "TEXT",
                            "smart": "REAL", "last_seen_ts": "REAL", "last_status": "TEXT",
                            "last_max_gain": "REAL", "last_update_status": "TEXT",
                            "also_seen": "TEXT", "source": "TEXT"},
                "mcap_cache": {"price_at_fetch": "REAL", "liq_usd": "REAL", "pair_created_at": "TEXT"},
            }.items():
                existing = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
                for name, kind in columns.items():
                    if name not in existing:
                        db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {kind}")
            db.execute("CREATE INDEX IF NOT EXISTS signals_token ON signals(chain,ca_norm,last_seen_ts)")
            if not db.execute("SELECT 1 FROM metadata WHERE key='seen_migrated'").fetchone():
                seen = self.root / "state/seen_ids.txt"
                if seen.exists():
                    now = self.clock()
                    db.executemany("INSERT OR IGNORE INTO signals(signal_id,first_seen_ts,pushed_at,last_reason) VALUES(?,?,?,'legacy_seen')",
                                   ((sid.strip(), now, now) for sid in seen.read_text().splitlines() if sid.strip()))
                db.execute("INSERT INTO metadata VALUES('seen_migrated','1')")

    @contextmanager
    def connect(self, timeout=30):
        db = sqlite3.connect(self.path, timeout=timeout)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def ws_fingerprint(self, signal_id):
        # WS reception must not wait behind an ingest writer.
        with self.connect(timeout=0) as db:
            row = db.execute("SELECT fingerprint FROM ws_fingerprints WHERE signal_id=?",
                             (signal_id,)).fetchone()
        return row[0] if row else None

    def record_ws_fingerprint(self, signal_id, fingerprint):
        """Call only after the complete inbox file has been published."""
        with self.connect(timeout=0) as db:
            db.execute("INSERT INTO ws_fingerprints VALUES(?,?) ON CONFLICT(signal_id) "
                       "DO UPDATE SET fingerprint=excluded.fingerprint", (signal_id, fingerprint))

    def terminal(self, reason):
        reason = reason or ""
        if reason.split(":", 1)[0] in {
            "test_signal", "test_ca", "missing_ca", "chain_blocked", "sell_skip", "wash_trading_tag",
            "chain_unmapped", "bad_ca_format", "mcap_above_max"
        }:
            return True
        if reason.startswith("mcap_out_of_range:"):
            value = parse_num(reason.split(":", 1)[1])
            return value is not None and value > self.cfg["filter"]["mcap_max"]
        return False

    def should_evaluate(self, signal_id, fingerprint):
        with self.connect() as db:
            # Old P0 unknown rejections are still eligible within the window.
            if self.cfg["filter"].get("mcap_unknown_policy", "pending") == "pending":
                db.execute("UPDATE signals SET last_reason='mcap_pending' WHERE signal_id=? "
                           "AND last_reason='mcap_unknown' AND last_terminal IS NULL AND pushed_at IS NULL AND first_seen_ts >= ?",
                           (signal_id, self.clock() - self.cfg["reeval"]["window_min"] * 60))
            # Expire lazily on an update/retry; do not add a background worker.
            db.execute("UPDATE signals SET last_reason='mcap_unknown',last_terminal=1,last_verdict='reject' WHERE signal_id=? "
                       "AND last_reason='mcap_pending' AND pushed_at IS NULL AND first_seen_ts < ?",
                       (signal_id, self.clock() - self.cfg["reeval"]["window_min"] * 60))
            row = db.execute("SELECT * FROM signals WHERE signal_id=?", (signal_id,)).fetchone()
        if row is None:
            return True
        terminal = self.terminal(row["last_reason"]) if row["last_terminal"] is None else bool(row["last_terminal"])
        retry_due = (row["last_reason"] != "mcap_pending" or row["last_eval_ts"] is None
                     or self.clock() - row["last_eval_ts"] >= self.cfg["reeval"].get("pending_retry_s", 0))
        return (row["pushed_at"] is None and not terminal and retry_due
                and (row["last_fingerprint"] != fingerprint or row["last_reason"] == "mcap_pending")
                and row["eval_count"] < self.cfg["reeval"]["max_evals"]
                and self.clock() - row["first_seen_ts"] <= self.cfg["reeval"]["window_min"] * 60)

    def mcap_window_expired(self, signal_id, *, unknown_only=False):
        with self.connect() as db:
            row = db.execute("SELECT first_seen_ts,last_reason,pushed_at FROM signals WHERE signal_id=?", (signal_id,)).fetchone()
        return bool(row and (not unknown_only or (row[1] == "mcap_unknown" and row[2] is None))
                    and self.clock() - row[0] > self.cfg["reeval"]["window_min"] * 60)

    def observe(self, signal_id, sig=None):
        """Record first local observation before enrichment can add latency."""
        with self.connect() as db:
            db.execute("INSERT OR IGNORE INTO signals(signal_id,first_seen_ts) VALUES(?,?)",
                       (signal_id, self.clock()))
            previous = db.execute("SELECT last_status,last_max_gain,last_update_status,also_seen FROM signals WHERE signal_id=?",
                                  (signal_id,)).fetchone()
            if sig is not None:
                chain, ca = token_key(sig.get("chain"), sig.get("ca"))
                price = parse_num(sig.get("price"))
                price = price if price and math.isfinite(price) and price > 0 else None
                db.execute("UPDATE signals SET first_price=COALESCE(first_price,?),chain=?,ca_norm=?,smart=?,last_seen_ts=?,last_status=?,last_max_gain=?,source=? WHERE signal_id=?",
                           (price, chain, ca, parse_num(sig.get("smart_money_count")), self.clock(),
                            str(sig.get("status") or "active").lower(), signal_max_gain_percent(sig),
                            str(sig.get("source") or ""), signal_id))
                db.execute("INSERT OR IGNORE INTO tokens(chain,ca_norm,first_seen_ts) VALUES(?,?,?)", (chain, ca, self.clock()))
        return dict(previous) if previous else None

    def record_status_update(self, signal_id, previous, sig, threshold):
        """Claim informational updates for an already-pushed signal atomically."""
        if not previous or threshold is None:
            return []
        try:
            threshold = float(threshold)
        except (TypeError, ValueError, OverflowError):
            return []
        if not math.isfinite(threshold):
            return []
        status = str(sig.get("status") or "active").lower()
        gain = signal_max_gain_percent(sig)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT pushed_at,last_update_status FROM signals WHERE signal_id=?",
                             (signal_id,)).fetchone()
            if not row or row[0] is None:
                return []
            changes = []
            old_status = str(previous.get("last_status") or "active").lower()
            if status in {"expired", "invalid"} and status != old_status and status != row[1]:
                changes.append({"kind": "status", "status": status})
                db.execute("UPDATE signals SET last_update_status=? WHERE signal_id=?", (status, signal_id))
            old_gain = previous.get("last_max_gain")
            crossed = gain is not None and gain >= threshold and (old_gain is None or old_gain < threshold)
            if crossed:
                prior = db.execute("SELECT value FROM metadata WHERE key=?", ("gain_update:" + signal_id,)).fetchone()
                if prior is None:
                    changes.append({"kind": "maxGain", "value": gain, "threshold": threshold})
                    db.execute("INSERT INTO metadata(key,value) VALUES(?,?)", ("gain_update:" + signal_id, str(gain)))
        return changes

    def reserve_token(self, chain, ca, signal_id, source, within_s=1800):
        """Claim a short-lived inbox reservation to close adapter/ingest races."""
        chain, ca = token_key(chain, ca)
        if not chain or not ca or within_s <= 0:
            return False
        now = self.clock()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM token_reservations WHERE reserved_at < ?", (now - within_s,))
            pushed = db.execute("SELECT last_push_ts FROM tokens WHERE chain=? AND ca_norm=?", (chain, ca)).fetchone()
            if pushed and pushed[0] is not None and now - within_s <= pushed[0] <= now:
                return False
            row = db.execute("SELECT signal_id,source FROM token_reservations WHERE chain=? AND ca_norm=?",
                             (chain, ca)).fetchone()
            if (row and row[0] != signal_id
                    and _different_known_source(row[1], source)):
                return False
            # Same-source reservations may refresh/replace one another; the
            # reservation only exists to arbitrate cross-source races.
            db.execute("INSERT OR REPLACE INTO token_reservations VALUES(?,?,?,?,?)",
                       (chain, ca, now, signal_id, str(source or "")))
            return True

    def pushed_token_other_source(self, chain, ca, signal_id, source, within_s=1800):
        chain, ca = token_key(chain, ca)
        now = self.clock()
        with self.connect() as db:
            row = db.execute("SELECT t.last_push_ts,t.last_push_sid,s.source FROM tokens t "
                             "LEFT JOIN signals s ON s.signal_id=t.last_push_sid "
                             "WHERE t.chain=? AND t.ca_norm=?", (chain, ca)).fetchone()
        return bool(row and row[0] is not None and now - within_s <= row[0] <= now
                    and row[1] != signal_id and _different_known_source(row[2], source))

    def pending_token_other_source(self, chain, ca, signal_id, source, within_s=1800):
        chain, ca = token_key(chain, ca)
        now = self.clock()
        with self.connect() as db:
            row = db.execute("SELECT signal_id,source,reserved_at FROM token_reservations WHERE chain=? AND ca_norm=?",
                             (chain, ca)).fetchone()
        return bool(row and row[0] != signal_id and _different_known_source(row[1], source)
                    and now - within_s <= row[2] <= now)

    def release_token_reservation(self, signal_id):
        with self.connect() as db:
            db.execute("DELETE FROM token_reservations WHERE signal_id=?", (signal_id,))

    def record_also_seen(self, chain, ca, source="gmgn"):
        chain, ca = token_key(chain, ca)
        with self.connect() as db:
            row = db.execute("SELECT last_push_sid FROM tokens WHERE chain=? AND ca_norm=?", (chain, ca)).fetchone()
            if not row or not row[0]:
                return None
            sid = row[0]
            current = db.execute("SELECT also_seen FROM signals WHERE signal_id=?", (sid,)).fetchone()
            values = [x for x in str(current[0] or "").split(",") if x]
            if source not in values:
                values.append(str(source))
                db.execute("UPDATE signals SET also_seen=? WHERE signal_id=?", (",".join(values), sid))
            return sid

    def build_context(self, sig, now=None):
        now = self.clock() if now is None else now
        chain, ca = token_key(sig.get("chain"), sig.get("ca"))
        window = self.cfg["filter"].get("l2", {}).get("confluence_window_min", 60) * 60
        with self.connect() as db:
            row = db.execute("SELECT * FROM signals WHERE signal_id=?", (sig["signal_id"],)).fetchone()
            token = db.execute("SELECT * FROM tokens WHERE chain=? AND ca_norm=?", (chain, ca)).fetchone()
            others = db.execute("SELECT COUNT(*),MAX(smart) FROM signals WHERE chain=? AND ca_norm=? AND signal_id!=? AND last_seen_ts>=? AND last_seen_ts<=?",
                                (chain, ca, sig["signal_id"], now - window, now)).fetchone()
        return EvalContext(now, row["first_seen_ts"] if row else now,
                           row["first_price"] if row else parse_num(sig.get("price")),
                           row["eval_count"] if row else 0,
                           token["last_push_ts"] if token else None,
                           token["last_push_smart"] if token else None, others[0] + 1, others[1])

    def token_recent(self, chain, ca, within_s):
        """Read only: a push exists in [now - within_s, now].

        Reuse token_key so EVM aliases/case match and Solana case stays intact.
        Observations and rejected evaluations alone do not count as pushes.
        """
        chain, ca = token_key(chain, ca)
        if not chain or not ca or within_s <= 0:
            return False
        now = self.clock()
        with self.connect() as db:
            row = db.execute("SELECT last_push_ts FROM tokens WHERE chain=? AND ca_norm=?",
                             (chain, ca)).fetchone()
        return bool(row and row[0] is not None and now - within_s <= row[0] <= now)

    def record_eval(self, signal_id, fingerprint, reason=None, useful=None, *, result=None, mcap_source=None):
        if result is not None:
            if reason is not None:
                raise TypeError("supply either reason or result")
            reason = result
        if not isinstance(reason, (str, FilterResult)):
            raise TypeError("record_eval requires a reason or FilterResult")
        structured = isinstance(reason, FilterResult)
        result = reason if structured else FilterResult("accept" if useful else "pending" if reason == "mcap_pending" else "reject", reason, terminal=self.terminal(reason))
        with self.connect() as db:
            db.execute("""INSERT INTO signals(signal_id,first_seen_ts,last_fingerprint,eval_count,last_reason)
                          VALUES(?,?,?,1,?) ON CONFLICT(signal_id) DO UPDATE SET
                          last_fingerprint=excluded.last_fingerprint, eval_count=eval_count+1,
                          last_reason=excluded.last_reason""", (signal_id, self.clock(), fingerprint, result.reason))
            db.execute("UPDATE signals SET last_verdict=?,last_terminal=?,last_eval_ts=? WHERE signal_id=?",
                       (result.verdict, int(result.terminal) if structured else None, self.clock(), signal_id))
            db.execute("INSERT INTO evals VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                       (signal_id, self.clock(), fingerprint, result.verdict, result.reason, result.layer,
                        int(result.terminal), json.dumps(result.flags), json.dumps(result.would_reject),
                        result.score, result.mcap_eval, mcap_source))

    def _mark_pushed(self, db, signal_id, chain=None, ca=None, smart=None):
        db.execute("INSERT OR IGNORE INTO signals(signal_id,first_seen_ts) VALUES(?,?)", (signal_id, self.clock()))
        if chain is None and ca is None:
            observed = db.execute("SELECT chain,ca_norm,smart FROM signals WHERE signal_id=?", (signal_id,)).fetchone()
            chain, ca, smart = observed
        claimed = db.execute("UPDATE signals SET pushed_at=? WHERE signal_id=? AND pushed_at IS NULL",
                             (self.clock(), signal_id)).rowcount == 1
        if claimed and chain is not None and ca is not None:
            chain, ca = token_key(chain, ca)
            db.execute("INSERT INTO tokens VALUES(?,?,?,?,?,?) ON CONFLICT(chain,ca_norm) DO UPDATE SET "
                       "last_push_ts=excluded.last_push_ts,last_push_smart=excluded.last_push_smart,last_push_sid=excluded.last_push_sid",
                       (chain, ca, self.clock(), self.clock(), parse_num(smart), signal_id))
        return claimed

    def mark_pushed(self, signal_id, chain=None, ca=None, smart=None):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            return self._mark_pushed(db, signal_id, chain, ca, smart)

    def queue_push(self, sig):
        # Claim and payload are committed together: a claim cannot lose its output.
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if not self._mark_pushed(db, sig["signal_id"], sig.get("chain"), sig.get("ca"), sig.get("smart_money_count")):
                return False
            first_seen = db.execute("SELECT first_seen_ts FROM signals WHERE signal_id=?",
                                    (sig["signal_id"],)).fetchone()[0]
            sig["first_seen_ts"] = datetime.fromtimestamp(first_seen, TZ).isoformat()
            db.execute("INSERT INTO pushes(signal_id,payload) VALUES(?,?)",
                       (sig["signal_id"], json.dumps(sig, ensure_ascii=False)))
            db.execute("DELETE FROM token_reservations WHERE signal_id=?", (sig["signal_id"],))
            return True

    def existing_output_ids(self, path):
        """Incrementally reconcile append-only output with the durable index.

        A checkpoint only covers complete lines actually read. Re-reading after
        a crash is harmless; replacement/truncation rebuilds the fallback index.
        Malformed historical lines are left untouched and skipped.
        """
        key = str(path.resolve())
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
            checkpoint = json.loads(row[0]) if row else {}
            if not path.exists():
                db.execute("DELETE FROM output_ids WHERE path=?", (key,))
                db.execute("DELETE FROM metadata WHERE key=?", (key,))
                return set()
            with path.open("rb") as f:
                stat = os.fstat(f.fileno())
                identity = [stat.st_dev, stat.st_ino]
                offset = checkpoint.get("offset", 0)
                if checkpoint.get("identity") != identity or stat.st_size < offset:
                    db.execute("DELETE FROM output_ids WHERE path=?", (key,))
                    offset = 0
                f.seek(offset)
                while line := f.readline():
                    if line.endswith(b"\n"):
                        offset = f.tell()
                    try:
                        obj = json.loads(line)
                    except (ValueError, UnicodeDecodeError):
                        continue
                    if isinstance(obj, dict) and isinstance(obj.get("signal_id"), str):
                        db.execute("INSERT OR IGNORE INTO output_ids VALUES(?,?)", (key, obj["signal_id"]))
            db.execute("INSERT OR REPLACE INTO metadata VALUES(?,?)",
                       (key, json.dumps({"identity": identity, "offset": offset})))
            # Only undelivered journal entries can need recovery deduplication.
            return {r[0] for r in db.execute("""SELECT o.signal_id FROM output_ids o
                JOIN pushes p ON p.signal_id=o.signal_id WHERE o.path=? AND p.delivered=0""", (key,))}

    def update_pending_payload(self, sig):
        with self.connect() as db:
            db.execute("UPDATE pushes SET payload=? WHERE signal_id=? AND delivered=0",
                       (json.dumps(sig, ensure_ascii=False), sig["signal_id"]))

    def pending_pushes(self):
        with self.connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM pushes WHERE delivered=0 ORDER BY rowid")]

    def assign_batch(self, ids, batch):
        with self.connect() as db:
            db.executemany("UPDATE pushes SET batch=? WHERE signal_id=? AND batch IS NULL", ((batch, sid) for sid in ids))

    def delivered(self, ids):
        with self.connect() as db:
            db.executemany("UPDATE pushes SET delivered=1 WHERE signal_id=?", ((sid,) for sid in ids))

    def breaker_is_open(self, name):
        with self.connect() as db:
            row = db.execute("SELECT open_until FROM breaker WHERE name=?", (name,)).fetchone()
        return bool(row and row[0] > self.clock())

    def open_breaker(self, name, reason, cooldown):
        with self.connect() as db:
            db.execute("INSERT INTO breaker(name,open_until,reason) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET open_until=excluded.open_until,reason=excluded.reason",
                       (name, self.clock() + cooldown, reason))

    def backoff_breaker(self, name, base=1.0):
        """Persist consecutive 429s across clients/restarts, capped at 120s."""
        key = "mcap_backoff:" + name
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
            failures = min((int(row[0]) if row else 0) + 1, 32)
            cooldown = min(120.0, max(1.0, base) * 2 ** (failures - 1))
            db.execute("INSERT INTO breaker(name,open_until,reason) VALUES(?,?,'429') "
                       "ON CONFLICT(name) DO UPDATE SET open_until=excluded.open_until,"
                       "reason=excluded.reason", (name, self.clock() + cooldown))
            db.execute("INSERT OR REPLACE INTO metadata VALUES(?,?)", (key, str(failures)))
        return cooldown

    def reset_breaker(self, name):
        with self.connect() as db:
            db.execute("DELETE FROM breaker WHERE name=?", (name,))
            db.execute("DELETE FROM metadata WHERE key=?", ("mcap_backoff:" + name,))

    def take_mcap_token(self, source, rate):
        """Atomic one-token bucket shared by CLI and worker; return wait seconds."""
        key = "mcap_bucket:" + source
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            now = self.clock()
            row = db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
            previous = json.loads(row[0]) if row else {"ts": now, "tokens": 1.0}
            tokens = min(1.0, previous["tokens"] + max(0.0, now - previous["ts"]) * rate)
            wait = max(0.0, (1.0 - tokens) / rate)
            if not wait:
                tokens -= 1.0
            db.execute("INSERT OR REPLACE INTO metadata VALUES(?,?)",
                       (key, json.dumps({"ts": now, "tokens": tokens})))
        return wait

    def migrate_mcap_cache(self):
        """One-version, one-time import. Never modify the legacy JSON file."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM metadata WHERE key='mcap_cache_migrated'").fetchone():
                return
            path = self.root / "state/mcap_cache.json"
            try:
                entries = json.loads(path.read_text()) if path.exists() else {}
            except (ValueError, UnicodeDecodeError):
                entries = {}
            for key, entry in (entries.items() if isinstance(entries, dict) else []):
                if not isinstance(entry, dict):
                    continue
                try:
                    ts = float(entry.get("ts", 0))
                    mcap = float(entry["mcap"]) if entry.get("mcap") is not None else None
                except (TypeError, ValueError, OverflowError):
                    continue
                source, error = entry.get("source"), entry.get("error")
                if (not math.isfinite(ts) or (mcap is not None and (not math.isfinite(mcap) or mcap <= 0))
                        or (source is not None and (not isinstance(source, str) or source not in
                            {"dexscreener", "geckoterminal", "pumpfun", "binance"}))
                        or (mcap is not None and source is None)
                        or (error is not None and not isinstance(error, str))):
                    continue
                db.execute("INSERT OR IGNORE INTO mcap_cache(key,mcap,source,ts,error) VALUES(?,?,?,?,?)", (key, mcap, source, ts, error))
            db.execute("INSERT INTO metadata VALUES('mcap_cache_migrated','1')")

    def get_mcap_cache(self, key):
        with self.connect() as db:
            row = db.execute("SELECT * FROM mcap_cache WHERE key=?", (key,)).fetchone()
        return dict(row) if row else None

    def put_mcap_cache(self, key, mcap, source, error, **metadata):
        with self.connect() as db:
            db.execute("INSERT OR REPLACE INTO mcap_cache(key,mcap,source,ts,error,price_at_fetch,liq_usd,pair_created_at) VALUES(?,?,?,?,?,?,?,?)",
                       (key, mcap, source, self.clock(), error, metadata.get("price_at_fetch"), metadata.get("liq_usd"), metadata.get("pair_created_at")))


_default = None


def default_store():
    global _default
    if _default is None:
        _default = Store()
    return _default


def should_evaluate(signal_id, fingerprint):
    return default_store().should_evaluate(signal_id, fingerprint)


def record_eval(signal_id, fingerprint, reason=None, useful=None, *, result=None, mcap_source=None):
    return default_store().record_eval(signal_id, fingerprint, reason, useful,
                                       result=result, mcap_source=mcap_source)


def build_context(sig, now=None):
    return default_store().build_context(sig, now)


def mark_pushed(signal_id, chain=None, ca=None, smart=None):
    return default_store().mark_pushed(signal_id, chain, ca, smart)


def token_recent(chain, ca, within_s):
    return default_store().token_recent(chain, ca, within_s)
