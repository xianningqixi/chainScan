# Signal pipeline — information only

P0 landing items 1–9, P1-1 resident ingest, P1-2 market-cap enrichment,
P1-3 supervision and the existing P1-4 monitoring are implemented. Schema v1 fields and meanings are frozen;
`latest.jsonl` remains append-only, and batches remain JSONL at
`outbox/push_YYYYmmdd_HHMMSS.jsonl`. Historical rows are not rewritten.
There is no trading, order placement, signing, wallet integration, or populated
execution field. 土狗交易员 only reads signal outputs; its logic is unchanged and
the pipeline does not read its instructions or responses.

The current primary source is `bin/binance_smy_ws.py`. It publishes an atomic inbox
file for the resident `bin/ingest_worker.py`, which polls every 0.5 seconds.
`bin/webhook_server.py`
is an alternate local receiver: POST `/ingest` writes an atomic inbox file and
returns `{"ok": true, "queued": "<path>"}` immediately. It no longer runs ingest.
It remains bound to `127.0.0.1` and rejects request bodies over 1 MiB with HTTP
413. If `WEBHOOK_TOKEN` (or `SIGNAL_WEBHOOK_TOKEN`) is set, requests must carry
the same value in `X-Signal-Token`; the service remains disabled by default in
`config/services.toml`. Webhook-only use therefore also needs the worker or a
separate invocation of `bin/ingest.py`. Resident processes need a restart to
load changed Python modules; this implementation does not restart existing
production services.

Ingest takes `state/ingest.lock`, renames inputs to `*.processing`, enriches missing
market caps with DexScreener then GeckoTerminal, filters, and annotates accepted
Binance signals with FOMO REST results. It appends accepted rows to `latest.jsonl`,
writes push batches, appends their paths to `outbox/NEW`, then calls the notifier.
Abandoned `.processing` files are retried on the next run. Producers must publish
complete files by atomic rename (the supplied producers do this).

Malformed JSONL rows, non-object entries and invalid field types are isolated
individually in `inbox/failed/`, with a `.error.json` sidecar giving the source
filename and physical line number (or one-based JSON array entry index). Good
entries before and after a bad entry still publish, and the original processed
file goes to `inbox/done/`. Database, filesystem and unexpected processing errors
leave the original `.processing` file available for retry; the ingest report
includes `errors`, and the CLI exits nonzero. They are never labeled `bad_input`.
To replay a failed entry manually, inspect its sidecar, correct a copy, write it
to a temporary inbox filename not ending in `.json`/`.jsonl`, then atomically
rename it to a new `.jsonl` filename and run ingest. Keep failed originals for
diagnosis. Previously pushed IDs remain deduplicated.

SQLite WAL state lives at `state/pipeline.db`. Rejected signals can be evaluated
again when their meaningful input fingerprint changes, up to 30 evaluations in
120 minutes after first local observation. Already pushed IDs cannot push again.
Test signals/addresses, missing addresses, blocked chains, sell signals,
wash-trading rejections and market caps above the upper bound are terminal.
Low or unknown market caps and weak flow can recover on later updates.
On first initialization, `state/seen_ids.txt` is imported conservatively as
already pushed, including old rejected IDs; the file is retained and never
updated. This deliberately prevents old IDs from being pushed again after migration.

The database also journals accepted payloads before publishing files. Interrupted
delivery resumes on the next ingest invocation. SQLite indexes IDs in
`latest.jsonl`, scanning history once and only new bytes afterward; malformed
historical lines are skipped without rewriting them. Replacement or truncation
invalidates the checkpoint. Existing batch IDs also prevent repeated rows.
Same-second batches use the next unused second in the
frozen filename format, so an existing push file is never overwritten.

Configuration is in `config/pipeline.toml`, read with Python 3.11+ `tomllib`.
`SIGNALS_ROOT` selects the data root (default: `/workspace/signals`);
`SIGNALS_CONFIG` can select a TOML override. Missing sections inherit the bundled
defaults. The timezone remains UTC+8.

- Market cap: **$30,000–$5,000,000**, inclusive. `mcap_unknown_policy` defaults
  to `pending`: missing valuations yield `mcap_pending`, and another update or
  explicit retry (including an identical input) can evaluate again, within the
  existing `max_evals`/`window_min` limits. On the first update/retry after the
  window, the stored reason becomes `mcap_unknown`. No retry worker is added.
  `pass_flagged` allows an unknown valuation through the cap gate and appends
  `mcap_unknown` to accepted notes; all other filters still apply. `reject`
  preserves the old `mcap_unknown` rejection and changed-input retry behavior.
- Flow: **buy_usd ≥ 1,000 OR smart_money_count ≥ 1**. Both smart counts 3 and 5
  pass the existing default; no new smart-count gate was added.
- Chains: sol, bsc, eth, base, arb, op, matic/polygon and the existing numeric IDs.
  Existing status, watch-buy and risk-tag checks remain unchanged.
- Enrichment caches live in SQLite `mcap_cache`: success TTL 1,200 seconds,
  definitive absence TTL 300 seconds. Transient failures are not cached. For one
  compatibility version, the old `state/mcap_cache.json` is imported once without
  modifying it; existing SQLite entries take precedence.
- DexScreener falls back to GeckoTerminal. Per-source one-token buckets are
  shared through SQLite, configured by `enrich.dexscreener_rps` (default/cap 4/s)
  and `geckoterminal_rps` (default/cap 0.4/s). HTTP 429 persists a source breaker;
  consecutive rate limits back off 1, 2, 4, …, 120 seconds. A successful response
  or definitive absence resets that source's counter. `backoff_base` is configurable.
- The 4-second request timeout is capped by remaining `budget_ms` (default 6,000).
  Rate-limit waits consume this budget; exhaustion returns `error="budget"`.
  Source failures remain in notes (e.g. `mcap_enrich=failed:429`) even if fallback
  succeeds. `enrich` events retain HTTP status, with separate backoff/skip events.
  Stalled reads cannot hold up the caller beyond its HTTP budget; at most one
  background read per source remains until its socket completes, with no state writes.
- `mcap_source`, when present, is one of `dexscreener`, `geckoterminal`, `pumpfun`,
  `binance`. pump.fun was **skipped** in P1-2: the referenced v3 coin endpoint
  returned HTTP 404 (`Cannot GET /coins/...`) on the read-only availability probe.
  See `scratch/p1_2_review.md` for evidence and validation.
- FOMO: enabled by default, 1,800-second persisted breaker after HTTP 402/429 or
  quota errors. During cooldown there is no verification HTTP request:
  `summary="quota_exceeded"`, `credits_note="breaker_open"`, and
  `notes` includes `fomo_verify=quota_exceeded`. Quota, errors, timeouts, no hits
  and name mismatches only annotate; none can reject an accepted signal.


- Status/gain updates are disabled by default (`notify.push_updates=false`). If explicitly
  enabled, an already-pushed signal changing to `expired`/`invalid`, or crossing
  `notify.max_gain_threshold` (default 100%), appends informational Chinese text to
  `PENDING_CHAT` only; it never appends to `latest.jsonl` or a push batch.
- GMGN remains disabled. Its inbox reservation and ingest-side cross-source check
  close the pending-inbox race; a skipped GMGN observation persists `also_seen=gmgn`
  in SQLite without rewriting append-only output.

Run from the repository:

```sh
.venv/bin/python bin/webhook_server.py
# In another shell / existing routine:
.venv/bin/python bin/ingest.py
.venv/bin/python bin/notify_outbox.py
```

`NEW` is a newline-separated list of batch paths. The notifier and ingest share
`state/outbox.lock` so consuming the marker cannot discard newly appended paths.
Missing or malformed batches stay queued. Every successful batch appends a
section to `outbox/PENDING_CHAT`:

```text
### batch push_20260930_000001.jsonl <timestamp>
🔔 新信号 TOKEN (sol)
CA: ...
聪明钱: 3 | 方向: buy
FOMO核验: quota_exceeded
```

`last_notify.txt` is only the latest notification preview. `PENDING_CHAT` retains
all unconsumed batches. The consumption convention is for the existing routine
to read it and then truncate or move it; coordinate read/truncate with
`state/outbox.lock`, or move the queue while holding the lock and read the moved
snapshot afterward, so concurrent appends are not lost. An optional
`PENDING_CHAT.offset` stores a consumer-owned byte cursor, initially `0`. The
pipeline never uses it as input and does not reset an existing cursor; a consumer
using it must reset it when truncating/rotating the file. These are documented
conventions only; no 土狗交易员 code has been changed. Delivery to this text queue
is at least once: an interruption after appending but before acknowledging `NEW`
can repeat a section, identifiable by the batch header.

Realtime notifications are optional and off by default. Only a nonempty
`NOTIFY_WEBHOOK_URL` enables `WebhookSink`; `FileSink` always appends and fsyncs
the batch to `PENDING_CHAT` first. The webhook receives one JSON POST per nonempty
batch, with only a `text` field containing that exact UTF-8 section (including
the batch header). It contains informational text, with no execution fields.
HTTP uses the standard library with a 3-second timeout. Failures record a
`notify_error` event with the sink, error type, and HTTP status when available;
URLs and response bodies are not logged. Webhook failures do not requeue batches
or affect file delivery, `NEW`, the preview, or the consumer cursor. There is no
webhook retry queue, and responses are not interpreted. Requests are synchronous
under the existing outbox lock, so a slow endpoint can delay the notifier.

Offline validation:

```sh
.venv/bin/python -m pytest -q
.venv/bin/python bin/replay.py --from inbox/done --out /tmp/replay
```

Replay requires an empty, separate output directory, sets `ENRICH_OFFLINE=1` and
`FOMO_VERIFY_DRY=1`, and disables socket connections. It copies inputs into that
root, invokes the real ingest path and writes `report.json`. Offline enrichment
reads `tests/fixtures/dexscreener_offline.json` (or `ENRICH_FIXTURE`). Those responses
are **synthetic**, first missing then $100K, for five fixture tokens; they are not
historical valuations. Other tokens stay unknown unless input already has mcap.
Replay accelerates arrivals; evaluation windows use local processing time.
Fixtures also include five complete public update sequences and a sanitized
recorded DexScreener snapshot. Tests disable networking and secret-file reads.

`scratch/baseline_replay.json` and `scratch/p0_replay.json` compare the same 1,746
inputs: accepted **4 → 6**, with six unique accepted IDs. SAPIJIJU
(`binance-57937`) recovers from an initial missing-mcap rejection on its second
evaluation. These results validate recovery mechanics with synthetic valuations,
not historical profitability or real enrichment coverage; replay does not model
real arrival delays. The original [P0 review](scratch/p0_review.md) and
[B1 recheck](scratch/p0_b1_recheck.md) record the review findings and follow-up.

Monitoring writes JSON events to `state/events.jsonl`: `ws_connected`, `ws_error`,
`ws_message` (including duplicate messages), `sig_queued`, `eval`, `accepted`,
`rejected`, `enrich`, `fomo`, and `notify`. Enrichment records actual per-request
HTTP outcomes, source and duration without changing fallback/backoff behavior.
FOMO records summary and duration and remains annotation-only. Logs omit raw
payloads, HTTP bodies, URLs and credentials. Logging failures are best effort,
reported on stderr, and never prevent a push. CLI stdout remains its JSON report.

New accepted rows add `first_seen_ts` (first local evaluation time, retained across
reevaluations) and `pipeline_latency_ms` (signal `ts` to the push append boundary,
including time spent waiting in inbox). The timestamp and all existing schema
fields retain their meanings. Missing/invalid or timezone-less timestamps yield
null latency; future timestamps clamp to zero. Pending journal payloads preserve
the measured value across delivery retries. Historical rows are untouched.

```sh
.venv/bin/python bin/healthcheck.py
.venv/bin/python bin/healthcheck.py --stats 24h
```

Healthcheck reads events, inbox, the FOMO SQLite breaker and optional
`state/supervisor.json`; it does not start services or consume outputs. Exit codes:
**0** healthy; **1** warning (any enrich 429 in the last hour, open FOMO breaker,
supervisor restarts or unreadable optional state); **2** critical (WS messages
absent/stale by 10 minutes, inbox backlog at least 50 files, or unreadable required
monitoring state). Backlog includes abandoned `.processing` files. The 429 ratio
uses actual HTTP requests; no requests yields null, not zero. Supervisor absence
is reported as `not_installed` without a warning. Restart counts are cumulative
within a supervisor run. An open FOMO breaker warns about verification availability only.
`--stats` accepts positive `m`/`h`/`d` durations and reports rejection reasons and
latency P50/P95 with sample counts (linear interpolation). Statistics cover
observed events only; missing latency samples are counted separately. No live
latency target has yet been demonstrated. Event retention/rotation is deferred.

Addon statistics use only `eval` events in the requested window (inclusive start
and end; future events are excluded). `flags` and `would_reject` count evaluations
containing each label, once per label per event. Repeated evaluations of one
signal count separately. `evals`, `evals_by_layer`, `evals_by_layer_reason`, and
`evals_by_verdict` describe the final evaluation decisions; a layer is the
decision's layer, not every annotation's origin. Raw reason and flag labels are
preserved, including values such as `late:16.0`. `would_reject_by_verdict` separates
shadow hits on accepted evaluations from pending/rejected evaluations; accepted
evaluations do not necessarily imply delivery. Existing `accepted`,
`rejected_by_reason`, and latency statistics still use their original outcome
events, without counting their annotations again.

Historical evals without layer/verdict are counted under `unknown`; missing or
malformed annotation lists contribute no labels, and invalid list members are
ignored. Empty distributions are `{}`. These additive stats need no `tokens` or
`evals` database tables and perform no migration. Old workers emitting no addon
fields cannot provide historical shadow metrics.

The dedicated addon tests cover the frozen 1,882-row v4 corpus in original/fixed
mcap modes, L0 gates, all L1 policy settings, tags, fingerprint buckets, token
cooldown/state, and market-cap correction. They pin shipped behavior: new L0
rules flag, cooldown and mcap correction shadow, the independent v4 DEV Close
gate remains active, and the score gate remains off. Known review gaps such as
the `0xdead` test prefix and retryable dead statuses are unchanged. Price buckets
have fixed logarithmic boundaries, so even a small price move across a boundary
can trigger reevaluation. Time-dependent unit tests use controlled clocks; the
replay clock limitation remains and these tests do not authorize reject defaults.

P1-3 adds `bin/supervisor.py` using only the standard library. Configuration is
`SIGNALS_ROOT/config/services.toml` (or `--config <path>`), with an explicit boolean
`enabled` for each of `binance_smy_ws`, `ingest_worker`, and `webhook_server`.
All three ship **disabled** to keep installation separate from production cutover.
Unknown services, invalid switches and missing configuration fail closed. Optional
`args` is an array of literal command arguments; no shell is used. FOMO WS/SSE
remain disabled and are not managed, and Wind is not connected.

After a separately coordinated cutover, enable the desired services and run:

```sh
.venv/bin/python bin/supervisor.py
```

Children use the supervisor's Python interpreter, run from the code repository
root, and inherit `SIGNALS_ROOT`; the repository and its `bin` directory are added
to `PYTHONPATH`. Exits outside shutdown, including exit code 0, restart after
1, 2, 4, 8, 16, 32, then 60 seconds. A child that ran for at least 60 seconds resets
the consecutive-failure delay to 1 second. Monitoring polls every 0.1 seconds;
backoff never blocks monitoring of other children. Failed launches also back off.
`state/supervisor.json` is atomically replaced with each service's `enabled`,
`pid` (null when stopped), `restarts`, `last_exit_code`, `backoff_s`, and startup
`error`. Restart counts track successful retry launches, including recovery from
a failed initial launch; counts start at zero for a new supervisor run.

SIGTERM/SIGINT stops restart scheduling, sends TERM to all owned children, and
waits up to 30 seconds total before killing and reaping any unresponsive owned
children. The worker finishes its current batch; the WS listener cancels its
connection task and closes the connection; the webhook closes its server.
The supervisor holds `state/supervisor.lock` to reject a second supervisor on
the same root. It never discovers, adopts or signals processes from recorded
PIDs. Existing unmanaged services are left alone; enabling replacements while
they are still running can create duplicate producers/workers.

Operational messages use `common.log_event` and standard-library rotation:
`state/supervisor.log`, `state/binance_smy_ws.log`, and `state/webhook_server.log`
contain JSON lines, each capped at 10 MiB with five backups (`.1`–`.5`). Unexpected
child stdout/stderr is captured as `service_output` in the supervisor log. The WS
logger no longer prints a duplicate copy, and no `.out` files are created. Existing
`state/events.jsonl` telemetry remains append-only and unrotated so healthcheck
and statistics retain their existing behavior. Existing historical logs are not
renamed or removed by installation; rotation happens only on future log writes.

`deploy/signals.service` is an optional systemd template. Before installing it,
set the deployment user (`User=`/`Group=`), repository paths and `SIGNALS_ROOT`
for the target host. It runs the supervisor in the foreground and gives it 45
seconds to stop its children. No unit has been installed, enabled or started.

A future cutover must first confirm there are no unprocessed inbox files, then
coordinate stopping the old service owners before enabling replacements. No
cutover, inbox changes, or restart of production PIDs 805754/806790 was performed
for P1-3. No filter add-on, realtime sink, GMGN adapter, or other P1-5/P2 work is
included.

## P2-4 清理（手动维护）

- `bin/legacy/fomo_ws_listener.py` 与 `bin/legacy/fomo_sse_listener.py` 已停用，**不由 supervisor 管理，也不要启动**；当前主入口仍是 `bin/binance_smy_ws.py`。
- `latest.jsonl` 始终保持原路径和追加写语义。文件超过 50 MiB 后，需要人工执行下面的命令按行内 `ts`（UTC+8）月份复制归档；命令不会截断、重写或替换当前文件：

  ```sh
  .venv/bin/python bin/archive_latest.py
  ```

- 命令默认低于或等于 50 MiB 时不写任何归档；可用 `--dry-run` 预览，`--force` 仅用于人工补归档/测试。归档文件为 `archive/latest_YYYYMM.jsonl`，重复执行同一月份是幂等的。
