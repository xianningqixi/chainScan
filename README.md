# signals — 链上信号信息管道（chainScan）

> 本文面向**需要理解和运维本系统的其他 Agent**。只记录长期有效的操作事实，不写历史变更流水账。
> 路径、命令、字段名、环境变量、TOML key 保持英文原样。

---

## 1. 这是什么 / What this is

- 本仓库**只是信号信息管道（information only）**：采集 → 补全市值 → 过滤 → 去重 → 追加输出 → 生成中文通知文本。
- **没有**自动交易、下单、签名、钱包集成；输出中**没有**任何被填充的 execution 字段。
- 下游「土狗交易员」**可以读取**本管道的输出（`latest.jsonl`、`outbox/push_*.jsonl`、`outbox/PENDING_CHAT`），但本管道**不控制它**，也不读取它的指令或响应。

---

## 2. 架构 / 数据流

```
Binance Web3 Smart Money WS
  └─ bin/binance_smy_ws.py            (常驻；把消息转成 schema v1 信号)
       └─ 原子 rename 写入 inbox/*.jsonl
            └─ bin/ingest_worker.py   (常驻；每 0.5s 轮询 inbox，调用 ingest.process_batch)
                 ├─ 补全 mcap（DexScreener → GeckoTerminal 回退）
                 ├─ 过滤（filter_signal.py，L0/L1/L2）
                 ├─ 对已接受的 Binance 信号做 FOMO REST 核验（仅注释）
                 ├─ 追加 latest.jsonl（只追加）
                 ├─ 写 outbox/push_YYYYmmdd_HHMMSS.jsonl，并把路径追加到 outbox/NEW
                 └─ 调用 notifier（bin/notify_outbox.py）
                      └─ 向 outbox/PENDING_CHAT 追加中文段落
```

关键机制：

- **原子投递**：生产者必须先写临时文件名（不以 `.json`/`.jsonl` 结尾），再原子 rename 成 `.jsonl`。自带生产者已遵守。
- **认领**：ingest 持有 `state/ingest.lock`，把输入 rename 为 `*.processing`；处理完移入 `inbox/done/`。遗留的 `.processing` 会在下次运行重试。
- **坏输入隔离**：畸形 JSONL 行、非对象条目、字段类型错误**逐条**隔离到 `inbox/failed/`，附 `.error.json` sidecar（源文件名 + 物理行号或数组下标）。同文件中的好条目照常发布。
- **系统错误**（DB/文件系统/意外异常）不会标记为 bad_input：原 `.processing` 保留待重试，报告含 `errors`，CLI 非零退出。
- **SQLite 状态与日志**：`state/pipeline.db`（WAL）。保存去重 ID、重评状态、mcap 缓存、限速/熔断状态，以及**发布前的 accepted payload journal**——中断的投递会在下一次 ingest 续上。已推送的 ID 永不再推。
- **同秒批次**：push 文件名冲突时顺延到下一个未使用的秒，已有 push 文件绝不覆盖。
- **锁**：`state/ingest.lock`（ingest 互斥）、`state/outbox.lock`（ingest 与 notifier 共享，保护 `NEW`/`PENDING_CHAT`）、`state/supervisor.lock`（若使用 supervisor）、`state/*.log.lock`（日志轮转）。
- **遥测**：`state/events.jsonl`（`ws_connected`、`ws_error`、`ws_message`、`sig_queued`、`eval`、`accepted`、`rejected`、`enrich`、`fomo`、`notify` 等），只追加、不轮转；不记录原始 payload、HTTP body、URL、凭证。运维日志 `state/<name>.log` 为 JSONL，10 MiB × 5 份轮转。
- 常驻进程**不会热加载**修改后的 Python 模块；代码改动需重启才生效（重启生产需人类批准，见 §8）。

---

## 3. 硬约束（Hard constraints）

- **禁止自动交易 / no auto-trade ever**。不得加入下单、签名、钱包、execution 字段。
- **冻结 schema v1 字段含义**：`chain` / `ca` / `direction` / `signal_type` 等核心字段，以及其他 v1 字段（`schema_version`、`signal_id`、`ts`、`source`、`source_url`、`token_name`、`smart_money_count`、`buy_usd`、`price`、`mcap`、`status`、`notes`、`tags` …）的**含义不可修改**。只允许追加新字段（例如已有的 `first_seen_ts`、`pipeline_latency_ms`、`filter_flags`、`fomo_verify` 等）。`tests/test_schema_snapshot.py` 守护此约束。
- **`latest.jsonl` 只追加**，不重写、不截断、不替换历史行。
- **固定输出路径**：`latest.jsonl`、`outbox/push_YYYYmmdd_HHMMSS.jsonl`，不得改名/改格式。
- **FOMO 仅 annotate**：配额耗尽、错误、超时、无命中、名称不匹配都只写注释，**不可因 FOMO 结果 reject 已接受的信号**。
- **mcap 区间 $30,000–$5,000,000，两端包含**（`filter.mcap_min` / `filter.mcap_max`）。
- **没有 name/CA blacklist**，不要擅自添加。
- **未经人类明确批准，不要启用** `supervisor`、`webhook_server`、`gmgn_adapter`、`notify.push_updates`。

---

## 4. 目录地图

| 路径 | 说明 | 是否入库 |
|---|---|---|
| `bin/` | 全部入口与模块（见下） | ✅ |
| `bin/legacy/` | 已停用的 FOMO WS/SSE listener，**不要启动** | ✅ |
| `config/pipeline.toml` | 过滤/补全/FOMO/重评/通知参数 | ✅ |
| `config/services.toml` | supervisor 服务开关（全部 `enabled = false`） | ✅ |
| `tests/` | pytest 测试；`tests/fixtures/` 含合成/脱敏夹具 | ✅ |
| `deploy/signals.service` | 可选 systemd 模板（**未安装**） | ✅ |
| `inbox/` | 待处理输入；`inbox/done/`、`inbox/failed/` | ❌ 运行时 |
| `outbox/` | `push_*.jsonl`、`NEW`、`PENDING_CHAT`、`last_notify.txt`、`notified/` | ❌ 运行时 |
| `state/` | `pipeline.db`、`events.jsonl`、锁、日志、**secrets** | ❌ local-only |
| `scratch/` | Agent 的 review/plan 笔记 | ❌ local-only |
| `latest.jsonl` | 已接受信号的只追加总表 | ❌ 运行时 |
| `archive/` | `archive_latest.py` 的月度归档输出 | ❌ 勿提交（注意：当前**未**列入 `.gitignore`） |
| `logs/` | 历史日志目录 | ❌ gitignored |
| `.venv/` | 已存在的 Python 虚拟环境 | ❌ |

`bin/` 主要文件：

- `binance_smy_ws.py` — 主信号源（Binance WS → inbox），无需 API key。
- `ingest_worker.py` — 常驻 inbox 消费者（0.5s 轮询）。
- `ingest.py` — 单次 ingest（worker 内部也调用它）；可 CLI 单跑。
- `filter_signal.py` / `mcap_enrich.py` / `fomo_verify.py` / `store.py` / `common.py` — 过滤、市值补全、FOMO 核验、SQLite 存储、公共工具。
- `notify_outbox.py` — `NEW` → `PENDING_CHAT` 通知器。
- `healthcheck.py` — 只读健康检查与统计。
- `replay.py` — 离线回放验证。
- `archive_latest.py` — 人工归档工具。
- `supervisor.py`、`webhook_server.py`、`gmgn_adapter.py` — P2 功能，存在但保持关闭（§10）。

---

## 5. 本地运行

环境：Python 3.11+（使用 `tomllib`），仓库已有 `.venv/`（含 `websockets`、`pytest`）。所有命令在仓库根目录执行。

```sh
# 测试（离线，测试会禁用网络与 secret 文件读取）
.venv/bin/python -m pytest -q

# 主信号源 + 常驻 worker（各开一个 shell，或按现有方式 nohup 后台运行）
.venv/bin/python bin/binance_smy_ws.py
.venv/bin/python bin/ingest_worker.py

# 单次手动 ingest / 手动触发通知（一般不需要，worker 已自动调用）
.venv/bin/python bin/ingest.py
.venv/bin/python bin/notify_outbox.py
```

- 如需隔离实验，设置 `SIGNALS_ROOT=/tmp/xxx` 指向另一个数据根，**不要**在生产根上起第二套 WS/worker（会产生重复生产者/消费者）。
- **不要默认用 supervisor 上线**；见 §8、§10。

### 健康检查

```sh
.venv/bin/python bin/healthcheck.py            # 健康状态
.venv/bin/python bin/healthcheck.py --stats 24h # 统计：拒绝原因、延迟 P50/P95、filter flags 等（支持 m/h/d）
```

只读：读取 `state/events.jsonl`、inbox、FOMO 熔断状态、可选 `state/supervisor.json`；不启动服务、不消费输出。

| 退出码 | 含义 |
|---|---|
| **0** | healthy |
| **1** | warning：最近 1 小时有 enrich 429、FOMO 熔断打开、supervisor 有重启、可选状态不可读 |
| **2** | critical：WS 消息缺失或超过 10 分钟未更新、inbox 积压 ≥ 50 个文件（含遗留 `.processing`）、必需监控状态不可读 |

supervisor 未安装时报告 `not_installed`，不算 warning。

### 离线回放验证

```sh
.venv/bin/python bin/replay.py --from inbox/done --out /tmp/replay_$(date +%s)
```

`--out` 必须是空的独立目录。回放自动设置 `ENRICH_OFFLINE=1`、`FOMO_VERIFY_DRY=1` 并禁用 socket，走真实 ingest 路径，输出 `report.json`。离线市值来自 `tests/fixtures/dexscreener_offline.json`（**合成数据**，不是历史真实估值）。回放会加速到达时间，不模拟真实延迟；结果只验证机制，不代表收益。可选 `--config`、`--compare`、`--mcap-mode`、`--label maxgain`。

---

## 6. 配置旋钮

配置用 `tomllib` 读取。默认 `config/pipeline.toml`；`SIGNALS_CONFIG` 可指定覆盖文件，缺失的 section 继承默认值。时区固定 UTC+8。

### `config/pipeline.toml` 要点

- **`[filter]`**
  - `mcap_min = 30000`、`mcap_max = 5000000`（含端点，硬约束）。
  - 流量门槛：`buy_usd ≥ min_buy_usd (1000)` **或** `smart_money_count ≥ min_smart (1)`。
  - `allowed_chains` / `allowed_chain_ids`：sol、bsc、eth、base、arb、op、matic/polygon 及对应数字 ID。
  - `bad_status`：失效类状态直接拒绝。
  - `mcap_unknown_policy = "pending"`：市值未知时标记 `mcap_pending`，后续更新可再评估；窗口过期后记为 `mcap_unknown`。其他取值：`pass_flagged`（放行并加注释）、`reject`。
  - `filter_version = "v5-shadow"`；`mcap_scale_policy = "shadow"`；`baseline_dev_close_gate = true`（v4 DEV Close 门仍生效）；`append_flags_to_notes = false`（注释写入 `filter_flags`，不污染 `notes`）。
- **`[filter.l0]` / `[filter.l1]`**：新规则大多为 `"flag"` 或 `"shadow"`——只打标/影子统计，**不拒绝**。把它们改成 reject 需人类批准。
- **`[filter.l2]`**：`score_gate_enabled = false`（评分门关闭）。
- **`[enrich]`**：成功缓存 `ttl_ok = 1200`s，确定不存在缓存 `ttl_fail = 300`s（瞬时错误不缓存）；单请求 `timeout = 4`s，总预算 `budget_ms = 6000`；限速 `dexscreener_rps = 4.0`、`geckoterminal_rps = 0.4`（经 SQLite 跨进程共享）；HTTP 429 触发按源熔断，指数退避 1→2→4…→120s（`backoff_base`）。
- **`[fomo]`**：`enabled = true`、`timeout = 4`、`breaker_cooldown = 1800`。遇 HTTP 402/429 或配额错误打开持久熔断，冷却期内不发请求，注释为 `summary="quota_exceeded"` / `credits_note="breaker_open"`。**仍然只注释。**
- **`[reeval]`**：被拒信号在输入指纹变化时可再评估，`max_evals = 30` 次 / `window_min = 120` 分钟内。测试地址、缺地址、禁用链、卖出信号、刷量、超上限市值等为终态拒绝；市值低/未知、流量弱可恢复。
- **`[notify]`**：`push_updates = false`（状态/涨幅更新通知关闭）；`max_gain_threshold = 100`。即使开启也只向 `PENDING_CHAT` 追加文本，不写 `latest.jsonl`/push 批次。

### `config/services.toml` 要点

- 只被 `bin/supervisor.py` 读取。列出 `binance_smy_ws`、`ingest_worker`、`webhook_server`、`gmgn_adapter`，**全部 `enabled = false`**。
- 含义：安装 supervisor 与生产切换是分离的；当前生产**不由 supervisor 管理**。`enabled = false` 不影响已在运行的 nohup 进程。
- 未知服务、非法开关、缺配置均 fail closed。**不要修改这些开关**，除非人类明确批准切换。

---

## 7. Secrets

- `state/secrets.env`、`state/fomo_api_key.env`、`cookies.json` 等 **NEVER commit**（均已 gitignored；整个 `state/` 被忽略）。
- 不要在 README、代码、日志、提交信息、scratch 中写入任何 key/cookie/token 值。
- `fomo_verify.py` 从环境变量或 `state/fomo_api_key.env` 读取 FOMO 凭证；`gmgn_adapter.py` 只从 `state/secrets.env` 读取 GMGN 凭证。

环境变量（仅列名称）：

| 变量 | 用途 |
|---|---|
| `SIGNALS_ROOT` | 数据根目录（默认 `/workspace/signals`） |
| `SIGNALS_CONFIG` | TOML 覆盖配置路径 |
| `FOMOSCAN_API_KEY` / `FOMOSCAN_BEARER` / `FOMOSCAN_COOKIE` | FOMO REST 凭证 |
| `FOMOSCAN_API_BASE` | FOMO API 基址 |
| `FOMO_VERIFY` | 设为 `0` 跳过 FOMO 核验 |
| `FOMO_VERIFY_DRY` | 设为 `1` 返回模拟结果，不发 HTTP |
| `FOMO_VERIFY_TIMEOUT` | FOMO 请求超时（覆盖 `fomo.timeout`） |
| `WEBHOOK_TOKEN` / `SIGNAL_WEBHOOK_TOKEN` | webhook_server 的 `X-Signal-Token` 校验值 |
| `NOTIFY_WEBHOOK_URL` | 非空时启用通知 webhook（默认不设） |
| `ENRICH_OFFLINE` | 离线市值补全（回放/测试） |
| `ENRICH_FIXTURE` | 离线补全夹具路径 |

---

## 8. 当前生产姿态（as of this push）

- 生产只运行两个常驻进程：`bin/binance_smy_ws.py` 与 `bin/ingest_worker.py`，均为**非托管（nohup 风格）**。
- **运行时 PID 会变化；用 `ps` 查看**，例如：`ps aux | grep -E 'bin/(binance_smy_ws|ingest_worker)\.py' | grep -v grep`。
- supervisor、webhook_server、GMGN adapter、`notify.push_updates` 全部保持默认关闭；systemd 单元未安装。
- **未经人类批准，不要 kill、重启或替换生产进程**，也不要在同一 `SIGNALS_ROOT` 上启动第二个 WS/worker/supervisor。
- 未来如要切换到 supervisor：先确认 inbox 无未处理文件，协调停止旧进程，再启用 `services.toml` 中的服务——**这整件事需要人类批准并协调**。

---

## 9. 给 Agent 的操作规范

- `scratch/` 下的 review/plan 文档是 **local-only、gitignored**，不会推到 GitHub，**不要依赖它们**。需要长期保留的概念写进本 README。
- 修改代码前后都跑 `.venv/bin/python -m pytest -q`；测试必须全绿再交付。
- 不要擅自开启任何已关闭的服务或功能开关（§3、§10），不要把 flag/shadow 策略改成 reject。
- **不要提交** secrets、`state/`、`inbox/`、`outbox/`、`latest.jsonl`、`archive/`、日志、`*.db`、`scratch/`。提交前检查 `git status`。
- 不要修改/截断/重写 `latest.jsonl` 或已存在的 push 文件；不要删除 `inbox/failed/` 中的原件（保留用于诊断）。
- 手动重放单个失败条目：看 sidecar → 修正**副本** → 以非 `.json`/`.jsonl` 临时名写入 `inbox/` → 原子 rename 为新的 `.jsonl` 名。已推送 ID 仍会去重。
- 读 `state/events.jsonl` / `healthcheck.py` 诊断问题，优先只读操作。

### 通知消费约定（给下游消费者）

- `outbox/NEW`：换行分隔的待通知 batch 路径。notifier 与 ingest 共享 `state/outbox.lock`，消费 `NEW` 不会丢失新追加的路径；缺失/畸形 batch 留在队列。处理完的 batch 移至 `outbox/notified/`。
- `outbox/PENDING_CHAT`：每个成功 batch 追加一段中文文本（fsync），以 `### batch push_YYYYmmdd_HHMMSS.jsonl <timestamp>` 开头，含代币名、链、CA、聪明钱数、方向、FOMO 核验摘要。保留所有未消费段落。
- `outbox/last_notify.txt`：仅最近一次预览。
- 消费方式：持有 `state/outbox.lock` 读取后截断；或持锁把文件 move 走，释放锁后再读快照——避免并发追加丢失。可选 `outbox/PENDING_CHAT.offset` 是**消费方自有**的字节游标，管道从不读取也不重置；消费方截断/轮转时需自行归零。
- 投递语义为 **at least once**：可能出现重复段落，用 batch 头识别去重。
- `NOTIFY_WEBHOOK_URL` 非空时，额外对每个 batch POST `{"text": "<同一段文本>"}`（3s 超时，失败只记 `notify_error`，不重试、不影响文件投递）。默认未设置。

---

## 10. P2 功能：存在但保持关闭

| 功能 | 状态与说明 |
|---|---|
| `bin/supervisor.py` | 标准库实现，读 `config/services.toml`（全关）。管理子进程、指数退避重启（1…32s，再 60s）、写 `state/supervisor.json`，SIGTERM 时 30s 内优雅停止。**不会接管已有 nohup 进程**，并行启用会导致重复生产者。未启用。 |
| `deploy/signals.service` | 运行 supervisor 的可选 systemd 模板（`TimeoutStopSec=45`）。安装前需按主机调整 `User=`/路径/`SIGNALS_ROOT`。**未安装、未启用。** |
| `bin/webhook_server.py` | 备用本地接收器：仅绑定 `127.0.0.1`，`POST /ingest` 原子写 inbox 并立即返回 `{"ok": true, "queued": ...}`；body > 1 MiB 返回 413；设置 token 时校验 `X-Signal-Token`。本身不跑 ingest。未启用。 |
| `bin/gmgn_adapter.py` | 离线、显式调用的一次性适配器：`gmgn_adapter.py payload.json [--json]`，GMGN payload → schema v1 → inbox。无轮询、无网络客户端。未启用。 |
| `notify.push_updates` | 已推送信号变为 `expired`/`invalid` 或涨幅越过 `max_gain_threshold` 时追加信息性文本到 `PENDING_CHAT`。`false`。 |
| `bin/archive_latest.py` | **人工**运行。`latest.jsonl` 超过 50 MiB 时按行内 `ts`（UTC+8）月份**复制**到 `archive/latest_YYYYMM.jsonl`，幂等；**绝不截断/重写** `latest.jsonl`。支持 `--dry-run`；`--force` 仅用于人工补归档/测试。 |
| `bin/legacy/fomo_ws_listener.py`、`bin/legacy/fomo_sse_listener.py` | 已停用，不受 supervisor 管理，**不要启动**。FOMO 只通过 `fomo_verify.py` 的按需 REST 调用使用。 |

以上任何一项的启用都需要人类明确批准。
