# Spring Audit Event Pipeline

本地审计事件采集、合规判定与转发流水线。事件在多入口并发写入、重复提交和规则演进下，
仍能给出可复现且可解释的结论。

## 事件模型

各入口的事件统一归一为 `AuditEventRequest`（输入）→ `AuditEvent`（落账）：

| 字段 | 说明 |
|---|---|
| `eventId` | 事件唯一标识，去重键 |
| `occurredAt` | 业务发生时间，ISO-8601（如 `2026-09-25T10:15:30Z`） |
| `actor` / `resource` / `action` | 操作者 / 资源 / 动作，均必填 |
| `attributes` | 扩展键值对，可被规则引用 |
| `ledgerSeq` | 落账时分配的全局单调序号（到达顺序，可追溯） |
| `ingestedAt` | 落账时间 |

### 采集语义

- **拒绝不静默**：字段缺失或非法时返回可区分的 `RejectionReason`：
  `MISSING_EVENT_ID` / `MISSING_TIMESTAMP` / `INVALID_TIMESTAMP` /
  `MISSING_ACTOR` / `MISSING_RESOURCE` / `MISSING_ACTION` / `INVALID_PAYLOAD`。
  被拒绝的事件不落账、不判定、不转发。
- **去重**：同一 `eventId` 重复提交返回 `DUPLICATE` 及已存在记录，不产生新记录。
- **乱序容忍**：`ledgerSeq` 记录到达顺序；`snapshotOrdered()` 按
  `(occurredAt, ledgerSeq)` 排序给出业务顺序视图，乱序到达不破坏顺序语义与可追溯性。

## 合规判定语义

- 规则文本形式：`ruleId:vN field OP value [# 描述]`，操作符：
  `==`、`!=`、`~`（包含）、`>`、`<`、`=~`（正则）。
  字段支持 `eventId`/`actor`/`resource`/`action`/`occurredAt` 及任意 `attributes` 键。
- 每条落账事件对当前全部生效规则逐条判定，产出 `RuleVerdict`
  （含 `ruleId`、`ruleVersion`、`PASS`/`VIOLATION`、人类可读的判定依据）。
- **规则演进**：同 `ruleId` 可注册更高版本，新事件按新规则判定；
  历史事件的结论在落账时已固化（含规则版本快照），永久稳定、可复现。
- **显式失败**：同 `ruleId:vN` 注册不同定义抛 `RuleConflictException`；
  规则不可解析抛 `RuleParseException`。失败规则不会进入生效集，不会被静默跳过。

## 转发语义

- 落账事件进入主队列按落账顺序派发；失败事件进入延迟重试队列（指数退避，
  最多 5 次），**单条失败不阻塞其它事件**。
- 目标端 `LocalAuditSink` 按 `eventId` 幂等写入：重复投递不产生重复数据，
  重试保证不丢失。
- 耗尽重试的事件进入死信表（持久化进度 `DEAD`），可查询、可人工处理，不静默丢弃。
- 实时投递、重启恢复、按范围补投共用同一投递状态机：每个 `eventId` 一把互斥锁 +
  在途标记，三条路径并发时同一事件的“尝试→落状态→排重试”全程串行；
  尝试次数只按真实投递调用计数，不重不漏。

## 可恢复投递（重启续投）

- 受理事件时，事件体先追加到持久化事件存储（fsync）；本地目标端成功写入后才把投递进度
  （状态 + 尝试次数）标记为已送达（追加日志 fsync + 周期快照压缩）。数据都在本地数据目录
  （`audit.pipeline.data-dir`，默认 `./data/audit-pipeline`；`none` 为纯内存模式）。
- **本地目标端 `LocalAuditSink` 同样持久化**：已送达 id 追加落盘，进程重启后目标端内容
  随之恢复。否则“进度记为已送达”会在重启后找不到对应记录，对账无法成立。
- 进程启动时先扫描事件存储，把进度不是 `DELIVERED`/`DEAD` 的事件重新排队续投：
  - **已送达**事件：直接跳过，目标端不会再出现第二条（即使崩溃发生在
    “目标端已写入、进度未确认”之间，续投的重复调用也被目标端幂等去重）；
  - **等待/重试中**事件：沿用持久化的尝试次数继续重试，预算不重置；
  - **死信**事件：跨重启保留，不自动复活（必须经补投/人工重新挂起）。
- 恢复扫描在工作线程启动前完成入队，随后实时事件与恢复事件进入同一有界主队列
  （容量 10000，满则背压），由同一互斥入口处理：恢复期间新事件正常受理投递，
  两者不会互相卡死，也不会重复投递同一事件。

## 按范围补投（Replay）

- 两种发起方式（`POST /api/audit/replays`）：业务时间闭区间 `{from,to}`，
  或指定事件集合 `{eventIds:[...]`。任务异步执行，返回 `jobId`，可轮询进度。
- 补投判定（每个事件互斥执行）：
  - 当前 `DELIVERED` → 幂等跳过，目标端无新增；
  - 当前在途（实时通道正在投递）→ 复用在途尝试，不再发起第二次；
  - 当前 `DEAD` → 重置尝试计数、重新挂起，重新给足 5 次重试预算；
  - 其它未送达 → 立即重新尝试，失败则按正常退避重试。
- **等价性**：补投与实时投递并发时，目标端最终内容与“这批事件各自正常投递一次”
  完全一致——不多记录（目标端幂等 + 每事件互斥）、不少记录（未送达必重试/补投）。
- **稳定顺序**：补投任务单线程按 `(occurredAt, ledgerSeq)` 顺序提交，
  同一批次内事件相对次序稳定；批次与并发实时事件的交错顺序取决于实际调度
  （无全局跨通道顺序保证，单事件均幂等）。
- **内存有界**：事件按业务时间（UTC 小时）分桶存储，范围查询逐桶流式读取，
  一次只在内存持有一个桶；补投逐条提交到有界队列。补投跨度再长，
  内存占用只与单桶（单小时）规模相关，不随事件总量线性膨胀。
- **可重入**：中途失败重试、任务中断后重新发起，已送达事件都会跳过，
  已到目标端的事件不会再写一遍；任务计数满足 `scanned = submitted + skippedDelivered`。

## 投递进度与对账

每个事件的进度（`DeliveryState`）含：状态（`PENDING` 等待 / `RETRYING` 重试中 /
`DELIVERED` 已送达 / `DEAD` 彻底失败）、`attempts` 实际投递尝试次数、最近更新时间。

`GET /api/audit/reconcile` 返回对账报告：把进度中的“已送达”集合与本地目标端
实际记录集合交叉核对，给出 `consistent` 及两侧差集。高并发写入叠加失败重试后，
两者必须一致：目标端记录数 == 已送达计数，无重复计数、无漏计；
`attempts` 精确等于对目标端的实际投递调用次数（死信恒为 5）。

## REST 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/audit/events` | 提交事件；200 接受 / 409 重复 / 422 拒绝（含原因） |
| POST | `/api/audit/rules` | 注册规则，body `{"spec": "r1:v1 action == DELETE"}` |
| GET | `/api/audit/events` | 业务顺序事件视图 |
| GET | `/api/audit/verdicts/{eventId}` | 该事件的判定结论快照 |
| GET | `/api/audit/forward/status` | 已投递 id 列表与死信数（原字段保持不变） |
| GET | `/api/audit/forward/states` | 全部事件投递进度明细（状态 + 尝试次数） |
| GET | `/api/audit/forward/states/{eventId}` | 单事件投递进度 |
| GET | `/api/audit/forward/reconcile` | 进度 × 目标端实际内容对账报告 |
| POST | `/api/audit/replays` | 发起补投（`from`+`to` 或 `eventIds`），202 返回任务句柄 |
| GET | `/api/audit/replays` / `/api/audit/replays/{jobId}` | 补投任务列表 / 单任务进度 |

## 兼容范围

- Java 21（构建显式 `--release 21`），Spring Boot 3.4.x，Maven 构建。
- 存储与转发目标均为本地实现：默认启用**文件持久化**（事件 JSONL 分桶 +
  本地目标已送达日志 + 投递进度快照/日志，目录 `audit.pipeline.data-dir`），
  支持进程重启恢复；数据目录设为 `none` 时退回**纯内存模式**（采集/判定/转发/补投/
  对账语义不变，按范围补投经内存账本视图工作，但重启后目标端与进度均不恢复）。
  接口（`EventStore`/`DeliveryStateStore`/`LocalAuditSink`）可替换为其它持久化实现，
  去重、顺序、幂等、死信、目标端与进度重启后一致的语义保持不变。
- 持久化为**单进程单目录**模型：同一数据目录不要被多个进程同时写
  （快照压缩与追加日志未做跨进程文件锁）。优雅停机（SIGTERM/容器停止）会触发
  进度快照压缩；硬杀进程时仅丢失未 fsync 的在途写入窗口，重启后自动续投。
- “不重复”的边界：投递是**至少一次 + 目标端幂等**。崩溃于“目标端写入成功、
  进度落盘之前”时，重启会产生一次额外投递调用，但目标端按 `eventId` 去重，
  结果仍是恰好一条。
- 补投顺序保证仅限同一批次内部的相对顺序稳定；补投与实时事件之间不提供
  全局线性次序。时间跨度无上限，内存只按单小时分桶有界占用。

## 本地验证

```bash
# 全量测试（18 个用例：并发写入、重复提交、规则演进、转发失败、
# 重启恢复、按范围/按 id 补投、补投与实时并发、长跨度补投顺序、
# 高并发对账、持久化存储恢复；测试日志打印输入事件、每次投递尝试与最终结果）
mvn test

# 启动服务（默认在 ./data/audit-pipeline 下落事件与投递进度）
mvn spring-boot:run

# 手工验证
curl -X POST localhost:8080/api/audit/rules -H 'Content-Type: application/json' \
  -d '{"spec":"no-delete:v1 action == DELETE # 禁止删除"}'
curl -X POST localhost:8080/api/audit/events -H 'Content-Type: application/json' \
  -d '{"eventId":"e1","occurredAt":"2026-09-25T10:00:00Z","actor":"alice","resource":"db","action":"DELETE"}'
curl localhost:8080/api/audit/verdicts/e1
curl localhost:8080/api/audit/forward/status
curl localhost:8080/api/audit/forward/states/e1      # 状态 + 尝试次数
curl localhost:8080/api/audit/forward/reconcile      # 进度 × 目标端对账

# 按业务时间范围补投（闭区间）
curl -X POST localhost:8080/api/audit/replays -H 'Content-Type: application/json' \
  -d '{"from":"2026-09-01T00:00:00Z","to":"2026-09-30T23:59:59Z"}'
# 按指定事件补投（可用于死信修复后重新挂起）
curl -X POST localhost:8080/api/audit/replays -H 'Content-Type: application/json' \
  -d '{"eventIds":["e1","e2"]}'
curl localhost:8080/api/audit/replays               # 任务进度（scanned/submitted/skippedDelivered）

# 验证重启续投：kill 掉服务进程后重新启动，观察日志“重启恢复完成，续投 N 条…”，
# 已送达事件不会重复出现在 forward/status，未完成事件自动送达
```

持久化目录布局：

```
data/audit-pipeline/
├── events/
│   ├── events-2026100110.jsonl   # 按业务时间 UTC 小时分桶的事件（追加、fsync）
│   ├── events-2026100111.jsonl
│   └── index.tsv                 # eventId → 桶名 追加索引
├── sink/
│   └── delivered.jsonl           # 本地目标端已送达 id（追加、fsync，重启恢复）
└── delivery/
    ├── state-snapshot.json       # 投递进度快照（周期压缩，原子替换）
    └── state-journal.jsonl       # 快照之后的状态变更日志（追加、fsync）
```
