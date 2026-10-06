# Spring Audit Event Pipeline

本地审计事件采集、合规判定与转发流水线。事件在多入口并发写入、重复提交和规则演进下，
仍能给出可复现且可解释的结论。在此基础上提供：

- **可恢复投递**：进程重启后，已收下未送达的事件接着送完，已送达的不重复；恢复期间
  实时事件照常受理，两者并发互不卡死、不重复送达。
- **按范围补投**：按业务时间范围或一批事件 id 重新送达；长跨度补投内存有界、
  与实时并发最终不重不漏且顺序稳定、中断后安全重发。
- **进度与对账**：每事件的送达/重试中/死信阶段与尝试次数可查，账本、状态表、
  目标端三方可对账。

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
- 耗尽重试的事件进入死信（状态 `DEAD`），可查询、可人工处理，不静默丢弃。

## 可恢复投递（重启不丢、不重）

投递状态与事件本身都做了本地持久化，默认落在 `audit.data-dir`（默认 `./audit-data`）：

| 文件 | 内容 | 语义 |
|---|---|---|
| `events.log` | 已接受事件（长度前缀记录，追加即 fsync） | 重启后重建账本与业务顺序视图 |
| `delivery-state.log` | 每事件投递状态（PENDING/DELIVERED/DEAD + attempts） | 重启后据此恢复；启动时折叠压缩 |
| `sink-delivered.log` | 目标端已接受的事件 | 重启后重建目标端，重复送达被幂等拦截 |

- **PENDING（等待/重试中）**：重启后由恢复流程重新认领、接着送完；
- **DELIVERED（已送达）**：重启后绝不重投，目标端不会出现第二条；
- **DEAD（彻底失败，等待人工）**：不会被自动恢复复活，只能由显式补投/人工重投。
- 恢复与实时投递共用同一队列与“**认领 + 状态判定**”原语：同一事件任一时刻
  至多一个投递路径在处理，因此恢复期间新来的实时事件照常受理投递，两者并发
  互不卡死，也不会把同一事件送达两次。
- 持久化采用长度前缀追加日志并在每条记录后 fsync；崩溃时最多丢失“尚未写完”
  的一条，文件尾部残缺记录在下次打开时自动截断到最后一条完整记录边界。

## 按范围补投（Replay）

- 两种选择方式：按业务时间范围 `[from, to)`，或给定一批 `eventIds`
  （重复 id 自动去重，未知名被忽略）。
- 已 `DELIVERED` 的事件直接跳过；`PENDING` 且在途的事件交给在途方；
  `DEAD` 事件在**显式补投**时会被复活重新投递（用于人工修复目标端后补数），
  但自动恢复仍不会复活死信。
- 补投是异步任务，返回 `jobId`，可轮询任务进度
  （命中数 / 本次新送达 / 此前已送达跳过 / 重试中 / 死信 / 未完成数）。
- **内存有界**：范围补投按 `(occurredAt, ledgerSeq)` 游标分页（每页 64 条）读取，
  同时在途事件数被信号窗口限制在 64；因此无论时间跨度多长、命中事件多少，
  补投过程常驻内存的事件体与在途集合都有上界，**不随事件总量线性膨胀**
  （测试用 2000 事件实测在途峰值恒为 64）。
- **与实时并发的最终一致性**：补投与实时最终都走同一认领原语 + 目标端幂等，
  目标端最终内容与这批事件各自恰好正常投递一次完全一致——不多记录、不少记录，
  目标端查询视图按 `(occurredAt, ledgerSeq)` 给出稳定业务顺序。
- **失败重试与中断重发安全**：补投中途失败按退避重试；进程中断后重新发起
  同一范围/同一批补投是幂等的，已送达的不会再写，未完成的继续补齐。

## 投递进度与对账

每个事件可查询其投递阶段（已送达 / 等待重试中 / 彻底失败）、累计尝试次数与
最近错误。进度汇总给出各阶段数量与累计尝试次数。

对账接口交叉核对三方：**账本 / 投递状态表 / 本地目标端**——

- 账本事件集合 == 有投递状态的事件集合（无幽灵状态、无漏状态）；
- `DELIVERED` 状态集合 == 目标端记录集合（状态说送达的实物必在，实物有的状态
  必为送达），不重复计数、不漏计；
- 返回是否对平，以及 `missingInSink` / `extraInSink` / `stateMismatch` /
  `deadLetters` 明细。高并发写入叠加失败重试收敛后必须对平（有测试覆盖）。

## REST 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/audit/events` | 提交事件；200 接受 / 409 重复 / 422 拒绝（含原因） |
| POST | `/api/audit/rules` | 注册规则，body `{"spec": "r1:v1 action == DELETE"}` |
| GET | `/api/audit/events` | 业务顺序事件视图 |
| GET | `/api/audit/verdicts/{eventId}` | 该事件的判定结论快照 |
| GET | `/api/audit/forward/status` | 已投递、死信、进度汇总与目标端原始尝试次数 |
| GET | `/api/audit/delivery/events/{eventId}` | 单事件投递状态（阶段/尝试次数/最近错误） |
| GET | `/api/audit/delivery/progress` | 投递进度汇总（PENDING/DELIVERED/DEAD/总尝试） |
| GET | `/api/audit/delivery/reconcile` | 账本/状态表/目标端三方对账报告 |
| POST | `/api/audit/replay` | 发起补投，body `{"from":...,"to":...}` 或 `{"eventIds":[...]}`；返回 202 + `jobId` |
| GET | `/api/audit/replay/{jobId}` | 查询补投任务进度 |

## 兼容范围

- Java 21，Spring Boot 3.4.x，Maven 构建。
- 存储与转发目标默认使用**本地文件持久化 + 进程内组件**（`./audit-data` 下三份
  fsync 追加日志），适用于本地验证与嵌入式/单节点部署。`AuditLedger` 仍保留
  纯内存构造（无参），`EventStore` / `DeliveryStateStore` / `LocalAuditSink`
  均为接口或可继承基类，可替换为外部数据库/消息系统实现而不改变上层语义
  （去重、顺序、幂等、认领互斥、死信、补投与对账）。
- **重启边界**：以单条记录 fsync 为持久化粒度，崩溃最多丢失未写完的一条；
  已 fsync 的事件不丢，已送达事件不重。
- **时间跨度边界**：补投支持任意大的业务时间范围；资源占用由分页大小（64）与
  在途窗口（64）决定，与命中事件总量无关。
- **补投与实时并发边界**：最终一致而非“投递动作严格串行”；目标端在补投进行中
  可能短暂观察到中间状态，待补投任务与实时投递收敛后内容对平且业务顺序稳定。
  判定请在任务 `COMPLETED` 且实时队列收敛后进行（对账接口即用于此判定）。
- 补投选择按业务时间 `occurredAt`（闭开区间 `[from,to)`），与账本业务顺序视图
  使用同一排序键 `(occurredAt, ledgerSeq)`，因此乱序到达不影响补投选择与顺序。

## 本地验证

```bash
# 全量测试：除原有的并发写入/重复提交/规则演进/转发失败组合外，新增
#  - DurableLogTest：追加日志的崩溃尾部截断与压缩
#  - RecoveryReplayReconciliationTest：重启恢复（在途 PENDING 续投、已送达不重、
#    恢复期间实时受理）、按范围/按 id 补投、补投与实时高并发不重不漏、
#    补投失败重试与中断重发、高并发对账、长跨度补投内存有界（2000 事件在途峰值=64）。
# 日志打印输入事件、每次投递尝试（成功/失败/退避/死信/恢复/补投）与最终对账结果。
mvn test

# 启动服务（持久化目录可用环境变量覆盖：AUDIT_DATA_DIR=/path mvn spring-boot:run）
mvn spring-boot:run

# 手工验证
curl -X POST localhost:8080/api/audit/rules -H 'Content-Type: application/json' \
  -d '{"spec":"no-delete:v1 action == DELETE # 禁止删除"}'
curl -X POST localhost:8080/api/audit/events -H 'Content-Type: application/json' \
  -d '{"eventId":"e1","occurredAt":"2026-09-25T10:00:00Z","actor":"alice","resource":"db","action":"DELETE"}'
curl localhost:8080/api/audit/verdicts/e1
curl localhost:8080/api/audit/forward/status

# 投递进度与三方对账
curl localhost:8080/api/audit/delivery/progress
curl localhost:8080/api/audit/delivery/events/e1
curl localhost:8080/api/audit/delivery/reconcile

# 按业务时间范围补投（返回 jobId 后轮询）
curl -X POST localhost:8080/api/audit/replay -H 'Content-Type: application/json' \
  -d '{"from":"2026-09-01T00:00:00Z","to":"2026-10-01T00:00:00Z"}'
curl localhost:8080/api/audit/replay/<jobId>

# 按一批事件 id 补投
curl -X POST localhost:8080/api/audit/replay -H 'Content-Type: application/json' \
  -d '{"eventIds":["e1","e2"]}'
```

### 手动验证重启恢复

```bash
mvn spring-boot:run
# 提交若干事件后直接 kill 进程（制造未送达窗口可先让目标端不可用），再重新启动：
# 日志中会出现“重启恢复：重新认领未送达事件 … 已送达事件不重投”，
# 随后 curl /api/audit/delivery/reconcile 应返回 "consistent": true。
```
