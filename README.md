# Spring Boot 本地审计事件流水线 (spring-audit-event-pipeline-java)

本地运行的审计事件 **采集 → 规范化落账 → 合规判定 → 可靠转发** 流水线。
目标：多入口并发写入、重复提交、规则演进下，结论 **可复现、可解释、不重不漏、顺序可追溯**。

## 1. 事件模型

### 1.1 原始事件 RawAuditEvent（输入，来自不同入口）

| 字段 | 说明 |
| --- | --- |
| source | 入口标识（http / 线程名等）；缺失归一为 `unknown` |
| eventId | 事件幂等键，**必填**（空/空白拒绝） |
| timestamp | ISO-8601 字符串（如 `2026-09-25T10:00:00Z`）；缺失或不可解析拒绝 |
| actor | 操作者，必填 |
| resource | 资源，必填 |
| action | 动作，必填 |
| attributes | 附加属性，可空（归一为空 Map） |

拒绝不会被静默接受，返回状态 `REJECTED` + 可区分的拒绝原因枚举：
`MISSING_EVENT_ID`、`MISSING_TIMESTAMP`、`INVALID_TIMESTAMP`、
`MISSING_ACTOR`、`MISSING_RESOURCE`、`MISSING_ACTION`。

### 1.2 规范化事件 AuditEvent（落账模型）

字段在 RawAuditEvent 基础上增加：

- `sequence`：账本在写入临界区内单调分配的序列号（0 起），表示**落账顺序**；
- `ingestedAt`：实际落账时间。

事件自带的 `timestamp` 表示事件**固有发生时间**，二者分离：
乱序到达时，固有时间戳语义不被落账顺序破坏，而 `sequence` 保证账本顺序可追溯。

### 1.3 幂等与并发

- 账本以 `eventId` 去重：重复提交返回 `DUPLICATE` 与既有记录，**不产生第二条独立记录**，
  也不会重新判定或重新排程转发；
- 落账是单个原子操作（`putIfAbsent` 返回是否新建），避免 check-then-act 竞态；
- 序列号在同一临界区分配，高并发下连续无空洞。

## 2. 合规判定语义

### 2.1 规则 ComplianceRule

`ruleId / version / description / severity` + 三个 glob 匹配维度：
`actorPattern / resourcePattern / actionPattern`（`*` 任意多字符，`?` 单字符，空值归一为 `*`）。
三个维度同时匹配即命中，命中即为违规。

### 2.2 规则集版本与结论固化

- 规则集以**整体快照**形式更新，成功更新一次 `ruleSetVersion` 加 1（初始为空规则集 v0）；
- 任一规则不可解析（非法模式字符、缺 ruleId/severity、version<1）或同集内 ruleId 冲突，
  **整个更新失败**（抛 `RuleParseException`，HTTP 400），旧规则集继续生效，绝不跳过坏规则；
- 判定结论 `ComplianceVerdict` 固化 `eventId + ruleSetVersion + 每条命中的规则版本与文字依据`；
- **规则演进不回溯**：新事件按当前规则集判定，历史结论对象保持不变；
  历史快照在注册表中保留，可用 `snapshotAt(version)` 对历史事件**重新复现**当初结论；
- 每条违规给出可解释依据（命中规则、各维度实际值与模式、规则版本、规则集版本）。

## 3. 转发语义（本地目标）

转发目标为本地 JSON Lines 文件（属性 `audit.forward.file`，默认 `build/audit-forward.jsonl`）。

- **顺序性**：单工作线程严格按事件 `sequence` 处理；失败重试保持在原序列号位置，
  文件内行序与账本序列号一致。事件“已落账但尚未入转发队列”的短暂缺口由有界宽限窗口
  （默认 100ms）等待补齐，防止高并发下入队顺序与序列号顺序不一致；
- **不阻塞**：瞬时失败在原位置指数退避重试（默认 5 次：10/20/40/80/160ms 量级）；
  达到上限转入死信列表后立即越过该事件处理后续事件，单条永久失败不长期阻塞其它事件；
  可用 `retryDeadLetters()`（`GET/POST` 见下）重投；
- **不重**：三层幂等 —— 入队按 eventId 去重；目标端已投递集合去重；
  目标实例启动时扫描文件恢复已投递 eventId（重启后重复投递也安全）；
- **不丢**：未成功且未进死信的任务持续重试；死信可查询数量并重投；
- 宽限窗口耗尽仍缺号时会越过缺口（告警日志）；缺号事件一旦入队仍会被投递，目标端幂等兜底。

## 4. HTTP 接口（多入口之一）

| 方法/路径 | 说明 |
| --- | --- |
| `POST /api/events` | 采集事件。201 ACCEPTED / 200 DUPLICATE / 422 REJECTED+reason |
| `POST /api/rules` | 整体替换规则集；坏规则/冲突返回 400 RULES_REJECTED |
| `GET /api/rules` | 当前规则集快照（含版本号） |
| `GET /api/events` / `GET /api/events/{id}` | 账本查询（按序列号顺序） |
| `GET /api/verdicts` / `GET /api/verdicts/{id}` | 判定结论查询 |
| `GET /api/forward/status` | 转发状态（死信数） |

事件请求体示例：

```json
{
  "source": "http",
  "eventId": "evt-001",
  "timestamp": "2026-09-25T10:00:00Z",
  "actor": "alice",
  "resource": "db/customers",
  "action": "delete",
  "attributes": {"ip": "10.0.0.1"}
}
```

转发文件每行是一个 JSON 对象，包含事件全字段、`sequence`、`ruleSetVersion`、
`compliant` 与 `violations`（含 explanation）。

## 5. 兼容范围

- JDK 21+（Spring Boot 3.4 / Jakarta）；
- 单 JVM、单进程本地账本（内存）+ 本地文件转发目标，无外部中间件依赖；
- 去重以单进程账本与目标文件内容为准；多进程/分布式部署不在当前兼容范围内。

## 6. 本地验证

```bash
# 全量测试（12 个，含并发/重复/规则演进/转发失败组合场景）
mvn clean test

# 启动服务（随机端口；转发文件见 application.properties）
mvn spring-boot:run
```

测试覆盖（surefire 报告在 `target/surefire-reports`）：

- `NormalizerAndStoreTest`：六种可区分拒绝原因、默认值归一、重复提交幂等、乱序落账语义；
- `RuleEvolutionTest`：规则演进后新事件按新版本判定、历史结论按旧快照可复现、
  冲突/不可解析/缺字段规则导致整体更新失败且旧规则保留；
- `ForwardServiceTest`：瞬时失败有界重试且文件无重复行、顺序保持；
  毒消息进入死信且不阻塞健康事件、修复后重投；重启恢复去重与重复入队去重；
- `PipelineConcurrencyTest`：12 线程 × 300 次写入（含大量重复）组合规则演进与转发，
  校验唯一记录数、序列号连续、拒绝/违规/合规计数、转发文件不重不漏有序、历史结论稳定；
- `AuditApiIntegrationTest`：HTTP 端到端（接受/重复/不同拒绝原因/坏规则 400/规则演进）。

测试日志（SLF4J）中会打印每个**输入事件**、**规则命中依据**与**最终判定**，例如：

```
INFO ... 输入事件(source=http, eventId=..., timestamp=..., actor=..., resource=..., action=...)
INFO ... 规则命中: eventId=... -> 命中规则 R-DEL(v1, 规则集v1)[HIGH]: actor='...' 匹配 '*', ...
INFO ... 最终判定: eventId=..., 规则集版本=1, 合规=false, 命中数=1
WARN ... 事件被拒绝: eventId=..., 原因=INVALID_TIMESTAMP
```
