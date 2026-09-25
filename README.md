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
- 耗尽重试的事件进入死信表（`deadLetters`），可查询、可人工处理，不静默丢弃。

## REST 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/audit/events` | 提交事件；200 接受 / 409 重复 / 422 拒绝（含原因） |
| POST | `/api/audit/rules` | 注册规则，body `{"spec": "r1:v1 action == DELETE"}` |
| GET | `/api/audit/events` | 业务顺序事件视图 |
| GET | `/api/audit/verdicts/{eventId}` | 该事件的判定结论快照 |
| GET | `/api/audit/forward/status` | 已投递与死信状态 |

## 兼容范围

- Java 21，Spring Boot 3.4.x，Maven 构建。
- 存储与转发目标均为本地进程内实现（内存账本 + 本地幂等 Sink），
  适用于本地验证与嵌入式部署；接口（`AuditLedger`/`LocalAuditSink`）可按需替换为持久化实现，
  语义（去重、顺序、幂等、死信）保持不变。

## 本地验证

```bash
# 全量测试（覆盖并发写入、重复提交、规则演进、转发失败组合场景，
# 测试日志打印输入事件、规则命中与最终判定）
mvn test

# 启动服务
mvn spring-boot:run

# 手工验证
curl -X POST localhost:8080/api/audit/rules -H 'Content-Type: application/json' \
  -d '{"spec":"no-delete:v1 action == DELETE # 禁止删除"}'
curl -X POST localhost:8080/api/audit/events -H 'Content-Type: application/json' \
  -d '{"eventId":"e1","occurredAt":"2026-09-25T10:00:00Z","actor":"alice","resource":"db","action":"DELETE"}'
curl localhost:8080/api/audit/verdicts/e1
curl localhost:8080/api/audit/forward/status
```
