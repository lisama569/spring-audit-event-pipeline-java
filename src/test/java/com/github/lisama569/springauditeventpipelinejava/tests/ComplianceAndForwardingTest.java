package com.github.lisama569.springauditeventpipelinejava.tests;

import com.github.lisama569.springauditeventpipelinejava.api.dto.AuditEventRequest;
import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceRule;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleConflictException;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleEngine;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleParseException;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardingService;
import com.github.lisama569.springauditeventpipelinejava.recovery.DurableDeliveryStateStore;
import com.github.lisama569.springauditeventpipelinejava.recovery.DurableEventStore;
import com.github.lisama569.springauditeventpipelinejava.recovery.DurableLocalAuditSink;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditLedger;
import com.github.lisama569.springauditeventpipelinejava.model.IngestResult;
import com.github.lisama569.springauditeventpipelinejava.model.RuleVerdict;
import com.github.lisama569.springauditeventpipelinejava.model.VerdictOutcome;
import com.github.lisama569.springauditeventpipelinejava.pipeline.AuditPipeline;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComplianceAndForwardingTest {

    private static final Logger log = LoggerFactory.getLogger(ComplianceAndForwardingTest.class);

    @TempDir
    Path dataDir;

    private AuditLedger ledger;
    private RuleRegistry registry;
    private DurableLocalAuditSink sink;
    private ForwardingService forwarding;
    private AuditPipeline pipeline;

    @BeforeEach
    void setUp() {
        DurableEventStore eventStore = new DurableEventStore(dataDir.resolve("events.log"));
        DurableDeliveryStateStore stateStore =
                new DurableDeliveryStateStore(dataDir.resolve("delivery-state.log"));
        ledger = new AuditLedger(eventStore);
        registry = new RuleRegistry();
        sink = new DurableLocalAuditSink(dataDir.resolve("sink-delivered.log"));
        sink.recover();
        forwarding = new ForwardingService(sink, stateStore, eventStore);
        forwarding.start();
        pipeline = new AuditPipeline(ledger, registry, new RuleEngine(), forwarding);
    }

    @AfterEach
    void tearDown() {
        forwarding.shutdown();
    }

    private static AuditEventRequest event(String id, String actor, String action) {
        return new AuditEventRequest(id, "2026-09-25T10:00:00Z", actor, "db", action, Map.of());
    }

    private void logVerdicts(String eventId) {
        for (RuleVerdict v : pipeline.verdictsOf(eventId)) {
            log.info("事件={} 规则命中={}:v{} 最终判定={} 依据={}",
                    eventId, v.ruleId(), v.ruleVersion(), v.outcome(), v.explanation());
        }
    }

    @Test
    void ruleEvolutionKeepsHistoricalVerdictsStable() {
        registry.register(ComplianceRule.parse("no-delete:v1 action == DELETE # 禁止删除"));
        pipeline.submit(event("old-1", "alice", "DELETE"));
        logVerdicts("old-1");

        List<RuleVerdict> before = pipeline.verdictsOf("old-1");
        assertEquals(1, before.size());
        assertEquals(VerdictOutcome.VIOLATION, before.get(0).outcome());
        assertEquals(1, before.get(0).ruleVersion());

        // 规则演进：v2 改为禁止 DROP；新事件按新规则判定
        registry.register(ComplianceRule.parse("no-delete:v2 action == DROP # 禁止删表"));
        pipeline.submit(event("new-1", "bob", "DELETE"));
        pipeline.submit(event("new-2", "carol", "DROP"));
        logVerdicts("new-1");
        logVerdicts("new-2");

        List<RuleVerdict> after = pipeline.verdictsOf("old-1");
        assertEquals(before, after, "规则更新后历史事件结论必须保持稳定");
        assertEquals(VerdictOutcome.PASS, pipeline.verdictsOf("new-1").get(0).outcome(),
                "新规则下 DELETE 不再违规");
        RuleVerdict dropVerdict = pipeline.verdictsOf("new-2").get(0);
        assertEquals(VerdictOutcome.VIOLATION, dropVerdict.outcome());
        assertEquals(2, dropVerdict.ruleVersion(), "新事件须按 v2 判定");
        assertTrue(dropVerdict.explanation().contains("DROP"), "违规结论须给出可解释依据");
    }

    @Test
    void ruleConflictAndUnparseableRuleFailExplicitly() {
        registry.register(ComplianceRule.parse("r1:v1 action == DELETE"));
        // 同 id 同版本不同定义 -> 冲突，显式失败
        assertThrows(RuleConflictException.class,
                () -> registry.register(ComplianceRule.parse("r1:v1 action == DROP")));
        // 同 id 同版本相同定义 -> 幂等，不报错
        registry.register(ComplianceRule.parse("r1:v1 action == DELETE"));
        // 不可解析规则 -> 显式失败而不是跳过
        assertThrows(RuleParseException.class, () -> ComplianceRule.parse("r2 action =="));
        assertThrows(RuleParseException.class, () -> ComplianceRule.parse("r2:vX action == DELETE"));
        assertThrows(RuleParseException.class, () -> ComplianceRule.parse("r2:v1 action ?? DELETE"));
        assertThrows(RuleParseException.class, () -> ComplianceRule.parse("r2:v1 action =~ [unclosed"));
        assertEquals(1, registry.activeRules().size(), "失败规则不得进入生效集");
    }

    @Test
    void forwardingFailureRetriesWithoutBlockingOrDuplicating() {
        // 前 3 次投递失败：验证重试且单条失败不阻塞其它事件
        sink.failNextDeliveries(3);
        for (int i = 0; i < 5; i++) {
            pipeline.submit(event("fwd-" + i, "alice", "READ"));
        }
        forwarding.awaitQuiescent(10_000);
        log.info("目标端已投递={}，死信={}", sink.deliveredEventIds(), forwarding.deadLetterCount());

        assertEquals(0, forwarding.deadLetterCount(), "重试应成功，不应进入死信");
        assertEquals(5, sink.deliveredCount(), "全部事件最终必须送达");
        assertEquals(5, sink.deliveredEventIds().stream().distinct().count(),
                "重复投递不得在目标端产生重复数据");
    }

    @Test
    void permanentFailureGoesToDeadLetterWithoutLosingOthers() {
        // 目标端对 doomed 持续故障：耗尽重试进入死信，其余事件不受影响
        sink.failAlways("doomed");
        pipeline.submit(event("doomed", "alice", "READ"));
        for (int i = 0; i < 3; i++) {
            pipeline.submit(event("survivor-" + i, "bob", "READ"));
        }
        forwarding.awaitQuiescent(10_000);
        log.info("死信={}，目标端已投递={}", forwarding.deadLetters(), sink.deliveredEventIds());

        assertEquals(1, forwarding.deadLetterCount(), "耗尽重试的事件须进入死信（不丢失，可人工处理）");
        assertEquals("doomed", forwarding.deadLetters().get(0).eventId());
        assertEquals(3, sink.deliveredCount(), "单条失败不得阻塞其它事件");
    }

    @Test
    void combinedScenarioWithDuplicateAndRuleEvolutionAndForwardFailure() {
        registry.register(ComplianceRule.parse("no-delete:v1 action == DELETE # 禁止删除"));
        sink.failNextDeliveries(2);

        // 合法违规事件 + 重复提交 + 非法事件 + 规则演进后的新事件
        pipeline.submit(event("c-1", "alice", "DELETE"));
        IngestResult dup = pipeline.submit(event("c-1", "alice", "DELETE"));
        IngestResult rejected = pipeline.submit(
                new AuditEventRequest("c-2", "bad-ts", "bob", "db", "READ", Map.of()));
        registry.register(ComplianceRule.parse("no-delete:v2 action == DROP # 禁止删表"));
        pipeline.submit(event("c-3", "carol", "DROP"));

        assertEquals(IngestResult.Status.DUPLICATE, dup.status());
        assertEquals(IngestResult.Status.REJECTED, rejected.status());
        assertEquals(com.github.lisama569.springauditeventpipelinejava.model.RejectionReason.INVALID_TIMESTAMP,
                rejected.reason());
        assertEquals(2, ledger.size(), "重复与拒绝均不产生新记录");

        logVerdicts("c-1");
        logVerdicts("c-3");
        assertEquals(VerdictOutcome.VIOLATION, pipeline.verdictsOf("c-1").get(0).outcome());
        assertEquals(1, pipeline.verdictsOf("c-1").get(0).ruleVersion());
        assertEquals(VerdictOutcome.VIOLATION, pipeline.verdictsOf("c-3").get(0).outcome());
        assertEquals(2, pipeline.verdictsOf("c-3").get(0).ruleVersion());

        forwarding.awaitQuiescent(10_000);
        assertEquals(0, forwarding.deadLetterCount());
        assertEquals(2, sink.deliveredCount(), "失败重试后全部落账事件须恰好送达一次");
        log.info("组合场景最终投递={}", sink.deliveredEventIds());
    }
}
