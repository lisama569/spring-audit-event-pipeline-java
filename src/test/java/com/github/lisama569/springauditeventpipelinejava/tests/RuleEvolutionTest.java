package com.github.lisama569.springauditeventpipelinejava.tests;

import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceRule;
import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceVerdict;
import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceEvaluator;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleParseException;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.compliance.Violation;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuleEvolutionTest {

    private static final Logger log = LoggerFactory.getLogger(RuleEvolutionTest.class);

    private AuditEvent event(String action) {
        return new AuditEvent("e-" + action, "http", Instant.parse("2026-09-25T10:00:00Z"),
                "bob", "db/customers", action, Map.of(), 0, Instant.now());
    }

    private ComplianceRule rule(String id, int version, String actionPattern, String severity) {
        return new ComplianceRule(id, version, "禁止 " + actionPattern, "*", "*", actionPattern, severity);
    }

    @Test
    void ruleEvolutionAndVerdictReproducibility() {
        RuleRegistry registry = new RuleRegistry();
        ComplianceEvaluator evaluator = new ComplianceEvaluator();

        // v1：只禁止 delete
        RuleRegistry.Snapshot v1 = registry.replaceRules(List.of(rule("R-DEL", 1, "delete", "HIGH")));
        assertEquals(1, v1.version());

        AuditEvent deleteEvent = event("delete");
        AuditEvent readEvent = event("read");
        ComplianceVerdict deleteUnderV1 = evaluator.evaluate(deleteEvent, v1);
        ComplianceVerdict readUnderV1 = evaluator.evaluate(readEvent, v1);

        log.info("规则命中(v1) delete: {}", deleteUnderV1.violations().stream().map(Violation::explanation).toList());
        assertFalse(deleteUnderV1.compliant());
        assertEquals("R-DEL", deleteUnderV1.violations().get(0).ruleId());
        assertTrue(readUnderV1.compliant());

        // v2：新增禁止 export
        RuleRegistry.Snapshot v2 = registry.replaceRules(List.of(
                rule("R-DEL", 1, "delete", "HIGH"),
                rule("R-EXPORT", 1, "export", "MEDIUM")));
        assertEquals(2, v2.version());

        AuditEvent exportEvent = event("export");
        ComplianceVerdict exportUnderV2 = evaluator.evaluate(exportEvent, v2);
        log.info("规则命中(v2) export: {}", exportUnderV2.violations().stream().map(Violation::explanation).toList());
        assertFalse(exportUnderV2.compliant());
        assertEquals("R-EXPORT", exportUnderV2.violations().get(0).ruleId());

        // 历史结论可按旧快照重新复现，结果与当初一致
        ComplianceVerdict deleteReproduced = evaluator.evaluate(deleteEvent, registry.snapshotAt(1));
        assertEquals(deleteUnderV1.ruleSetVersion(), deleteReproduced.ruleSetVersion());
        assertEquals(deleteUnderV1.violations().size(), deleteReproduced.violations().size());
        assertEquals(deleteUnderV1.violations().get(0).ruleId(),
                deleteReproduced.violations().get(0).ruleId());
        log.info("历史结论复现: eventId={}, ruleSetVersion={}, 命中规则={}",
                deleteEvent.eventId(), deleteReproduced.ruleSetVersion(),
                deleteReproduced.violations().stream().map(Violation::ruleId).toList());

        // 规则演进不影响历史版本下 read 的合规结论
        assertTrue(evaluator.evaluate(readEvent, registry.snapshotAt(1)).compliant());
    }

    @Test
    void unparseableOrConflictingRulesFailTheWholeUpdate() {
        RuleRegistry registry = new RuleRegistry();
        registry.replaceRules(List.of(rule("R-DEL", 1, "delete", "HIGH")));

        // 冲突：ruleId 重复 -> 明确失败
        RuleParseException conflict = assertThrows(RuleParseException.class, () ->
                registry.replaceRules(List.of(
                        rule("R-DEL", 1, "delete", "HIGH"),
                        rule("R-DEL", 2, "export", "HIGH"))));
        log.warn("规则冲突被拒绝: {}", conflict.getMessage());

        // 不可解析：模式含非法字符 -> 明确失败
        RuleParseException badPattern = assertThrows(RuleParseException.class, () ->
                registry.replaceRules(List.of(
                        new ComplianceRule("R-BAD", 1, "bad", "*", "*", "a$(rm -rf /)", "LOW"))));
        log.warn("不可解析规则被拒绝: {}", badPattern.getMessage());

        // 缺字段 -> 明确失败
        RuleParseException missingSeverity = assertThrows(RuleParseException.class, () ->
                registry.replaceRules(List.of(
                        new ComplianceRule("R-X", 1, "x", "*", "*", "read", " "))));
        log.warn("缺字段规则被拒绝: {}", missingSeverity.getMessage());

        // 旧规则集继续生效，版本号未推进
        assertEquals(1, registry.current().version());
        assertEquals(1, registry.current().rules().size());
    }
}
