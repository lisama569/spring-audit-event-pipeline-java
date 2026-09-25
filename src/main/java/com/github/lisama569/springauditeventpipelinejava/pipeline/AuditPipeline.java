package com.github.lisama569.springauditeventpipelinejava.pipeline;

import com.github.lisama569.springauditeventpipelinejava.api.dto.AuditEventRequest;
import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceRule;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleEngine;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardingService;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditLedger;
import com.github.lisama569.springauditeventpipelinejava.model.IngestResult;
import com.github.lisama569.springauditeventpipelinejava.model.RuleVerdict;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 编排：采集落账 -> 合规判定（结论留痕） -> 转发。
 * 判定结论在落账时一次性固化（含规则版本快照），规则演进不影响历史结论。
 */
public class AuditPipeline {

    private final AuditLedger ledger;
    private final RuleRegistry registry;
    private final RuleEngine engine;
    private final ForwardingService forwarding;
    private final ConcurrentHashMap<String, List<RuleVerdict>> verdictsByEventId = new ConcurrentHashMap<>();

    public AuditPipeline(AuditLedger ledger, RuleRegistry registry, RuleEngine engine, ForwardingService forwarding) {
        this.ledger = ledger;
        this.registry = registry;
        this.engine = engine;
        this.forwarding = forwarding;
    }

    public IngestResult submit(AuditEventRequest request) {
        IngestResult result = ledger.append(request);
        if (result.status() != IngestResult.Status.ACCEPTED) {
            return result; // 拒绝与重复都不触发判定与转发
        }
        List<ComplianceRule> rules = registry.activeRules();
        List<RuleVerdict> verdicts = engine.evaluate(result.event(), rules);
        verdictsByEventId.computeIfAbsent(result.event().eventId(),
                k -> new CopyOnWriteArrayList<>()).addAll(verdicts);
        forwarding.enqueue(result.event());
        return result;
    }

    /** 历史结论查询：规则更新后仍返回落账时的结论快照。 */
    public List<RuleVerdict> verdictsOf(String eventId) {
        return verdictsByEventId.getOrDefault(eventId, List.of());
    }
}
