package com.github.lisama569.springauditeventpipelinejava.ingest;

import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceEvaluator;
import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceVerdict;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.compliance.VerdictRepository;
import com.github.lisama569.springauditeventpipelinejava.compliance.Violation;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardService;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.model.IngestResult;
import com.github.lisama569.springauditeventpipelinejava.model.RawAuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 采集入口服务：多入口（HTTP、直接调用等）共用同一条流水线。
 * 重复提交在账本层幂等：重复事件不会重新判定、不会重新排程转发。
 */
public class AuditIngestService {

    private static final Logger log = LoggerFactory.getLogger(AuditIngestService.class);

    private final AuditEventNormalizer normalizer;
    private final AuditEventStore store;
    private final RuleRegistry ruleRegistry;
    private final ComplianceEvaluator evaluator;
    private final VerdictRepository verdictRepository;
    private final ForwardService forwardService;

    public AuditIngestService(AuditEventNormalizer normalizer,
                              AuditEventStore store,
                              RuleRegistry ruleRegistry,
                              ComplianceEvaluator evaluator,
                              VerdictRepository verdictRepository,
                              ForwardService forwardService) {
        this.normalizer = normalizer;
        this.store = store;
        this.ruleRegistry = ruleRegistry;
        this.evaluator = evaluator;
        this.verdictRepository = verdictRepository;
        this.forwardService = forwardService;
    }

    public IngestResult ingest(RawAuditEvent raw) {
        log.info("输入事件(source={}, eventId={}, timestamp={}, actor={}, resource={}, action={})",
                raw == null ? null : raw.source(),
                raw == null ? null : raw.eventId(),
                raw == null ? null : raw.timestamp(),
                raw == null ? null : raw.actor(),
                raw == null ? null : raw.resource(),
                raw == null ? null : raw.action());

        AuditEventNormalizer.Outcome outcome = normalizer.normalize(raw);
        if (outcome instanceof AuditEventNormalizer.Outcome.Rejected rejected) {
            log.warn("事件被拒绝: eventId={}, 原因={}",
                    raw == null ? null : raw.eventId(), rejected.reason());
            return IngestResult.rejected(rejected.reason());
        }
        AuditEvent normalized = ((AuditEventNormalizer.Outcome.Ok) outcome).event();

        AuditEventStore.StoredResult stored = store.putIfAbsent(normalized);
        AuditEvent event = stored.event();
        if (!stored.inserted()) {
            log.info("重复提交被幂等吸收: eventId={}, 既有序列号={}", event.eventId(), event.sequence());
            return IngestResult.duplicate(event);
        }

        RuleRegistry.Snapshot snapshot = ruleRegistry.current();
        ComplianceVerdict verdict = evaluator.evaluate(event, snapshot);
        ComplianceVerdict saved = verdictRepository.saveIfAbsent(event.eventId(), verdict);

        for (Violation v : saved.violations()) {
            log.info("规则命中: eventId={} -> {}", event.eventId(), v.explanation());
        }
        log.info("最终判定: eventId={}, 规则集版本={}, 合规={}, 命中数={}",
                event.eventId(), saved.ruleSetVersion(), saved.compliant(), saved.violations().size());

        forwardService.enqueue(event, saved);
        return IngestResult.accepted(event);
    }
}
