package com.github.lisama569.springauditeventpipelinejava;

import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceEvaluator;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.compliance.VerdictRepository;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardService;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardTarget;
import com.github.lisama569.springauditeventpipelinejava.forward.LocalFileForwardTarget;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditEventNormalizer;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditEventStore;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditIngestService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * 流水线装配：采集 -> 规范化 -> 幂等落账 -> 合规判定 -> 可靠转发。
 */
@Configuration
public class PipelineConfig {

    @Bean
    public AuditEventNormalizer auditEventNormalizer() {
        return new AuditEventNormalizer();
    }

    @Bean
    public AuditEventStore auditEventStore() {
        return new AuditEventStore();
    }

    @Bean
    public RuleRegistry ruleRegistry() {
        return new RuleRegistry();
    }

    @Bean
    public ComplianceEvaluator complianceEvaluator() {
        return new ComplianceEvaluator();
    }

    @Bean
    public VerdictRepository verdictRepository() {
        return new VerdictRepository();
    }

    @Bean
    public ForwardTarget forwardTarget(@Value("${audit.forward.file:build/audit-forward.jsonl}") String file) {
        return new LocalFileForwardTarget(Path.of(file));
    }

    @Bean(destroyMethod = "close")
    public ForwardService forwardService(ForwardTarget forwardTarget) {
        return new ForwardService(forwardTarget);
    }

    @Bean
    public AuditIngestService auditIngestService(AuditEventNormalizer normalizer,
                                                 AuditEventStore store,
                                                 RuleRegistry ruleRegistry,
                                                 ComplianceEvaluator evaluator,
                                                 VerdictRepository verdictRepository,
                                                 ForwardService forwardService) {
        return new AuditIngestService(normalizer, store, ruleRegistry, evaluator,
                verdictRepository, forwardService);
    }
}
