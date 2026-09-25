package com.github.lisama569.springauditeventpipelinejava.pipeline;

import com.github.lisama569.springauditeventpipelinejava.compliance.RuleEngine;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardingService;
import com.github.lisama569.springauditeventpipelinejava.forward.LocalAuditSink;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditLedger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PipelineConfig {

    @Bean
    public AuditLedger auditLedger() {
        return new AuditLedger();
    }

    @Bean
    public RuleRegistry ruleRegistry() {
        return new RuleRegistry();
    }

    @Bean
    public RuleEngine ruleEngine() {
        return new RuleEngine();
    }

    @Bean
    public LocalAuditSink localAuditSink() {
        return new LocalAuditSink();
    }

    @Bean(destroyMethod = "shutdown")
    public ForwardingService forwardingService(LocalAuditSink sink) {
        ForwardingService service = new ForwardingService(sink);
        service.start();
        return service;
    }

    @Bean
    public AuditPipeline auditPipeline(AuditLedger ledger, RuleRegistry registry,
                                       RuleEngine engine, ForwardingService forwarding) {
        return new AuditPipeline(ledger, registry, engine, forwarding);
    }
}
