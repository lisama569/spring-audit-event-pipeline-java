package com.github.lisama569.springauditeventpipelinejava.pipeline;

import com.github.lisama569.springauditeventpipelinejava.compliance.RuleEngine;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.forward.DurableDeliveryStateStore;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardingService;
import com.github.lisama569.springauditeventpipelinejava.forward.LocalAuditSink;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditLedger;
import com.github.lisama569.springauditeventpipelinejava.recovery.DurableEventStore;
import com.github.lisama569.springauditeventpipelinejava.recovery.EventSource;
import com.github.lisama569.springauditeventpipelinejava.recovery.EventStore;
import com.github.lisama569.springauditeventpipelinejava.recovery.LedgerEventSource;
import com.github.lisama569.springauditeventpipelinejava.recovery.ReplayService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

@Configuration
public class PipelineConfig {

    /**
     * 持久化数据目录（事件存储 + 投递进度）。
     * 默认本地 ./data/audit-pipeline；留空（audit.pipeline.data-dir=none）则退回纯内存模式
     * （语义不变，但不支持进程重启恢复）。
     */
    @Value("${audit.pipeline.data-dir:./data/audit-pipeline}")
    private String dataDir;

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
        return persistenceEnabled() ? new LocalAuditSink(Path.of(dataDir)) : new LocalAuditSink();
    }

    @Bean
    public EventStore eventStore() {
        if (persistenceEnabled()) {
            return new DurableEventStore(Path.of(dataDir));
        }
        return null;
    }

    @Bean
    public DurableDeliveryStateStore durableDeliveryStateStore() {
        if (persistenceEnabled()) {
            return new DurableDeliveryStateStore(Path.of(dataDir));
        }
        return null;
    }

    @Bean
    public EventSource eventSource(EventStore eventStore, AuditLedger ledger) {
        return eventStore != null ? eventStore : new LedgerEventSource(ledger);
    }

    @Bean(destroyMethod = "shutdown")
    public ForwardingService forwardingService(LocalAuditSink sink,
                                               DurableDeliveryStateStore durableStateStore,
                                               EventStore eventStore) {
        ForwardingService service;
        if (durableStateStore != null && eventStore != null) {
            service = new ForwardingService(sink, durableStateStore, eventStore);
        } else {
            service = new ForwardingService(sink);
        }
        service.start();
        return service;
    }

    @Bean
    public ReplayService replayService(EventSource eventSource, ForwardingService forwarding) {
        return new ReplayService(eventSource, forwarding);
    }

    @Bean
    public AuditPipeline auditPipeline(AuditLedger ledger, RuleRegistry registry,
                                       RuleEngine engine, ForwardingService forwarding) {
        return new AuditPipeline(ledger, registry, engine, forwarding);
    }

    private boolean persistenceEnabled() {
        return dataDir != null && !dataDir.isBlank() && !"none".equalsIgnoreCase(dataDir.trim());
    }
}
