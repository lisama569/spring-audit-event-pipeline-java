package com.github.lisama569.springauditeventpipelinejava.pipeline;

import com.github.lisama569.springauditeventpipelinejava.forward.ForwardingService;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditLedger;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleEngine;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.recovery.DurableDeliveryStateStore;
import com.github.lisama569.springauditeventpipelinejava.recovery.DurableEventStore;
import com.github.lisama569.springauditeventpipelinejava.recovery.DurableLocalAuditSink;
import com.github.lisama569.springauditeventpipelinejava.recovery.Reconciler;
import com.github.lisama569.springauditeventpipelinejava.recovery.ReplayService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

@Configuration
public class PipelineConfig {

    @Bean
    public DurableEventStore eventStore(@Value("${audit.data-dir:./audit-data}") String dataDir) {
        Path dir = Path.of(dataDir);
        return new DurableEventStore(dir.resolve("events.log"));
    }

    @Bean
    public DurableDeliveryStateStore deliveryStateStore(
            @Value("${audit.data-dir:./audit-data}") String dataDir) {
        return new DurableDeliveryStateStore(Path.of(dataDir).resolve("delivery-state.log"));
    }

    @Bean
    public DurableLocalAuditSink localAuditSink(
            @Value("${audit.data-dir:./audit-data}") String dataDir) {
        DurableLocalAuditSink sink =
                new DurableLocalAuditSink(Path.of(dataDir).resolve("sink-delivered.log"));
        sink.recover(); // 先重建目标端已送达视图，后续投递/恢复据此幂等
        return sink;
    }

    @Bean
    public AuditLedger auditLedger(DurableEventStore eventStore) {
        return new AuditLedger(eventStore);
    }

    @Bean
    public RuleRegistry ruleRegistry() {
        return new RuleRegistry();
    }

    @Bean
    public RuleEngine ruleEngine() {
        return new RuleEngine();
    }

    @Bean(destroyMethod = "shutdown")
    public ForwardingService forwardingService(DurableLocalAuditSink sink,
                                               DurableDeliveryStateStore stateStore,
                                               DurableEventStore eventStore) {
        ForwardingService service = new ForwardingService(sink, stateStore, eventStore);
        service.start(); // start 内部执行重启恢复：PENDING 事件继续送，DELIVERED 不重投
        return service;
    }

    @Bean
    public Reconciler reconciler(AuditLedger ledger, DurableDeliveryStateStore stateStore,
                                 DurableLocalAuditSink sink) {
        return new Reconciler(ledger, stateStore, sink);
    }

    @Bean
    public ReplayService replayService(DurableEventStore eventStore, ForwardingService forwarding) {
        return new ReplayService(eventStore, forwarding);
    }

    @Bean
    public AuditPipeline auditPipeline(AuditLedger ledger, RuleRegistry registry,
                                       RuleEngine engine, ForwardingService forwarding) {
        return new AuditPipeline(ledger, registry, engine, forwarding);
    }
}
