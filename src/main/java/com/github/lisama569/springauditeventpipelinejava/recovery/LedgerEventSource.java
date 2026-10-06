package com.github.lisama569.springauditeventpipelinejava.recovery;

import com.github.lisama569.springauditeventpipelinejava.ingest.AuditLedger;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;

import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * 基于内存 {@link AuditLedger} 的事件源（无持久化部署/测试使用）。
 * 范围与 id 查询都直接复用账本的业务顺序快照；持久化部署请用 {@link DurableEventStore}，
 * 其范围扫描内存有界。
 */
public class LedgerEventSource implements EventSource {

    private final AuditLedger ledger;

    public LedgerEventSource(AuditLedger ledger) {
        this.ledger = ledger;
    }

    @Override
    public Optional<AuditEvent> find(String eventId) {
        return ledger.find(eventId);
    }

    @Override
    public void forEachInRange(Instant from, Instant to, Consumer<AuditEvent> consumer) {
        if (from.isAfter(to)) {
            throw new IllegalArgumentException("时间范围非法: from=" + from + " > to=" + to);
        }
        for (AuditEvent event : ledger.snapshotOrdered()) {
            if (!event.occurredAt().isBefore(from) && !event.occurredAt().isAfter(to)) {
                consumer.accept(event);
            }
        }
    }

    @Override
    public void forEachIds(java.util.List<String> eventIds, Consumer<AuditEvent> consumer) {
        Set<String> unique = new HashSet<>(eventIds);
        for (AuditEvent event : ledger.snapshotOrdered()) {
            if (unique.contains(event.eventId())) {
                consumer.accept(event);
            }
        }
    }
}
