package com.github.lisama569.springauditeventpipelinejava.ingest;

import com.github.lisama569.springauditeventpipelinejava.api.dto.AuditEventRequest;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.model.IngestResult;
import com.github.lisama569.springauditeventpipelinejava.model.RejectionReason;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 线程安全落账存储：
 * - 按 eventId 去重（重复提交返回 DUPLICATE，不产生新记录）；
 * - ledgerSeq 单调递增，记录到达顺序，保证可追溯；
 * - snapshotOrdered() 按 (occurredAt, ledgerSeq) 排序，乱序到达不破坏业务顺序语义。
 */
public class AuditLedger {

    private final Map<String, AuditEvent> eventsById = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong(0);
    private final AuditEventValidator validator = new AuditEventValidator();

    public IngestResult append(AuditEventRequest request) {
        Optional<RejectionReason> rejection = validator.validate(request);
        if (rejection.isPresent()) {
            return IngestResult.rejected(rejection.get());
        }
        AuditEvent event = new AuditEvent(
                request.eventId().trim(),
                Instant.parse(request.occurredAt()),
                request.actor().trim(),
                request.resource().trim(),
                request.action().trim(),
                request.attributes() == null ? Map.of() : Map.copyOf(request.attributes()),
                0L,
                Instant.now());
        // 先占位去重：同一 eventId 并发提交只有一条能落账
        AuditEvent existing = eventsById.putIfAbsent(event.eventId(), event);
        if (existing != null) {
            return IngestResult.duplicate(existing);
        }
        // 占位成功后赋予全局单调序号（序号与落账可见性的先后不影响正确性，
        // 因为 snapshotOrdered 只在无并发写入或调用方接受弱一致时使用；
        // 为保证严格一致，这里用同步块赋号并替换记录）
        synchronized (this) {
            AuditEvent sequenced = new AuditEvent(
                    event.eventId(), event.occurredAt(), event.actor(), event.resource(),
                    event.action(), event.attributes(), sequence.incrementAndGet(), event.ingestedAt());
            eventsById.put(event.eventId(), sequenced);
            return IngestResult.accepted(sequenced);
        }
    }

    public Optional<AuditEvent> find(String eventId) {
        return Optional.ofNullable(eventsById.get(eventId));
    }

    /** 业务顺序视图：按 (occurredAt, ledgerSeq) 排序，乱序到达结果一致。 */
    public List<AuditEvent> snapshotOrdered() {
        List<AuditEvent> snapshot = new ArrayList<>(eventsById.values());
        snapshot.sort(Comparator.comparing(AuditEvent::occurredAt)
                .thenComparingLong(AuditEvent::ledgerSeq));
        return List.copyOf(snapshot);
    }

    public int size() {
        return eventsById.size();
    }
}
