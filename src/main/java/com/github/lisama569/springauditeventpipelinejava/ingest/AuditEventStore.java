package com.github.lisama569.springauditeventpipelinejava.ingest;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 审计事件账本（内存实现）。
 * <ul>
 *   <li>按 eventId 幂等：重复提交返回既有记录，不产生第二条独立记录；</li>
 *   <li>序列号在写入临界区内单调递增，仅反映落账顺序，用于乱序到达下的可追溯排序；</li>
 *   <li>所有方法 synchronized，保证多入口并发写入下的一致性。</li>
 * </ul>
 */
public class AuditEventStore {

    private final Map<String, AuditEvent> byId = new LinkedHashMap<>();
    private long nextSequence = 0;

    public synchronized boolean contains(String eventId) {
        return byId.containsKey(eventId);
    }

    public synchronized Optional<AuditEvent> find(String eventId) {
        return Optional.ofNullable(byId.get(eventId));
    }

    /**
     * 仅当 eventId 不存在时写入并分配序列号；否则返回已存在记录（幂等）。
     * 原子操作，避免“先 contains 再 put”的 check-then-act 竞态。
     */
    public synchronized StoredResult putIfAbsent(AuditEvent normalized) {
        AuditEvent existing = byId.get(normalized.eventId());
        if (existing != null) {
            return new StoredResult(existing, false);
        }
        AuditEvent stored = new AuditEvent(
                normalized.eventId(),
                normalized.source(),
                normalized.timestamp(),
                normalized.actor(),
                normalized.resource(),
                normalized.action(),
                normalized.attributes(),
                nextSequence++,
                Instant.now());
        byId.put(stored.eventId(), stored);
        return new StoredResult(stored, true);
    }

    /** 落账结果：inserted=true 表示本次新建，false 表示幂等命中已有记录。 */
    public record StoredResult(AuditEvent event, boolean inserted) {
    }

    /** 按落账序列号（即乱序到达下的账本顺序）返回全部事件。 */
    public synchronized List<AuditEvent> findAll() {
        return List.copyOf(byId.values());
    }
}
