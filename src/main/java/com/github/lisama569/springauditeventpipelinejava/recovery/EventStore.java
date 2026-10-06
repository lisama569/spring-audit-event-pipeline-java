package com.github.lisama569.springauditeventpipelinejava.recovery;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * 事件持久化存储：重启后可重建账本，并支持按业务时间范围/事件 id 的有界分页读取
 * （补投工作集不随总事件量线性膨胀）。
 */
public interface EventStore {

    /** 追加一条已落账事件（按 ledgerSeq 顺序追加，崩溃时以已刷盘记录为准）。 */
    void append(AuditEvent event);

    /** 启动时读取全部事件以重建内存账本。 */
    List<AuditEvent> loadAll();

    Optional<AuditEvent> find(String eventId);

    /**
     * 按 (occurredAt, ledgerSeq) 升序的游标分页：返回 fromExclusive 之后、业务时间落在
     * [from, to) 内的最多 limit 条事件；fromExclusive 为 null 表示从头开始。
     */
    List<AuditEvent> pageRange(Instant from, Instant to, String fromExclusive, int limit);

    /** 按事件 id 批量取数（仅存在的事件）。 */
    List<AuditEvent> findByIds(List<String> eventIds);

    long count();
}
