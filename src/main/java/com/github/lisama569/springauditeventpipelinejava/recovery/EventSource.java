package com.github.lisama569.springauditeventpipelinejava.recovery;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * 事件来源：供重启恢复与按范围补投读取历史事件。
 * 范围查询以流式回调逐条推送，长跨度补投时内存占用不随事件总量线性膨胀。
 */
public interface EventSource {

    Optional<AuditEvent> find(String eventId);

    /**
     * 按业务时间区间 [from, to]（闭区间），以 (occurredAt, ledgerSeq) 稳定顺序逐条推送事件。
     * 实现必须保证内存占用有界（不得一次性载入全量事件）。
     */
    void forEachInRange(Instant from, Instant to, Consumer<AuditEvent> consumer);

    /** 按指定 eventId 集合逐个推送（存在的才推，稳定顺序按 ledgerSeq）。 */
    void forEachIds(java.util.List<String> eventIds, Consumer<AuditEvent> consumer);
}
