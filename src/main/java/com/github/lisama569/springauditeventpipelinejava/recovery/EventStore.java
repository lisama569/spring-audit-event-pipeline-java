package com.github.lisama569.springauditeventpipelinejava.recovery;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;

import java.util.function.Consumer;

/**
 * 已落账事件的持久化存储：受理时追加，恢复/补投时按范围读取。
 * 追加须在向外部确认“已受理”前刷盘，保证重启后已收事件不丢。
 */
public abstract class EventStore implements EventSource {

    /** 追加一条已落账事件（按 eventId 幂等：重复追加直接忽略）。 */
    public abstract void append(AuditEvent event);

    /**
     * 扫描所有已持久化事件（恢复用），以流式回调推送、内存有界。
     * 终态事件由调用方依据投递进度自行过滤。
     */
    public abstract void forEach(Consumer<AuditEvent> consumer);
}
