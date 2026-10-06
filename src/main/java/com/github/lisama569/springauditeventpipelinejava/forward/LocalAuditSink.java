package com.github.lisama569.springauditeventpipelinejava.forward;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地转发目标：幂等写入（按 eventId 去重），重复投递不产生重复数据。
 * 支持测试注入失败以验证重试语义。
 * 子类可通过钩子加入持久化（见 DurableLocalAuditSink），投递判定语义保持不变。
 */
public class LocalAuditSink {

    private final Set<String> delivered = new LinkedHashSet<>();
    private final Set<String> alwaysFailing = new LinkedHashSet<>();
    private final Map<String, AuditEvent> eventsById = new LinkedHashMap<>();
    private final AtomicInteger injectedFailures = new AtomicInteger(0);
    private final AtomicInteger rawAttempts = new AtomicInteger(0);

    /**
     * 幂等投递。
     * @return true 成功（含已被去重的重复投递）；false 暂时失败，可重试。
     */
    public synchronized boolean deliver(AuditEvent event) {
        rawAttempts.incrementAndGet();
        if (alwaysFailing.contains(event.eventId())) {
            return false;
        }
        if (injectedFailures.getAndUpdate(c -> c > 0 ? c - 1 : 0) > 0) {
            return false;
        }
        boolean accepted = delivered.add(event.eventId()); // Set 语义保证幂等
        eventsById.putIfAbsent(event.eventId(), event);
        if (accepted) {
            onAccepted(event);
        }
        return true;
    }

    /** 新事件首次被目标端接受时的钩子（持久化子类在此落盘）。 */
    protected void onAccepted(AuditEvent event) {
        // 默认无持久化
    }

    /** 子类重建时回填内存视图。 */
    protected synchronized void seedFromRecovery(AuditEvent event) {
        delivered.add(event.eventId());
        eventsById.putIfAbsent(event.eventId(), event);
    }

    /** 子类使用：已接受事件的快照（eventId -> 首次送达的事件体）。 */
    protected synchronized List<AuditEvent> acceptedEventsView() {
        return new ArrayList<>(eventsById.values());
    }

    public synchronized List<String> deliveredEventIds() {
        return List.copyOf(new ArrayList<>(delivered));
    }

    public synchronized int deliveredCount() {
        return delivered.size();
    }

    /** 目标端内容（首次送达的事件体），默认按送达顺序；持久化子类可提供业务顺序视图。 */
    public synchronized List<AuditEvent> deliveredEvents() {
        return List.copyOf(new ArrayList<>(eventsById.values()));
    }

    /** 实际向目标端发起投递尝试的次数（含失败与重复尝试），供对账核对。 */
    public int rawAttemptCount() {
        return rawAttempts.get();
    }

    /** 测试用：注入接下来 count 次投递失败。 */
    public void failNextDeliveries(int count) {
        injectedFailures.addAndGet(count);
    }

    /** 测试用：指定 eventId 的投递永远失败（模拟目标端持续性故障）。 */
    public synchronized void failAlways(String eventId) {
        alwaysFailing.add(eventId);
    }

    protected Set<String> alwaysFailingIds() {
        return Collections.unmodifiableSet(alwaysFailing);
    }
}
