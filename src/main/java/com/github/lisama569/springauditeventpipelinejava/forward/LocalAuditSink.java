package com.github.lisama569.springauditeventpipelinejava.forward;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地转发目标：幂等写入（按 eventId 去重），重复投递不产生重复数据。
 * 支持测试注入失败以验证重试语义。
 */
public class LocalAuditSink {

    private final Set<String> delivered = new LinkedHashSet<>();
    private final Set<String> alwaysFailing = new LinkedHashSet<>();
    private final AtomicInteger injectedFailures = new AtomicInteger(0);

    /**
     * 幂等投递。
     * @return true 成功（含已被去重的重复投递）；false 暂时失败，可重试。
     */
    public synchronized boolean deliver(AuditEvent event) {
        if (alwaysFailing.contains(event.eventId())) {
            return false;
        }
        if (injectedFailures.getAndUpdate(c -> c > 0 ? c - 1 : 0) > 0) {
            return false;
        }
        delivered.add(event.eventId()); // Set 语义保证幂等
        return true;
    }

    public synchronized List<String> deliveredEventIds() {
        return List.copyOf(new ArrayList<>(delivered));
    }

    public synchronized int deliveredCount() {
        return delivered.size();
    }

    /** 测试用：注入接下来 count 次投递失败。 */
    public void failNextDeliveries(int count) {
        injectedFailures.addAndGet(count);
    }

    /** 测试用：指定 eventId 的投递永远失败（模拟目标端持续性故障）。 */
    public synchronized void failAlways(String eventId) {
        alwaysFailing.add(eventId);
    }
}
