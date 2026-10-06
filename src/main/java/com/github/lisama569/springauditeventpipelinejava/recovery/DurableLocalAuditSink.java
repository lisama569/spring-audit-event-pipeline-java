package com.github.lisama569.springauditeventpipelinejava.recovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.springauditeventpipelinejava.forward.LocalAuditSink;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

/**
 * 带本地持久化的投递目标。
 * - 事件首次被接受时同步追加落盘（fsync），内存更新与落盘在同一把锁内完成；
 * - 重启后 recover() 从日志重建，已送达记录不会因重复送达再出现第二条；
 * - deliveredEventIds() 返回按 (occurredAt, ledgerSeq) 的稳定业务顺序视图，
 *   使“补投与实时并发后目标端最终内容 == 各事件正常投递一次”的结果可断言。
 */
public class DurableLocalAuditSink extends LocalAuditSink {

    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    private final DurableLog log;
    private volatile boolean recovered;

    public DurableLocalAuditSink(Path file) {
        this.log = new DurableLog(file);
    }

    /** 重启后从持久化日志重建目标端视图（幂等，仅生效一次）。 */
    public synchronized void recover() {
        if (recovered) {
            return;
        }
        for (String line : log.readAll()) {
            seedFromRecovery(parse(line));
        }
        recovered = true;
    }

    @Override
    public synchronized boolean deliver(AuditEvent event) {
        if (!recovered) {
            recover(); // 容错：构造后未显式恢复时，首次投递前自动恢复一次
        }
        // 首次接受时 super.deliver 会回调 onAccepted 完成落盘，已存在则仅幂等返回
        return super.deliver(event);
    }

    @Override
    protected synchronized void onAccepted(AuditEvent event) {
        log.append(write(event));
    }

    @Override
    public synchronized List<String> deliveredEventIds() {
        return acceptedEventsView().stream()
                .sorted(Comparator.comparing(AuditEvent::occurredAt).thenComparingLong(AuditEvent::ledgerSeq))
                .map(AuditEvent::eventId)
                .toList();
    }

    @Override
    public synchronized List<AuditEvent> deliveredEvents() {
        return acceptedEventsView().stream()
                .sorted(Comparator.comparing(AuditEvent::occurredAt).thenComparingLong(AuditEvent::ledgerSeq))
                .toList();
    }

    private static String write(AuditEvent event) {
        try {
            return MAPPER.writeValueAsString(event);
        } catch (IOException e) {
            throw new UncheckedIOException("序列化目标端事件失败", e);
        }
    }

    private static AuditEvent parse(String line) {
        try {
            return MAPPER.readValue(line, AuditEvent.class);
        } catch (IOException e) {
            throw new UncheckedIOException("反序列化目标端事件失败: " + line, e);
        }
    }
}
