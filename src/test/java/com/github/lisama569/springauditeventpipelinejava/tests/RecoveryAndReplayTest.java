package com.github.lisama569.springauditeventpipelinejava.tests;

import com.github.lisama569.springauditeventpipelinejava.api.dto.AuditEventRequest;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleEngine;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.forward.DeliveryState;
import com.github.lisama569.springauditeventpipelinejava.forward.DeliveryStatus;
import com.github.lisama569.springauditeventpipelinejava.forward.DurableDeliveryStateStore;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardingService;
import com.github.lisama569.springauditeventpipelinejava.forward.LocalAuditSink;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditLedger;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.pipeline.AuditPipeline;
import com.github.lisama569.springauditeventpipelinejava.recovery.DurableEventStore;import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重启恢复语义：
 * - 已送达事件重启后不再投递（目标端不出现第二条）；
 * - 已受理未送达事件重启后续投完成（不丢失）；
 * - 恢复扫描期间新来的实时事件同样正常受理投递，两类并发不卡死、不重复。
 */
class RecoveryAndReplayTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryAndReplayTest.class);

    @TempDir
    Path tempDir;

    private static AuditEventRequest event(String id, String occurredAt) {
        return new AuditEventRequest(id, occurredAt, "alice", "db", "READ", Map.of());
    }

    private AuditPipeline newPipeline(LocalAuditSink sink) {
        AuditLedger ledger = new AuditLedger();
        RuleRegistry registry = new RuleRegistry();
        DurableEventStore eventStore = new DurableEventStore(tempDir);
        DurableDeliveryStateStore stateStore = new DurableDeliveryStateStore(tempDir);
        ForwardingService forwarding = new ForwardingService(sink, stateStore, eventStore);
        forwarding.start();
        return new AuditPipeline(ledger, registry, new RuleEngine(), forwarding);
    }

    /** 与真实进程一致：目标端内容持久化在数据目录，重启后恢复。 */
    private LocalAuditSink durableSink() {
        return new LocalAuditSink(tempDir);
    }

    @Test
    void resumesPendingAfterRestartWithoutDuplicatingDelivered() {
        LocalAuditSink sink = durableSink();

        // —— 第一个进程：3 条事件全部送达 ——
        AuditPipeline p1 = newPipeline(sink);
        ForwardingService f1 = extractForwarding(p1);
        for (int i = 1; i <= 3; i++) {
            p1.submit(event("evt-" + i, "2026-10-01T10:00:0" + i + "Z"));
        }
        f1.awaitQuiescent(10_000);
        assertEquals(3, sink.deliveredCount());
        log.info("[进程1] 已投递事件={}", sink.deliveredEventIds());
        f1.shutdown();
        assertEquals(List.of("evt-1", "evt-2", "evt-3"), sink.deliveredEventIds());

        // —— 重启：不新增事件，已送达的不得再投 ——
        AuditPipeline p2 = newPipeline(sink);
        ForwardingService f2 = extractForwarding(p2);
        f2.awaitQuiescent(10_000);
        log.info("[进程2 空重启] 目标端事件={}，各事件尝试次数={}", sink.deliveredEventIds(),
                f2.states().stream().map(s -> s.eventId() + "=" + s.attempts()).toList());
        assertEquals(3, sink.deliveredCount(), "重启后已送达事件不得重复投递");
        assertEquals(3, sink.deliveredEventIds().stream().distinct().count());
        for (String id : List.of("evt-1", "evt-2", "evt-3")) {
            DeliveryState state = f2.stateOf(id).orElseThrow();
            assertEquals(DeliveryStatus.DELIVERED, state.status());
            assertEquals(1, state.attempts(), "已送达事件重启后续投不得增加尝试次数");
        }
        // 目标端持久化恢复后，进度与目标端实际内容必须重新对得上
        assertTrue(f2.reconcile().consistent(), "整进程重启后对账必须一致");
        f2.shutdown();
    }

    @Test
    void resumesPendingEventsThatWereNotDeliveredAtShutdown() {
        LocalAuditSink sink = durableSink();

        // —— 第一个进程：ok-1 正常送达，doomed 永久失败成死信，pending-1 已送达后被拨回
        //    PENDING，模拟“事件已持久化、但崩溃发生在进度确认前”的停机现场 ——
        AuditPipeline p1 = newPipeline(sink);
        ForwardingService f1 = extractForwarding(p1);
        sink.failAlways("doomed");
        p1.submit(event("ok-1", "2026-10-01T09:00:00Z"));
        p1.submit(event("doomed", "2026-10-01T09:00:01Z"));
        p1.submit(event("pending-1", "2026-10-01T09:00:02Z"));
        f1.awaitQuiescent(10_000);
        assertEquals(2, sink.deliveredCount()); // ok-1 + pending-1
        assertEquals(1, f1.deadLetterCount());
        f1.shutdown(); // 先优雅停机（快照落盘），再模拟崩溃现场

        // 模拟崩溃现场：事件体已持久化、但进度卡在 DELIVERED 之前。
        // 停机后另开存储把 pending-1 拨回 PENDING（此时目标端已有幂等记录，
        // 正好验证“续投尝试会发生，但目标端不会出现第二条”）
        DurableDeliveryStateStore states1 = new DurableDeliveryStateStore(tempDir);
        DeliveryState before = states1.find("pending-1").orElseThrow();
        states1.save(new DeliveryState("pending-1", DeliveryStatus.PENDING, 0, before.updatedAtMs()));

        // 再制造一条“事件体已落盘、进度为 PENDING、目标端从无记录”的未投递事件（崩溃于首次投递前）
        DurableEventStore eventStore = new DurableEventStore(tempDir);
        AuditEvent neverDelivered = new AuditEvent("pending-2", java.time.Instant.parse("2026-10-01T09:00:03Z"),
                "alice", "db", "READ", Map.of(), 99, java.time.Instant.now());
        eventStore.append(neverDelivered);
        states1.registerIfAbsent("pending-2");
        states1.close();

        // —— 重启：pending-1 续投（目标端幂等不增记录）、pending-2 首次真正送达；
        //    ok-1 不重投；doomed 保持死信 ——
        AuditPipeline p2 = newPipeline(sink);
        ForwardingService f2 = extractForwarding(p2);
        f2.awaitQuiescent(10_000);
        log.info("[进程2] 目标端={}，死信={}，pending-1 尝试次数={}",
                sink.deliveredEventIds(),
                f2.deadLetters().stream().map(AuditEvent::eventId).toList(),
                f2.stateOf("pending-1").orElseThrow().attempts());
        assertEquals(3, sink.deliveredCount(), "停机前未送达的 pending-2 必须真正送达；幂等记录不重复");
        assertTrue(sink.deliveredEventIds()
                .containsAll(List.of("ok-1", "pending-1", "pending-2")));
        assertEquals(DeliveryStatus.DELIVERED, f2.stateOf("pending-1").orElseThrow().status(),
                "重启后停机前未确认送达的事件必须续投到终态 DELIVERED");
        assertEquals(1, f2.stateOf("pending-1").orElseThrow().attempts(),
                "续投只实际尝试一次（失败/重复都不重复计数）");
        assertEquals(DeliveryStatus.DELIVERED, f2.stateOf("pending-2").orElseThrow().status());
        assertEquals(1, f2.stateOf("pending-2").orElseThrow().attempts(), "从未投递的事件重启后首次即送达");
        assertEquals(1, f2.deadLetterCount(), "死信跨重启保留，不自动复活");
        assertEquals("doomed", f2.deadLetters().get(0).eventId());
        assertEquals(DeliveryStatus.DEAD, f2.stateOf("doomed").orElseThrow().status());
        assertTrue(f2.reconcile().consistent(), "重启续投完成后进度与目标端必须一致");
        f2.shutdown();
    }

    @Test
    void recoveryRunsConcurrentlyWithLiveEventsWithoutDeadlockOrDuplicates() {
        LocalAuditSink sink = durableSink();

        // 进程1：仅送达历史 3 条
        AuditPipeline p1 = newPipeline(sink);
        ForwardingService f1 = extractForwarding(p1);
        for (int i = 1; i <= 3; i++) {
            p1.submit(event("hist-" + i, "2026-10-01T08:00:0" + i + "Z"));
        }
        f1.awaitQuiescent(10_000);
        f1.shutdown();
        sink.failNextDeliveries(4); // 重启后让恢复投递与实时投递共同经历失败重试

        // 进程2：恢复进行的同时并发写入实时新事件
        AuditPipeline p2 = newPipeline(sink);
        ForwardingService f2 = extractForwarding(p2);
        List<Thread> writers = new java.util.ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            final int n = i;
            writers.add(new Thread(() -> p2.submit(event("live-" + n, "2026-10-01T11:00:0" + n + "Z"))));
        }
        writers.forEach(Thread::start);
        writers.forEach(t -> {
            try {
                t.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        f2.awaitQuiescent(15_000);

        List<String> expected = List.of("hist-1", "hist-2", "hist-3", "live-1", "live-2",
                "live-3", "live-4", "live-5");
        log.info("[恢复+实时并发] 目标端最终={}", sink.deliveredEventIds());
        assertEquals(expected.size(), sink.deliveredCount(), "恢复事件与实时事件合并不重不漏");
        assertTrue(sink.deliveredEventIds().containsAll(expected), "历史恢复与实时事件都必须送达");
        assertEquals(expected.size(), sink.deliveredEventIds().stream().distinct().count(),
                "并发恢复 + 实时投递不得产生任何重复记录");
        f2.shutdown();
    }

    private static ForwardingService extractForwarding(AuditPipeline pipeline) throws RuntimeException {
        try {
            var field = AuditPipeline.class.getDeclaredField("forwarding");
            field.setAccessible(true);
            return (ForwardingService) field.get(pipeline);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
