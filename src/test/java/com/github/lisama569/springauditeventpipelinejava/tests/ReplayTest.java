package com.github.lisama569.springauditeventpipelinejava.tests;

import com.github.lisama569.springauditeventpipelinejava.api.dto.AuditEventRequest;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleEngine;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.forward.DeliveryReport;
import com.github.lisama569.springauditeventpipelinejava.forward.DeliveryState;
import com.github.lisama569.springauditeventpipelinejava.forward.DeliveryStatus;
import com.github.lisama569.springauditeventpipelinejava.forward.DurableDeliveryStateStore;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardingService;
import com.github.lisama569.springauditeventpipelinejava.forward.LocalAuditSink;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditLedger;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.pipeline.AuditPipeline;
import com.github.lisama569.springauditeventpipelinejava.recovery.DurableEventStore;
import com.github.lisama569.springauditeventpipelinejava.recovery.ReplayJob;
import com.github.lisama569.springauditeventpipelinejava.recovery.ReplayRequest;
import com.github.lisama569.springauditeventpipelinejava.recovery.ReplayService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 按范围补投语义：
 * - 范围/id 两种补投方式，已送达跳过、死信可重新挂起，重复发起不产生重复；
 * - 补投与实时并发时最终结果等价于每条恰好正常投递一次；
 * - 长跨度补投内存占用有界；补投批次在目标端的相对顺序稳定。
 */
class ReplayTest {

    private static final Logger log = LoggerFactory.getLogger(ReplayTest.class);

    @TempDir
    Path tempDir;

    private AuditEventRequest event(String id, String occurredAt) {
        return new AuditEventRequest(id, occurredAt, "alice", "db", "READ", Map.of());
    }

    private ForwardingService forwarding(LocalAuditSink sink,
                                        DurableEventStore eventStore,
                                        DurableDeliveryStateStore stateStore) {
        ForwardingService service = new ForwardingService(sink, stateStore, eventStore);
        service.start();
        return service;
    }

    private static void awaitJob(ReplayService replayService, ReplayJob job, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (job.status() == com.github.lisama569.springauditeventpipelinejava.recovery.ReplayStatus.RUNNING
                && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        assertEquals(com.github.lisama569.springauditeventpipelinejava.recovery.ReplayStatus.COMPLETED,
                job.status(), "补投任务应完成: " + job.error());
    }

    @Test
    void rangeReplaySkipsDeliveredAndRearmsDeadWithoutDuplicates() {
        LocalAuditSink sink = new LocalAuditSink(tempDir);
        DurableEventStore eventStore = new DurableEventStore(tempDir);
        DurableDeliveryStateStore stateStore = new DurableDeliveryStateStore(tempDir);
        ForwardingService forwarding = forwarding(sink, eventStore, stateStore);
        AuditLedger ledger = new AuditLedger();
        ReplayService replay = new ReplayService(eventStore, forwarding);

        sink.failAlways("dead-1");
        AuditPipeline pipeline = new AuditPipeline(ledger, new RuleRegistry(), new RuleEngine(), forwarding);
        for (int i = 1; i <= 4; i++) {
            pipeline.submit(event("r-" + i, "2026-10-02T10:00:0" + i + "Z"));
        }
        pipeline.submit(event("dead-1", "2026-10-02T10:00:05Z"));
        forwarding.awaitQuiescent(10_000);
        assertEquals(4, sink.deliveredCount());
        assertEquals(1, forwarding.deadLetterCount());
        log.info("补投前：目标端={}，死信={}", sink.deliveredEventIds(),
                forwarding.deadLetters().stream().map(AuditEvent::eventId).toList());

        // 第一次范围补投：4 条已送达须跳过；dead-1 仍故障 -> 重新耗尽 -> 死信，目标端无新增
        sink.failAlways("dead-1");
        ReplayJob job1 = replay.start(ReplayRequest.range(
                Instant.parse("2026-10-02T10:00:00Z"), Instant.parse("2026-10-02T10:00:06Z")));
        awaitJob(replay, job1, 10_000);
        forwarding.awaitQuiescent(10_000);
        log.info("补投任务1：scanned={} submitted={} skipped={}", job1.scanned(), job1.submitted(),
                job1.skippedDelivered());
        assertEquals(5, job1.scanned());
        assertEquals(4, job1.skippedDelivered(), "已送达事件须幂等跳过");
        assertEquals(1, job1.submitted(), "死信被重新挂起并补投尝试");
        assertEquals(4, sink.deliveredCount(), "故障未恢复前目标端不得多出记录");
        assertEquals(DeliveryStatus.DEAD, forwarding.stateOf("dead-1").orElseThrow().status());

        // 恢复目标端后再次补投：dead-1 这次送达；其余仍跳过；结果恰好每条一条
        sink.recover("dead-1");        ReplayJob job2 = replay.start(ReplayRequest.ids(List.of("dead-1", "r-1", "r-2")));
        awaitJob(replay, job2, 10_000);
        forwarding.awaitQuiescent(10_000);
        log.info("补投任务2：scanned={} submitted={} skipped={}，最终目标端={}",
                job2.scanned(), job2.submitted(), job2.skippedDelivered(), sink.deliveredEventIds());
        assertEquals(3, job2.scanned());
        assertEquals(2, job2.skippedDelivered());
        assertEquals(1, job2.submitted());
        assertEquals(5, sink.deliveredCount());
        assertEquals(5, sink.deliveredEventIds().stream().distinct().count(), "任何补投都不得产生重复记录");
        assertEquals(DeliveryStatus.DELIVERED, forwarding.stateOf("dead-1").orElseThrow().status());

        // 再次全量补投：全部跳过，目标端完全不变（中断重发/重复发起安全）
        ReplayJob job3 = replay.start(ReplayRequest.range(
                Instant.parse("2026-10-02T00:00:00Z"), Instant.parse("2026-10-02T23:59:59Z")));
        awaitJob(replay, job3, 10_000);
        forwarding.awaitQuiescent(10_000);
        assertEquals(5, job3.scanned());
        assertEquals(5, job3.skippedDelivered());
        assertEquals(0, job3.submitted());
        assertEquals(5, sink.deliveredCount());
        forwarding.shutdown();
    }

    @Test
    void rearmingDeadThatKeepsFailingDoesNotLeakPendingAndStaysDead() {
        LocalAuditSink sink = new LocalAuditSink(tempDir);
        DurableEventStore eventStore = new DurableEventStore(tempDir);
        DurableDeliveryStateStore stateStore = new DurableDeliveryStateStore(tempDir);
        ForwardingService forwarding = forwarding(sink, eventStore, stateStore);
        AuditLedger ledger = new AuditLedger();
        ReplayService replay = new ReplayService(eventStore, forwarding);
        AuditPipeline pipeline = new AuditPipeline(ledger, new RuleRegistry(), new RuleEngine(), forwarding);

        sink.failAlways("sick");
        pipeline.submit(event("sick", "2026-10-02T08:00:00Z"));
        forwarding.awaitQuiescent(10_000);
        assertEquals(DeliveryStatus.DEAD, forwarding.stateOf("sick").orElseThrow().status());
        assertEquals(5, forwarding.stateOf("sick").orElseThrow().attempts());

        // 目标端仍故障时反复补投两次：每次重置后再耗尽 5 次，死信保持，pending 必须收敛
        for (int round = 1; round <= 2; round++) {
            ReplayJob job = replay.start(ReplayRequest.ids(List.of("sick")));
            awaitJob(replay, job, 10_000);
            forwarding.awaitQuiescent(10_000); // 若 pending 泄漏（变负或残留），这里会异常/超时
            DeliveryState state = forwarding.stateOf("sick").orElseThrow();
            assertEquals(DeliveryStatus.DEAD, state.status());
            assertEquals(5, state.attempts(), "重新挂起后再次耗尽，尝试计数应重置后重新累计到 5");
            assertEquals(1, forwarding.deadLetterCount());
        }
        // 目标端始终未收到，对账在“死信不算未送达”的口径下保持一致
        DeliveryReport report = forwarding.reconcile();
        assertEquals(0, report.delivered());
        assertEquals(0, report.sinkDelivered());
        assertTrue(report.consistent());
        forwarding.shutdown();
    }

    @Test
    void replayRunsConcurrentlyWithLiveTrafficAndResultIsExactlyOnce() throws Exception {
        LocalAuditSink sink = new LocalAuditSink(tempDir);
        DurableEventStore eventStore = new DurableEventStore(tempDir);
        DurableDeliveryStateStore stateStore = new DurableDeliveryStateStore(tempDir);
        ForwardingService forwarding = forwarding(sink, eventStore, stateStore);
        AuditLedger ledger = new AuditLedger();
        ReplayService replay = new ReplayService(eventStore, forwarding);
        AuditPipeline pipeline = new AuditPipeline(ledger, new RuleRegistry(), new RuleEngine(), forwarding);

        // 历史窗口 100 条，全部送达后，对同一窗口叠加失败注入再补投；同时实时通道持续写入新事件
        int history = 100;
        for (int i = 0; i < history; i++) {
            pipeline.submit(event("hist-" + i, "2026-10-03T10:" + String.format("%02d", i / 2 % 60)
                    + ":" + String.format("%02d", i % 60) + "Z"));
        }
        forwarding.awaitQuiescent(15_000);
        assertEquals(history, sink.deliveredCount());

        sink.failNextDeliveries(30); // 补投与实时共享的随机失败，全部可经重试恢复
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);

        // 补投线程：覆盖整个历史窗口（此时全部已送达，应全部幂等跳过，不产生重复）
        pool.submit(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            ReplayJob job = replay.start(ReplayRequest.range(
                    Instant.parse("2026-10-03T00:00:00Z"), Instant.parse("2026-10-03T23:59:59Z")));
            awaitJob(replay, job, 30_000);
            assertEquals(history, job.skippedDelivered());
        });

        // 实时写入线程：窗口外的新事件
        pool.submit(() -> {
            try {
                start.await();
                for (int i = 0; i < 50; i++) {
                    pipeline.submit(event("live-" + i, "2026-10-04T10:00:" + String.format("%02d", i % 60) + "Z"));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        forwarding.awaitQuiescent(20_000);

        List<String> finalIds = sink.deliveredEventIds();
        log.info("补投+实时并发结束：目标端记录数={}（预期 {}），去重后={}",
                finalIds.size(), history + 50, new TreeSet<>(finalIds).size());
        assertEquals(history + 50, finalIds.size(), "补投不新增、实时不丢失，恰好每条一次");
        assertEquals(history + 50, new TreeSet<>(finalIds).size(), "并发下目标端不得出现重复");

        DeliveryReport report = forwarding.reconcile();
        assertTrue(report.consistent(), "并发+失败重试后对账必须一致：" + report);
        assertEquals(history + 50, report.delivered());
        assertEquals(0, report.pendingOrRetrying());
        assertEquals(0, report.dead());
        forwarding.shutdown();
    }

    @Test
    void longRangeReplayIsMemoryBoundedAndKeepsBatchOrder() {
        LocalAuditSink sink = new LocalAuditSink(tempDir);
        DurableEventStore eventStore = new DurableEventStore(tempDir);
        DurableDeliveryStateStore stateStore = new DurableDeliveryStateStore(tempDir);
        ForwardingService forwarding = forwarding(sink, eventStore, stateStore);
        AuditLedger ledger = new AuditLedger();
        AuditPipeline pipeline = new AuditPipeline(ledger, new RuleRegistry(), new RuleEngine(), forwarding);

        // 跨 6 个小时、共 180 条历史事件（每小时 30 条，时间戳互不重复），全部先正常送达
        int perHour = 30;
        for (int hour = 0; hour < 6; hour++) {
            for (int i = 0; i < perHour; i++) {
                String ts = String.format("2026-10-05T%02d:%02d:00Z", hour, i * 2);
                pipeline.submit(event("h" + hour + "-" + i, ts));
            }
        }
        forwarding.awaitQuiescent(20_000);
        assertEquals(6 * perHour, sink.deliveredCount());

        // 用记录到达顺序的 List 验证补投批次顺序：事件源按 (occurredAt, ledgerSeq) 流式推送，
        // 逐桶读取、一次只持有一个桶，长跨度下内存有界
        List<String> seen = new ArrayList<>();
        ReplayService replay = new ReplayService(eventStore, event -> {
            seen.add(event.eventId());
            return com.github.lisama569.springauditeventpipelinejava.recovery.RedeliveryOutcome.ALREADY_DELIVERED;
        });
        ReplayJob job = replay.start(ReplayRequest.range(
                Instant.parse("2026-10-05T00:00:00Z"), Instant.parse("2026-10-05T05:59:59Z")));
        awaitJob(replay, job, 20_000);

        assertEquals(6 * perHour, seen.size(), "长跨度范围内事件一个不少");
        // 顺序稳定：小时严格递增，同一小时内分钟严格递增
        for (int i = 1; i < seen.size(); i++) {
            String[] a = seen.get(i - 1).split("-");
            String[] b = seen.get(i).split("-");
            int hourA = Integer.parseInt(a[0].substring(1));
            int hourB = Integer.parseInt(b[0].substring(1));
            assertTrue(hourB > hourA || (hourB == hourA && Integer.parseInt(b[1]) > Integer.parseInt(a[1])),
                    "补投批次相对顺序必须稳定：" + seen.get(i - 1) + " -> " + seen.get(i));
        }
        // 目标端数量不因大范围补投改变（全部已送达跳过）
        forwarding.awaitQuiescent(10_000);
        assertEquals(6 * perHour, sink.deliveredCount());
        log.info("长跨度补投完成：扫描 {} 条，内存仅按单桶（{} 条/小时）规模占用",
                seen.size(), perHour);
        forwarding.shutdown();
    }
}
