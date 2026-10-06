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
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 高并发 + 失败重试叠加下的进度与对账：
 * - 状态计数（已送达/等待重试/死信）与目标端实际内容完全一致，不重不漏；
 * - 尝试次数精确等于对目标端的实际投递调用次数（含成功、失败、重试）；
 * - 并发补投参与时对账仍一致。
 */
class ReconciliationTest {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationTest.class);

    @TempDir
    Path tempDir;

    /** 记录每次实际投递调用的 Sink：attempts(id) 必须与进度里的 attempts 一致。 */
    private static class CountingSink extends LocalAuditSink {
        private final Map<String, Integer> attempts = new java.util.concurrent.ConcurrentHashMap<>();

        CountingSink(Path dataDir) {
            super(dataDir);
        }

        @Override
        public synchronized boolean deliver(com.github.lisama569.springauditeventpipelinejava.model.AuditEvent event) {
            attempts.merge(event.eventId(), 1, Integer::sum);
            return super.deliver(event);
        }

        int attemptsOf(String id) {
            return attempts.getOrDefault(id, 0);
        }
    }

    private static AuditEventRequest event(String id, int minute) {
        return new AuditEventRequest(id, String.format("2026-10-06T10:%02d:00Z", minute),
                "alice", "db", "READ", Map.of());
    }

    @Test
    void countersStayExactUnderConcurrentWritesFailuresAndReplay() throws Exception {
        int threads = 8;
        int perThread = 40; // 共 320 个唯一事件
        int total = threads * perThread;
        CountingSink sink = new CountingSink(tempDir);
        sink.failNextDeliveries(60); // 60 次瞬时失败，分散在不同事件上，全部应经重试恢复
        sink.failAlways("dead-x");
        // 三个固定事件各失败前两次、第三次成功：用于精确断言尝试次数
        sink.failFirstTimes("ev-0-0", 2);
        sink.failFirstTimes("ev-7-39", 2);
        sink.failFirstTimes("ev-3-17", 2);

        DurableEventStore eventStore = new DurableEventStore(tempDir);
        DurableDeliveryStateStore stateStore = new DurableDeliveryStateStore(tempDir);
        ForwardingService forwarding = new ForwardingService(sink, stateStore, eventStore);
        forwarding.start();
        AuditLedger ledger = new AuditLedger();
        AuditPipeline pipeline = new AuditPipeline(ledger, new RuleRegistry(), new RuleEngine(), forwarding);
        ReplayService replay = new ReplayService(eventStore, forwarding);

        ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> accepted = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < threads; t++) {
            final int threadNo = t;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int i = 0; i < perThread; i++) {
                    String id = "ev-" + threadNo + "-" + i;
                    pipeline.submit(event(id, i));
                    accepted.add(id);
                }
            });
        }
        // 持续运行的补投线程：不断对全窗口重发（事件陆续落账），验证并发补投也不重复
        pool.submit(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            for (int round = 0; round < 5; round++) {
                ReplayJob job = replay.start(ReplayRequest.range(
                        Instant.parse("2026-10-06T00:00:00Z"), Instant.parse("2026-10-06T23:59:59Z")));
                long deadline = System.currentTimeMillis() + 30_000;
                while (job.status() == com.github.lisama569.springauditeventpipelinejava.recovery.ReplayStatus.RUNNING
                        && System.currentTimeMillis() < deadline) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        });
        // 永久失败事件也参与并发
        pool.submit(() -> {
            try {
                start.await();
                Thread.sleep(5);
                pipeline.submit(new AuditEventRequest("dead-x", "2026-10-06T11:00:00Z",
                        "bob", "db", "READ", Map.of()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
        forwarding.awaitQuiescent(30_000);

        log.info("输入事件：{} 个唯一业务事件 + 1 个永久失败事件 dead-x，样例={}",
                total, List.of("ev-0-0", "ev-3-17", "ev-7-39"));

        DeliveryReport report = forwarding.reconcile();
        log.info("对账结果：delivered={} dead={} pending={} sink={} consistent={}",
                report.delivered(), report.dead(), report.pendingOrRetrying(),
                report.sinkDelivered(), report.consistent());

        // 除死信外全部送达；目标端记录与已送达集合一致（不重不漏）
        assertEquals(total, report.delivered(), "320 个唯一事件全部送达");
        assertEquals(1, report.dead(), "仅 dead-x 进入死信");
        assertEquals(0, report.pendingOrRetrying(), "收敛后无等待/重试事件");
        assertEquals(total, report.sinkDelivered());
        assertTrue(report.consistent(), "进度与目标端实际内容必须对得上: " + report);

        // 每个事件的尝试次数必须精确等于对目标端的实际调用次数（不多不少）
        for (DeliveryState state : report.states()) {
            int actualCalls = sink.attemptsOf(state.eventId());
            assertEquals(actualCalls, state.attempts(),
                    "事件 " + state.eventId() + " 尝试次数与实际投递调用不一致");
            if (state.status() == DeliveryStatus.DELIVERED) {
                assertTrue(state.attempts() >= 1);
            } else {
                assertEquals(DeliveryStatus.DEAD, state.status());
                assertEquals(5, state.attempts(), "死信必须恰好耗尽 5 次尝试");
            }
        }
        // 目标端无重复
        assertEquals(new TreeSet<>(sink.deliveredEventIds()).size(), sink.deliveredEventIds().size());
        // 固定事件的尝试次数必须精确为 3（2 次失败 + 1 次成功），且最终已送达
        for (String id : List.of("ev-0-0", "ev-7-39", "ev-3-17")) {
            DeliveryState state = forwarding.stateOf(id).orElseThrow();
            assertEquals(DeliveryStatus.DELIVERED, state.status());
            assertEquals(3, state.attempts(), "事件 " + id + " 应失败 2 次后成功，共尝试 3 次");
            assertEquals(3, sink.attemptsOf(id), "进度尝试次数与目标端实际调用次数一致");
        }
        log.info("对账明细：死信={}，固定事件尝试次数 ev-0-0={} ev-3-17={} ev-7-39={}",
                forwarding.deadLetters().stream().findFirst().map(e -> e.eventId()).orElse(""),
                forwarding.stateOf("ev-0-0").map(DeliveryState::attempts).orElse(-1),
                forwarding.stateOf("ev-3-17").map(DeliveryState::attempts).orElse(-1),
                forwarding.stateOf("ev-7-39").map(DeliveryState::attempts).orElse(-1));

        // 全部故障恢复后对死信补投，最终全部送达，对账仍一致
        sink.recover("dead-x");
        ReplayJob last = replay.start(ReplayRequest.ids(List.of("dead-x")));
        long deadline = System.currentTimeMillis() + 15_000;
        while (last.status() == com.github.lisama569.springauditeventpipelinejava.recovery.ReplayStatus.RUNNING
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        forwarding.awaitQuiescent(15_000);
        DeliveryReport finalReport = forwarding.reconcile();
        assertEquals(0, finalReport.dead());
        assertEquals(total + 1, finalReport.delivered());
        assertEquals(total + 1, finalReport.sinkDelivered());
        assertTrue(finalReport.consistent());
        forwarding.shutdown();
    }
}
