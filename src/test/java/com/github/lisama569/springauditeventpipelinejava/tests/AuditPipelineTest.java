package com.github.lisama569.springauditeventpipelinejava.tests;

import com.github.lisama569.springauditeventpipelinejava.api.dto.AuditEventRequest;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleEngine;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardingService;
import com.github.lisama569.springauditeventpipelinejava.forward.LocalAuditSink;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditLedger;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.model.IngestResult;
import com.github.lisama569.springauditeventpipelinejava.model.RejectionReason;
import com.github.lisama569.springauditeventpipelinejava.pipeline.AuditPipeline;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuditPipelineTest {

    private static final Logger log = LoggerFactory.getLogger(AuditPipelineTest.class);

    private AuditLedger ledger;
    private RuleRegistry registry;
    private LocalAuditSink sink;
    private ForwardingService forwarding;
    private AuditPipeline pipeline;

    @BeforeEach
    void setUp() {
        ledger = new AuditLedger();
        registry = new RuleRegistry();
        sink = new LocalAuditSink();
        forwarding = new ForwardingService(sink);
        forwarding.start();
        pipeline = new AuditPipeline(ledger, registry, new RuleEngine(), forwarding);
    }

    @AfterEach
    void tearDown() {
        forwarding.shutdown();
    }

    private static AuditEventRequest event(String id, String occurredAt, String actor,
                                           String resource, String action) {
        return new AuditEventRequest(id, occurredAt, actor, resource, action, Map.of());
    }

    @Test
    void rejectsInvalidEventsWithDistinguishableReasons() {
        List<Object[]> cases = List.of(
                new Object[]{event(null, "2026-09-25T10:00:00Z", "alice", "db", "READ"), RejectionReason.MISSING_EVENT_ID},
                new Object[]{event("e1", null, "alice", "db", "READ"), RejectionReason.MISSING_TIMESTAMP},
                new Object[]{event("e2", "not-a-time", "alice", "db", "READ"), RejectionReason.INVALID_TIMESTAMP},
                new Object[]{event("e3", "2026-09-25T10:00:00Z", " ", "db", "READ"), RejectionReason.MISSING_ACTOR},
                new Object[]{event("e4", "2026-09-25T10:00:00Z", "alice", null, "READ"), RejectionReason.MISSING_RESOURCE},
                new Object[]{event("e5", "2026-09-25T10:00:00Z", "alice", "db", ""), RejectionReason.MISSING_ACTION});

        List<RejectionReason> seen = new ArrayList<>();
        for (Object[] c : cases) {
            IngestResult result = pipeline.submit((AuditEventRequest) c[0]);
            log.info("输入事件={} -> 拒绝原因={}", c[0], result.reason());
            assertEquals(IngestResult.Status.REJECTED, result.status());
            assertEquals(c[1], result.reason());
            seen.add(result.reason());
        }
        assertEquals(cases.size(), seen.stream().distinct().count(), "每种非法输入须有可区分的拒绝原因");
        assertEquals(0, ledger.size(), "被拒绝的事件不得落账");
    }

    @Test
    void duplicateSubmissionDoesNotCreateNewRecord() {
        AuditEventRequest req = event("dup-1", "2026-09-25T10:00:00Z", "alice", "db", "READ");
        IngestResult first = pipeline.submit(req);
        IngestResult second = pipeline.submit(req);
        log.info("首次提交={}，重复提交={}", first.status(), second.status());

        assertEquals(IngestResult.Status.ACCEPTED, first.status());
        assertEquals(IngestResult.Status.DUPLICATE, second.status());
        assertEquals(1, ledger.size());
        assertEquals(first.event().ledgerSeq(), second.event().ledgerSeq(),
                "重复提交应返回已存在的同一条记录");
    }

    @Test
    void concurrentWritesWithDuplicatesAndOutOfOrderArrival() throws InterruptedException {
        int threads = 8;
        int eventsPerThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<IngestResult> results = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < threads; t++) {
            final int threadNo = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < eventsPerThread; i++) {
                        // 所有线程提交同一批 eventId，制造大量并发重复；
                        // occurredAt 与提交顺序无关，制造乱序到达
                        String id = "ev-" + i;
                        String ts = "2026-09-25T10:00:" + String.format("%02d", i % 60) + "Z";
                        IngestResult r = pipeline.submit(event(id, ts, "actor-" + threadNo, "res", "READ"));
                        results.add(r);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        long accepted = results.stream().filter(r -> r.status() == IngestResult.Status.ACCEPTED).count();
        long duplicates = results.stream().filter(r -> r.status() == IngestResult.Status.DUPLICATE).count();
        log.info("并发提交总数={}，接受={}，去重={}", results.size(), accepted, duplicates);

        assertEquals(eventsPerThread, accepted, "每个 eventId 只能落账一次");
        assertEquals(threads * eventsPerThread - eventsPerThread, duplicates);
        assertEquals(eventsPerThread, ledger.size());

        // 乱序到达不破坏业务顺序：快照按 occurredAt 升序
        List<AuditEvent> ordered = ledger.snapshotOrdered();
        for (int i = 1; i < ordered.size(); i++) {
            assertTrue(!ordered.get(i).occurredAt().isBefore(ordered.get(i - 1).occurredAt()),
                    "snapshotOrdered 必须按 occurredAt 升序");
        }
        // ledgerSeq 单调且唯一，可追溯到达顺序
        List<Long> seqs = ordered.stream().map(AuditEvent::ledgerSeq).sorted().toList();
        assertIterableEquals(seqs.stream().distinct().toList(), seqs, "ledgerSeq 不得重复");

        forwarding.awaitQuiescent(10_000);
        assertEquals(eventsPerThread, sink.deliveredCount(), "并发下转发不得丢失或重复");
    }
}
