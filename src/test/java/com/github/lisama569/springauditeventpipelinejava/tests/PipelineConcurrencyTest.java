package com.github.lisama569.springauditeventpipelinejava.tests;

import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceEvaluator;
import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceRule;
import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceVerdict;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.compliance.VerdictRepository;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardService;
import com.github.lisama569.springauditeventpipelinejava.forward.LocalFileForwardTarget;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditEventNormalizer;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditEventStore;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditIngestService;
import com.github.lisama569.springauditeventpipelinejava.model.IngestResult;
import com.github.lisama569.springauditeventpipelinejava.model.RawAuditEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(PipelineConcurrencyTest.class);

    private static final int THREADS = 12;
    private static final int UNIQUE_EVENTS = 300;

    @Test
    void concurrentWritesWithDuplicatesRuleEvolutionAndForward(@TempDir Path dir) throws Exception {
        AuditEventStore store = new AuditEventStore();
        RuleRegistry registry = new RuleRegistry();
        VerdictRepository verdicts = new VerdictRepository();
        ComplianceEvaluator evaluator = new ComplianceEvaluator();
        LocalFileForwardTarget target = new LocalFileForwardTarget(dir.resolve("pipeline.jsonl"));
        ForwardService forward = new ForwardService(target, 5, 5);
        AuditIngestService service = new AuditIngestService(
                new AuditEventNormalizer(), store, registry, evaluator, verdicts, forward);

        // v1 规则：禁止 delete
        registry.replaceRules(List.of(
                new ComplianceRule("R-DEL", 1, "禁止删除", "*", "*", "delete", "HIGH")));

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();
        Map<String, AtomicInteger> rejected = new HashMap<>();

        for (int t = 0; t < THREADS; t++) {
            final int thread = t;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int i = 0; i < UNIQUE_EVENTS; i++) {
                    String id = "evt-" + (i % (UNIQUE_EVENTS / 2)); // 一半事件被多个线程重复提交
                    String action = i % 5 == 0 ? "delete" : "read";
                    RawAuditEvent raw = new RawAuditEvent("thread-" + thread, id,
                            "2026-09-25T10:" + String.format("%02d", i % 60) + ":00Z",
                            "actor" + (i % 7), "db/table" + (i % 3), action, Map.of());
                    IngestResult result = service.ingest(raw);
                    switch (result.status()) {
                        case ACCEPTED -> accepted.incrementAndGet();
                        case DUPLICATE -> duplicates.incrementAndGet();
                        case REJECTED -> {
                            synchronized (rejected) {
                                rejected.computeIfAbsent(result.rejectionReason().name(), k -> new AtomicInteger()).incrementAndGet();
                            }
                        }
                    }
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
        assertTrue(forward.awaitQuiescence(30, TimeUnit.SECONDS));

        int expectedUnique = UNIQUE_EVENTS / 2;
        log.info("并发采集结果: accepted={}, duplicates={}, rejected={}",
                accepted.get(), duplicates.get(), rejected);
        assertEquals(expectedUnique, accepted.get(), "每个 eventId 只产生一条独立记录");
        assertEquals(THREADS * UNIQUE_EVENTS, accepted.get() + duplicates.get());
        assertTrue(rejected.isEmpty(), "本批无非法事件: " + rejected);

        // 序列号连续无空洞，账本顺序可追溯
        List<Long> sequences = store.findAll().stream().map(e -> e.sequence()).sorted().toList();
        for (int i = 0; i < expectedUnique; i++) {
            assertEquals(i, sequences.get(i), "序列号必须连续: i=" + i);
        }

        // 违规结论与合规结论可区分
        long violations = verdicts.findAll().stream().filter(v -> !v.compliant()).count();
        long compliant = verdicts.findAll().stream().filter(ComplianceVerdict::compliant).count();
        long expectedViolations = store.findAll().stream().filter(e -> e.action().equals("delete")).count();
        log.info("判定统计: 违规={}, 合规={}, 规则集版本={}", violations, compliant, 1L);
        assertEquals(expectedViolations, violations);
        assertEquals(expectedUnique - expectedViolations, compliant);
        verdicts.findAll().stream().filter(v -> !v.compliant()).findFirst().ifPresent(v ->
                log.info("样例违规结论: eventId={}, 依据={}", v.eventId(),
                        v.violations().get(0).explanation()));

        // 转发文件不重不漏且按序列号有序
        List<String> lines = Files.readAllLines(dir.resolve("pipeline.jsonl")).stream()
                .filter(s -> !s.isBlank()).toList();
        assertEquals(expectedUnique, lines.size());
        long lastSeq = -1;
        List<Long> actualSeq = new ArrayList<>();
        for (String line : lines) {
            long seq = Long.parseLong(line.replaceAll(".*\"sequence\":(\\d+).*", "$1"));
            actualSeq.add(seq);
            assertTrue(seq > lastSeq, "转发文件须按序列号递增, 实际顺序=" + actualSeq);
            lastSeq = seq;
        }

        // 规则演进到 v2：新增禁止 export；历史结论保持 v1 且可复现
        List<ComplianceVerdict> beforeEvolution = new ArrayList<>(verdicts.findAll());
        registry.replaceRules(List.of(
                new ComplianceRule("R-DEL", 1, "禁止删除", "*", "*", "delete", "HIGH"),
                new ComplianceRule("R-EXPORT", 1, "禁止导出", "*", "*", "export", "MEDIUM")));
        RawAuditEvent export = new RawAuditEvent("http", "evt-export",
                "2026-09-25T11:00:00Z", "bob", "db/x", "export", Map.of());
        IngestResult exportResult = service.ingest(export);
        assertEquals(IngestResult.Status.ACCEPTED, exportResult.status());
        assertTrue(forward.awaitQuiescence(10, TimeUnit.SECONDS));

        ComplianceVerdict exportVerdict = verdicts.find("evt-export").orElseThrow();
        assertEquals(2, exportVerdict.ruleSetVersion());
        assertEquals("R-EXPORT", exportVerdict.violations().get(0).ruleId());
        log.info("规则演进后新事件按 v2 判定: {}", exportVerdict.violations().get(0).explanation());

        // 历史结论未被改写
        for (ComplianceVerdict old : beforeEvolution) {
            assertEquals(old, verdicts.find(old.eventId()).orElseThrow(),
                    "eventId=" + old.eventId() + " 的历史结论须保持稳定");
            assertEquals(1, old.ruleSetVersion());
        }
        // 历史判定可按旧版本快照重新复现
        ComplianceVerdict replayed = evaluator.evaluate(
                store.find(beforeEvolution.get(0).eventId()).orElseThrow(),
                registry.snapshotAt(1));
        assertEquals(beforeEvolution.get(0).violations().size(), replayed.violations().size());
        log.info("历史结论复现通过: eventId={} 在规则集 v1 下命中数={}",
                replayed.eventId(), replayed.violations().size());

        forward.close();
    }
}
