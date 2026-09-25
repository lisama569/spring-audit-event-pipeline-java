package com.github.lisama569.springauditeventpipelinejava.tests;

import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceVerdict;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardService;
import com.github.lisama569.springauditeventpipelinejava.forward.LocalFileForwardTarget;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ForwardServiceTest {

    private static final Logger log = LoggerFactory.getLogger(ForwardServiceTest.class);

    private AuditEvent event(long seq, String id) {
        return new AuditEvent(id, "http", Instant.parse("2026-09-25T10:00:00Z"),
                "alice", "db", "read", Map.of(), seq, Instant.now());
    }

    private ComplianceVerdict verdict(String id, long ruleSetVersion) {
        return ComplianceVerdict.of(id, ruleSetVersion, List.of());
    }

    private List<String> lines(Path file) throws Exception {
        return Files.exists(file) ? Files.readAllLines(file).stream().filter(s -> !s.isBlank()).toList()
                : List.of();
    }

    @Test
    void failedDeliveryRetriedAndNeverDuplicated(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("forward.jsonl");
        LocalFileForwardTarget target = new LocalFileForwardTarget(file);
        AtomicInteger attempts = new AtomicInteger();
        target.failWhen(e -> e.eventId().equals("flaky") && attempts.incrementAndGet() <= 2);

        try (ForwardService service = new ForwardService(target, 5, 10)) {
            service.enqueue(event(0, "ok-1"), verdict("ok-1", 1));
            service.enqueue(event(1, "flaky"), verdict("flaky", 1));
            service.enqueue(event(2, "ok-2"), verdict("ok-2", 1));
            assertTrue(service.awaitQuiescence(10, TimeUnit.SECONDS));
        }

        List<String> lines = lines(file);
        log.info("转发文件内容: {}", lines);
        assertEquals(3, lines.size());
        long flakyLines = lines.stream().filter(l -> l.contains("\"eventId\":\"flaky\"")).count();
        assertEquals(1, flakyLines, "重复投递不得产生重复数据");
        assertEquals(3, attempts.get(), "flaky 应经历 3 次尝试（前两次失败）");
        // 顺序语义：序列号顺序
        int idxOk1 = 0, idxFlaky = 1, idxOk2 = 2;
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).contains("ok-1")) idxOk1 = i;
            if (lines.get(i).contains("flaky")) idxFlaky = i;
            if (lines.get(i).contains("ok-2")) idxOk2 = i;
        }
        assertTrue(idxOk1 < idxFlaky && idxFlaky < idxOk2,
                "转发须保持序列号顺序: " + idxOk1 + "," + idxFlaky + "," + idxOk2);
    }

    @Test
    void poisonEventEntersDeadLetterThenRedelivered(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("forward.jsonl");
        LocalFileForwardTarget target = new LocalFileForwardTarget(file);
        target.failWhen(e -> e.eventId().equals("poison"));

        ForwardService service = new ForwardService(target, 3, 5);
        service.enqueue(event(0, "poison"), verdict("poison", 1));
        service.enqueue(event(1, "healthy"), verdict("healthy", 1));
        assertTrue(service.awaitQuiescence(10, TimeUnit.SECONDS));

        assertEquals(1, service.deadLetterCount(), "超过重试上限应进入死信");
        // 单条事件失败不得阻塞其它事件
        assertEquals(1, lines(file).size());
        assertTrue(lines(file).get(0).contains("healthy"));
        log.warn("poison 进入死信，healthy 仍投递成功");

        // 排除故障后重投死信，目标端幂等保证安全
        target.failWhen(e -> false);
        int redelivered = service.retryDeadLetters();
        assertEquals(1, redelivered);
        assertTrue(service.awaitQuiescence(10, TimeUnit.SECONDS));
        assertEquals(0, service.deadLetterCount());
        assertEquals(2, lines(file).size());
        assertTrue(lines(file).stream().anyMatch(l -> l.contains("poison")));
        assertFalse(lines(file).stream().allMatch(l -> l.contains("healthy")));
        service.close();
    }

    @Test
    void targetDedupSurvivesRestartAndDuplicateEnqueue(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("forward.jsonl");
        LocalFileForwardTarget target1 = new LocalFileForwardTarget(file);
        ForwardService service1 = new ForwardService(target1, 3, 5);
        AuditEvent e = event(0, "only-once");
        service1.enqueue(e, verdict("only-once", 1));
        service1.enqueue(e, verdict("only-once", 1)); // 重复入队
        assertTrue(service1.awaitQuiescence(10, TimeUnit.SECONDS));
        service1.close();

        // 模拟服务重启：新目标实例从文件恢复已投递 eventId
        LocalFileForwardTarget target2 = new LocalFileForwardTarget(file);
        target2.deliver(e, verdict("only-once", 1));
        assertEquals(1, lines(file).size(), "重启后重复投递也不得产生重复数据");
        log.info("去重验证通过，文件行数={}", lines(file).size());
    }
}
