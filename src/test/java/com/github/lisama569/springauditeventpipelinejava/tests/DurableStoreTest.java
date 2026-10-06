package com.github.lisama569.springauditeventpipelinejava.tests;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.recovery.DurableEventStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 持久化事件存储：跨“重启”（重新打开目录）后的加载、去重追加与范围查询。 */
class DurableStoreTest {

    private static final Logger log = LoggerFactory.getLogger(DurableStoreTest.class);

    @TempDir
    Path tempDir;

    private AuditEvent event(String id, String ts, long seq) {
        return new AuditEvent(id, Instant.parse(ts), "alice", "db", "READ", Map.of(), seq, Instant.now());
    }

    @Test
    void persistsAcrossReopenWithDedupAndOrderedRangeScan() {
        DurableEventStore store = new DurableEventStore(tempDir);
        // 乱序追加：迟到事件落到较早的桶，读取时桶内排序消化
        store.append(event("a", "2026-10-01T10:00:00Z", 1));
        store.append(event("b", "2026-10-01T11:30:00Z", 2));
        store.append(event("late", "2026-10-01T10:45:00Z", 3));
        store.append(event("a", "2026-10-01T10:00:00Z", 1)); // 重复追加忽略
        assertEquals(3, count(store, Instant.parse("2026-10-01T00:00:00Z"),
                Instant.parse("2026-10-02T00:00:00Z")));

        // “重启”：重新打开同一目录，索引与事件体都要恢复
        DurableEventStore reopened = new DurableEventStore(tempDir);
        assertTrue(reopened.find("late").isPresent());
        assertEquals(3, reopened.find("late").orElseThrow().ledgerSeq());

        List<String> ids = new ArrayList<>();
        reopened.forEachInRange(Instant.parse("2026-10-01T10:00:00Z"),
                Instant.parse("2026-10-01T11:00:00Z"), e -> ids.add(e.eventId()));
        log.info("重开后范围[10:00,11:00]查询结果={}", ids);
        assertEquals(List.of("a", "late"), ids, "范围闭区间，按 occurredAt 升序，迟到事件不丢");

        List<String> all = new ArrayList<>();
        reopened.forEach(e -> all.add(e.eventId()));
        assertEquals(List.of("a", "late", "b"), all, "全量扫描跨桶有序，重复追加不产生重复");
        assertEquals(3, all.size());
    }

    private static int count(DurableEventStore store, Instant from, Instant to) {
        int[] n = {0};
        store.forEachInRange(from, to, e -> n[0]++);
        return n[0];
    }
}
