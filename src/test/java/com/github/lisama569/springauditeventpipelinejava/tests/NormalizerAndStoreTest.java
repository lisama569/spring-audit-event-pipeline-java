package com.github.lisama569.springauditeventpipelinejava.tests;

import com.github.lisama569.springauditeventpipelinejava.ingest.AuditEventNormalizer;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditEventStore;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.model.RawAuditEvent;
import com.github.lisama569.springauditeventpipelinejava.model.RejectionReason;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NormalizerAndStoreTest {

    private static final Logger log = LoggerFactory.getLogger(NormalizerAndStoreTest.class);

    private final AuditEventNormalizer normalizer = new AuditEventNormalizer();

    private static RawAuditEvent valid(String id) {
        return new RawAuditEvent("http", id, "2026-09-25T10:00:00Z",
                "alice", "db/customers", "read", Map.of());
    }

    private RejectionReason rejectReason(RawAuditEvent raw) {
        log.info("输入事件: {}", raw);
        AuditEventNormalizer.Outcome outcome = normalizer.normalize(raw);
        AuditEventNormalizer.Outcome.Rejected rejected = assertInstanceOf(
                AuditEventNormalizer.Outcome.Rejected.class, outcome);
        log.info("拒绝原因: {}", rejected.reason());
        return rejected.reason();
    }

    @Test
    void rejectionReasonsAreDistinguishable() {
        assertEquals(RejectionReason.MISSING_EVENT_ID,
                rejectReason(new RawAuditEvent("http", null, "2026-09-25T10:00:00Z", "a", "r", "x", null)));
        assertEquals(RejectionReason.MISSING_EVENT_ID,
                rejectReason(new RawAuditEvent("http", "  ", "2026-09-25T10:00:00Z", "a", "r", "x", null)));
        assertEquals(RejectionReason.MISSING_TIMESTAMP,
                rejectReason(new RawAuditEvent("http", "e1", null, "a", "r", "x", null)));
        assertEquals(RejectionReason.INVALID_TIMESTAMP,
                rejectReason(new RawAuditEvent("http", "e2", "not-a-time", "a", "r", "x", null)));
        assertEquals(RejectionReason.MISSING_ACTOR,
                rejectReason(new RawAuditEvent("http", "e3", "2026-09-25T10:00:00Z", "", "r", "x", null)));
        assertEquals(RejectionReason.MISSING_RESOURCE,
                rejectReason(new RawAuditEvent("http", "e4", "2026-09-25T10:00:00Z", "a", null, "x", null)));
        assertEquals(RejectionReason.MISSING_ACTION,
                rejectReason(new RawAuditEvent("http", "e5", "2026-09-25T10:00:00Z", "a", "r", " ", null)));
    }

    @Test
    void validEventNormalizesWithDefaults() {
        AuditEventNormalizer.Outcome outcome = normalizer.normalize(
                new RawAuditEvent(null, "e1", "2026-09-25T10:00:00Z", "alice", "db", "read", null));
        AuditEvent event = ((AuditEventNormalizer.Outcome.Ok) outcome).event();
        assertEquals("unknown", event.source());
        assertEquals(Map.of(), event.attributes());
        assertEquals("2026-09-25T10:00:00Z", event.timestamp().toString());
    }

    @Test
    void duplicateSubmissionYieldsSingleRecord() {
        AuditEventStore store = new AuditEventStore();
        AuditEvent first = ((AuditEventNormalizer.Outcome.Ok) normalizer.normalize(valid("dup-1"))).event();
        AuditEvent second = ((AuditEventNormalizer.Outcome.Ok) normalizer.normalize(valid("dup-1"))).event();

        AuditEventStore.StoredResult r1 = store.putIfAbsent(first);
        AuditEventStore.StoredResult r2 = store.putIfAbsent(second);

        assertEquals(1, store.findAll().size());
        assertTrue(r1.inserted());
        assertFalse(r2.inserted());
        assertEquals(r1.event().sequence(), r2.event().sequence());
        assertTrue(store.contains("dup-1"));
        log.info("重复提交幂等: eventId=dup-1, sequence={}", r1.event().sequence());
    }

    @Test
    void outOfOrderArrivalKeepsLedgerSequenceAndOriginalTimestamps() {
        AuditEventStore store = new AuditEventStore();
        // 乱序到达：时间戳晚的事件先落账
        AuditEvent late = ((AuditEventNormalizer.Outcome.Ok) normalizer.normalize(
                new RawAuditEvent("http", "late", "2026-09-25T10:05:00Z", "a", "r", "x", null))).event();
        AuditEvent early = ((AuditEventNormalizer.Outcome.Ok) normalizer.normalize(
                new RawAuditEvent("http", "early", "2026-09-25T10:00:00Z", "a", "r", "x", null))).event();

        AuditEvent s1 = store.putIfAbsent(late).event();
        AuditEvent s2 = store.putIfAbsent(early).event();

        assertEquals(0, s1.sequence());
        assertEquals(1, s2.sequence());
        // 固有时间戳语义不被落账顺序破坏
        assertTrue(s1.timestamp().isAfter(s2.timestamp()));
        assertEquals("late", store.findAll().get(0).eventId());
        log.info("乱序落账: late(seq={}, ts={}), early(seq={}, ts={})",
                s1.sequence(), s1.timestamp(), s2.sequence(), s2.timestamp());
    }
}
