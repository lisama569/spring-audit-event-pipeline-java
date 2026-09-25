package com.github.lisama569.springauditeventpipelinejava.model;

import java.time.Instant;
import java.util.Map;

/**
 * 落账后的规范化审计事件。
 * sequence 由账本在写入时单调分配，保证乱序到达下的顺序语义与可追溯性。
 */
public record AuditEvent(
        String eventId,
        String source,
        Instant timestamp,
        String actor,
        String resource,
        String action,
        Map<String, String> attributes,
        long sequence,
        Instant ingestedAt) {
}
