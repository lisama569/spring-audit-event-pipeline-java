package com.github.lisama569.springauditeventpipelinejava.model;

import java.time.Instant;
import java.util.Map;

/**
 * 落账后的统一审计事件模型。
 * ledgerSeq 为落账顺序号（到达顺序），occurredAt 为业务发生时间。
 */
public record AuditEvent(
        String eventId,
        Instant occurredAt,
        String actor,
        String resource,
        String action,
        Map<String, String> attributes,
        long ledgerSeq,
        Instant ingestedAt) {
}
