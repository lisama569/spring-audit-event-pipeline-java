package com.github.lisama569.springauditeventpipelinejava.ingest;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.model.RawAuditEvent;
import com.github.lisama569.springauditeventpipelinejava.model.RejectionReason;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;

/**
 * 将原始事件规范化为统一模型。
 * 缺失或非法字段产生可区分的拒绝原因，绝不静默接受。
 */
public class AuditEventNormalizer {

    /** 归一化结果：成功携带待落账事件（序列号尚未分配），失败携带拒绝原因。 */
    public sealed interface Outcome {
        record Ok(AuditEvent event) implements Outcome {}
        record Rejected(RejectionReason reason) implements Outcome {}
    }

    public Outcome normalize(RawAuditEvent raw) {
        if (raw == null) {
            return new Outcome.Rejected(RejectionReason.MISSING_EVENT_ID);
        }
        if (isBlank(raw.eventId())) {
            return new Outcome.Rejected(RejectionReason.MISSING_EVENT_ID);
        }
        if (isBlank(raw.timestamp())) {
            return new Outcome.Rejected(RejectionReason.MISSING_TIMESTAMP);
        }
        final Instant timestamp;
        try {
            timestamp = Instant.parse(raw.timestamp().trim());
        } catch (DateTimeParseException e) {
            return new Outcome.Rejected(RejectionReason.INVALID_TIMESTAMP);
        }
        if (isBlank(raw.actor())) {
            return new Outcome.Rejected(RejectionReason.MISSING_ACTOR);
        }
        if (isBlank(raw.resource())) {
            return new Outcome.Rejected(RejectionReason.MISSING_RESOURCE);
        }
        if (isBlank(raw.action())) {
            return new Outcome.Rejected(RejectionReason.MISSING_ACTION);
        }

        String source = isBlank(raw.source()) ? "unknown" : raw.source().trim();
        Map<String, String> attributes = raw.attributes() == null ? Map.of() : Map.copyOf(raw.attributes());

        AuditEvent event = new AuditEvent(
                raw.eventId().trim(),
                source,
                timestamp,
                raw.actor().trim(),
                raw.resource().trim(),
                raw.action().trim(),
                attributes,
                -1L,
                null);
        return new Outcome.Ok(event);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
