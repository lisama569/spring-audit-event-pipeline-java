package com.github.lisama569.springauditeventpipelinejava.ingest;

import com.github.lisama569.springauditeventpipelinejava.api.dto.AuditEventRequest;
import com.github.lisama569.springauditeventpipelinejava.model.RejectionReason;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Optional;

/**
 * 入口规范化校验：缺失或非法字段给出可区分拒绝原因，不静默接受。
 * 时间戳须为 ISO-8601（如 2026-09-25T10:15:30Z）。
 */
public class AuditEventValidator {

    public Optional<RejectionReason> validate(AuditEventRequest request) {
        if (request == null) {
            return Optional.of(RejectionReason.INVALID_PAYLOAD);
        }
        if (isBlank(request.eventId())) {
            return Optional.of(RejectionReason.MISSING_EVENT_ID);
        }
        if (isBlank(request.occurredAt())) {
            return Optional.of(RejectionReason.MISSING_TIMESTAMP);
        }
        try {
            Instant.parse(request.occurredAt());
        } catch (DateTimeParseException e) {
            return Optional.of(RejectionReason.INVALID_TIMESTAMP);
        }
        if (isBlank(request.actor())) {
            return Optional.of(RejectionReason.MISSING_ACTOR);
        }
        if (isBlank(request.resource())) {
            return Optional.of(RejectionReason.MISSING_RESOURCE);
        }
        if (isBlank(request.action())) {
            return Optional.of(RejectionReason.MISSING_ACTION);
        }
        return Optional.empty();
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
