package com.github.lisama569.springauditeventpipelinejava.api.dto;

import java.util.Map;

/** 各入口统一的原始事件请求（字段允许缺失，由校验器判定）。 */
public record AuditEventRequest(
        String eventId,
        String occurredAt,
        String actor,
        String resource,
        String action,
        Map<String, String> attributes) {
}
