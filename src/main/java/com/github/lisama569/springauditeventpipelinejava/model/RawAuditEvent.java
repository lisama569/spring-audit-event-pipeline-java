package com.github.lisama569.springauditeventpipelinejava.model;

import java.util.Map;

/**
 * 来自某一入口的原始审计事件（尚未规范化）。
 * timestamp 以字符串承载，非法格式在规范化阶段被拒绝。
 */
public record RawAuditEvent(
        String source,
        String eventId,
        String timestamp,
        String actor,
        String resource,
        String action,
        Map<String, String> attributes) {
}
