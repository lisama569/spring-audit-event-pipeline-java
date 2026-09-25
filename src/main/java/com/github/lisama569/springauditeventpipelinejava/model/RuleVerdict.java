package com.github.lisama569.springauditeventpipelinejava.model;

import java.time.Instant;

/**
 * 一条事件对一条规则的判定结论。
 * 记录规则 id 与版本快照，保证规则演进后历史结论可复现。
 */
public record RuleVerdict(
        String eventId,
        String ruleId,
        int ruleVersion,
        VerdictOutcome outcome,
        String explanation,
        Instant decidedAt) {
}
