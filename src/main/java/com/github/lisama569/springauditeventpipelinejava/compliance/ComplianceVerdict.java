package com.github.lisama569.springauditeventpipelinejava.compliance;

import java.util.List;

/**
 * 对单个事件的合规判定结论。
 * ruleSetVersion 固化了判定时使用的规则集版本，历史结论可据此重新复现。
 */
public record ComplianceVerdict(
        String eventId,
        long ruleSetVersion,
        List<Violation> violations,
        boolean compliant) {

    public static ComplianceVerdict of(String eventId, long ruleSetVersion, List<Violation> violations) {
        return new ComplianceVerdict(eventId, ruleSetVersion, List.copyOf(violations), violations.isEmpty());
    }
}
