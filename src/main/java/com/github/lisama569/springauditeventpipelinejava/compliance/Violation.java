package com.github.lisama569.springauditeventpipelinejava.compliance;

/**
 * 一次命中：记录被命中的规则版本及其可解释依据。
 */
public record Violation(
        String ruleId,
        int ruleVersion,
        String severity,
        String explanation) {
}
