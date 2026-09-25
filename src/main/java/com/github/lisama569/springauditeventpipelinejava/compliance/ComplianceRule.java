package com.github.lisama569.springauditeventpipelinejava.compliance;

/**
 * 一条合规规则（不可变值对象）。
 * 匹配维度为 actor / resource / action 的 glob 模式（* 表示任意），任一维度非空则需全部匹配。
 * 命中即视为违规；severity 仅用于解释，不影响命中逻辑。
 */
public record ComplianceRule(
        String ruleId,
        int version,
        String description,
        String actorPattern,
        String resourcePattern,
        String actionPattern,
        String severity) {
}
