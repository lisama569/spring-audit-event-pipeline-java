package com.github.lisama569.springauditeventpipelinejava.compliance;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * 针对给定规则集快照判定事件。
 * 结论中固化 ruleSetVersion 与每条命中规则的版本/依据，保证可解释、可复现。
 */
public class ComplianceEvaluator {

    public ComplianceVerdict evaluate(AuditEvent event, RuleRegistry.Snapshot snapshot) {
        if (snapshot == null) {
            throw new RuleParseException("判定失败：规则集快照为 null");
        }
        List<Violation> violations = new ArrayList<>();
        for (ComplianceRule rule : snapshot.rules()) {
            if (matches(rule.actorPattern(), event.actor())
                    && matches(rule.resourcePattern(), event.resource())
                    && matches(rule.actionPattern(), event.action())) {
                String explanation = String.format(
                        "命中规则 %s(v%d, 规则集v%d)[%s]: actor='%s' 匹配 '%s', resource='%s' 匹配 '%s', action='%s' 匹配 '%s'%s",
                        rule.ruleId(),
                        rule.version(),
                        snapshot.version(),
                        rule.severity(),
                        event.actor(), rule.actorPattern(),
                        event.resource(), rule.resourcePattern(),
                        event.action(), rule.actionPattern(),
                        rule.description().isBlank() ? "" : " — " + rule.description());
                violations.add(new Violation(rule.ruleId(), rule.version(), rule.severity(), explanation));
            }
        }
        return ComplianceVerdict.of(event.eventId(), snapshot.version(), violations);
    }

    /** 仅支持 *（任意多字符）与 ?（单字符）的 glob 匹配。 */
    static boolean matches(String pattern, String value) {
        String p = pattern == null || pattern.isBlank() ? "*" : pattern;
        String v = value == null ? "" : value;
        // 转成正则
        StringBuilder regex = new StringBuilder("^");
        for (int i = 0; i < p.length(); i++) {
            char c = p.charAt(i);
            switch (c) {
                case '*' -> regex.append(".*");
                case '?' -> regex.append('.');
                default -> regex.append(escape(c));
            }
        }
        regex.append('$');
        return v.matches(regex.toString());
    }

    private static String escape(char c) {
        if (".$^{}[]|()+\\".indexOf(c) >= 0) {
            return "\\" + c;
        }
        return String.valueOf(c);
    }
}
