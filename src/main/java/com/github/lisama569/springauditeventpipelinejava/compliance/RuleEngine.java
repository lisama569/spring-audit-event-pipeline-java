package com.github.lisama569.springauditeventpipelinejava.compliance;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.model.RuleVerdict;
import com.github.lisama569.springauditeventpipelinejava.model.VerdictOutcome;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 对事件逐条执行规则，产出带规则版本快照的判定结论。
 * 结论中固化 ruleId/ruleVersion/解释文本，规则演进后历史结论仍可复现。
 */
public class RuleEngine {

    public List<RuleVerdict> evaluate(AuditEvent event, List<ComplianceRule> rules) {
        List<RuleVerdict> verdicts = new ArrayList<>(rules.size());
        Instant now = Instant.now();
        for (ComplianceRule rule : rules) {
            boolean hit = rule.matches(event);
            verdicts.add(new RuleVerdict(
                    event.eventId(),
                    rule.id(),
                    rule.version(),
                    hit ? VerdictOutcome.VIOLATION : VerdictOutcome.PASS,
                    rule.explain(event),
                    now));
        }
        return List.copyOf(verdicts);
    }
}
