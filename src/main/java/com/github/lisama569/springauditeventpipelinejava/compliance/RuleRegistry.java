package com.github.lisama569.springauditeventpipelinejava.compliance;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 规则注册表。
 * <ul>
 *   <li>规则集以整体快照方式更新：任何一条规则不可解析或同 set 内 ruleId 冲突，
 *       整个更新失败并抛出 {@link RuleParseException}，旧规则集继续生效；</li>
 *   <li>每次成功更新产生新的快照版本（ruleSetVersion 单调递增），
 *       历史快照被保留，使历史判定结论可按版本重新复现；</li>
 *   <li>新事件始终按当前最新版本判定。</li>
 * </ul>
 */
public class RuleRegistry {

    /** 允许出现在匹配模式中的字符：字母数字、常见资源分隔符与 *? 通配符。 */
    private static final Pattern ALLOWED_PATTERN = Pattern.compile("[A-Za-z0-9_*?./:@\\-]*");

    private final List<Snapshot> history = new ArrayList<>();

    public RuleRegistry() {
        history.add(new Snapshot(0, List.of()));
    }

    public record Snapshot(long version, List<ComplianceRule> rules) {
        public Snapshot {
            rules = List.copyOf(rules);
        }
    }

    public synchronized Snapshot current() {
        return history.get(history.size() - 1);
    }

    /**
     * 用新规则集替换当前规则集。校验失败抛出异常且不改变当前状态。
     */
    public synchronized Snapshot replaceRules(List<ComplianceRule> rules) {
        if (rules == null) {
            throw new RuleParseException("规则集不能为 null");
        }
        List<ComplianceRule> validated = new ArrayList<>(rules.size());
        Set<String> seen = new HashSet<>();
        for (ComplianceRule rule : rules) {
            validated.add(validate(rule, seen));
        }
        Snapshot snapshot = new Snapshot(current().version() + 1, validated);
        history.add(snapshot);
        return snapshot;
    }

    /** 取回历史快照用于结论复现；不存在则明确失败。 */
    public synchronized Snapshot snapshotAt(long version) {
        if (version < 0 || version >= history.size()) {
            throw new RuleParseException("规则集版本不存在: " + version);
        }
        return history.get((int) version);
    }

    private ComplianceRule validate(ComplianceRule rule, Set<String> seen) {
        if (rule == null) {
            throw new RuleParseException("规则不能为 null");
        }
        if (rule.ruleId() == null || rule.ruleId().isBlank()) {
            throw new RuleParseException("规则缺少 ruleId");
        }
        if (!seen.add(rule.ruleId())) {
            throw new RuleParseException("规则集存在冲突：ruleId 重复 -> " + rule.ruleId());
        }
        if (rule.version() < 1) {
            throw new RuleParseException("规则 " + rule.ruleId() + " 的 version 必须 >= 1");
        }
        if (rule.severity() == null || rule.severity().isBlank()) {
            throw new RuleParseException("规则 " + rule.ruleId() + " 缺少 severity");
        }
        requireSafePattern(rule.ruleId(), "actorPattern", rule.actorPattern());
        requireSafePattern(rule.ruleId(), "resourcePattern", rule.resourcePattern());
        requireSafePattern(rule.ruleId(), "actionPattern", rule.actionPattern());
        return new ComplianceRule(
                rule.ruleId(),
                rule.version(),
                rule.description() == null ? "" : rule.description(),
                normalize(rule.actorPattern()),
                normalize(rule.resourcePattern()),
                normalize(rule.actionPattern()),
                rule.severity().trim());
    }

    private void requireSafePattern(String ruleId, String field, String pattern) {
        String p = normalize(pattern);
        if (!ALLOWED_PATTERN.matcher(p).matches()) {
            throw new RuleParseException("规则 " + ruleId + " 的 " + field + " 不可解析: " + pattern);
        }
    }

    private static String normalize(String pattern) {
        return pattern == null || pattern.isBlank() ? "*" : pattern.trim();
    }
}
