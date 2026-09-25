package com.github.lisama569.springauditeventpipelinejava.compliance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 版本化规则注册表。
 * - 同 id 同版本但定义不同 -> RuleConflictException（显式失败，不静默覆盖）；
 * - 同 id 更高版本注册后，新事件按新规则判定；
 * - 历史版本保留，供历史结论复现。
 */
public class RuleRegistry {

    /** key = id + ":v" + version */
    private final Map<String, ComplianceRule> rulesByKey = new HashMap<>();

    public synchronized void register(ComplianceRule rule) {
        String key = key(rule.id(), rule.version());
        ComplianceRule existing = rulesByKey.get(key);
        if (existing != null && !existing.equals(rule)) {
            throw new RuleConflictException(
                    "规则冲突: " + key + " 已存在定义 [" + describe(existing)
                            + "]，与新定义 [" + describe(rule) + "] 不一致");
        }
        rulesByKey.put(key, rule);
    }

    /** 当前生效规则集（每个 id 取最高版本），按 id 排序保证判定顺序可复现。 */
    public synchronized List<ComplianceRule> activeRules() {
        Map<String, ComplianceRule> latest = new HashMap<>();
        for (ComplianceRule rule : rulesByKey.values()) {
            ComplianceRule cur = latest.get(rule.id());
            if (cur == null || rule.version() > cur.version()) {
                latest.put(rule.id(), rule);
            }
        }
        List<ComplianceRule> result = new ArrayList<>(latest.values());
        result.sort((a, b) -> a.id().compareTo(b.id()));
        return List.copyOf(result);
    }

    /** 取指定 id+version 的历史规则（用于复现历史结论）。 */
    public synchronized ComplianceRule ruleOf(String id, int version) {
        return rulesByKey.get(key(id, version));
    }

    public synchronized void clear() {
        rulesByKey.clear();
    }

    private static String key(String id, int version) {
        return id + ":v" + version;
    }

    private static String describe(ComplianceRule r) {
        return r.field() + " " + r.operator() + " " + r.value();
    }
}
