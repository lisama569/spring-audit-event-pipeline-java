package com.github.lisama569.springauditeventpipelinejava.compliance;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 历史判定结论账本：结论一旦形成即固化（含规则集版本），规则演进不改变历史结论。
 */
public class VerdictRepository {

    private final Map<String, ComplianceVerdict> byEventId = new LinkedHashMap<>();

    /** 幂等：同一事件只保存首次判定结论。返回当前保存的结论。 */
    public synchronized ComplianceVerdict saveIfAbsent(String eventId, ComplianceVerdict verdict) {
        ComplianceVerdict existing = byEventId.get(eventId);
        if (existing != null) {
            return existing;
        }
        byEventId.put(eventId, verdict);
        return verdict;
    }

    public synchronized Optional<ComplianceVerdict> find(String eventId) {
        return Optional.ofNullable(byEventId.get(eventId));
    }

    public synchronized List<ComplianceVerdict> findAll() {
        return List.copyOf(byEventId.values());
    }
}
