package com.github.lisama569.springauditeventpipelinejava.recovery;

import com.github.lisama569.springauditeventpipelinejava.forward.LocalAuditSink;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditLedger;
import com.github.lisama569.springauditeventpipelinejava.model.DeliveryProgress;
import com.github.lisama569.springauditeventpipelinejava.model.DeliveryState;
import com.github.lisama569.springauditeventpipelinejava.model.DeliveryStatus;
import com.github.lisama569.springauditeventpipelinejava.model.ReconciliationReport;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 投递进度查询与三方对账（账本 / 投递状态表 / 目标端）。
 * 计数以持久化状态表为准，目标端内容以其幂等视图为准，两者与账本逐项交叉核对，
 * 在高并发写入叠加失败重试时仍可发现“重复计数/漏计/状态与实物不符”。
 */
public class Reconciler {

    private final AuditLedger ledger;
    private final DeliveryStateStore stateStore;
    private final LocalAuditSink sink;

    public Reconciler(AuditLedger ledger, DeliveryStateStore stateStore, LocalAuditSink sink) {
        this.ledger = ledger;
        this.stateStore = stateStore;
        this.sink = sink;
    }

    /** 单事件投递状态（含尝试次数与最近错误）；未落账/无状态事件返回空。 */
    public DeliveryState stateOf(String eventId) {
        return stateStore.find(eventId).orElse(null);
    }

    /** 进度汇总：各阶段数量与累计尝试次数。 */
    public DeliveryProgress progress() {
        long pending = 0;
        long delivered = 0;
        long dead = 0;
        long attempts = 0;
        List<DeliveryStateStore.Record> records = stateStore.all();
        for (DeliveryStateStore.Record record : records) {
            DeliveryState state = record.state();
            attempts += state.attempts();
            switch (state.status()) {
                case PENDING -> pending++;
                case DELIVERED -> delivered++;
                case DEAD -> dead++;
            }
        }
        return new DeliveryProgress(records.size(), pending, delivered, dead, attempts);
    }

    /**
     * 三方对账：
     * - ledger == state 键集合（每个落账事件恰有一条状态，状态表无幽灵键）；
     * - DELIVERED 状态集合 == 目标端集合（不重不漏）；
     * - PENDING/DEAD 不得出现在目标端（否则是状态与实物不符）。
     */
    public ReconciliationReport reconcile() {
        Set<String> ledgerIds = new HashSet<>();
        ledger.snapshotOrdered().forEach(e -> ledgerIds.add(e.eventId()));

        Set<String> sinkIds = new HashSet<>(sink.deliveredEventIds());

        Set<String> stateIds = new HashSet<>();
        Set<String> stateDelivered = new HashSet<>();
        Set<String> stateDead = new HashSet<>();
        for (DeliveryStateStore.Record record : stateStore.all()) {
            stateIds.add(record.eventId());
            switch (record.state().status()) {
                case DELIVERED -> stateDelivered.add(record.eventId());
                case DEAD -> stateDead.add(record.eventId());
                default -> {
                }
            }
        }

        List<String> missingInSink = new ArrayList<>(stateDelivered);
        missingInSink.removeAll(sinkIds);
        missingInSink.sort(String::compareTo);

        List<String> extraInSink = new ArrayList<>(sinkIds);
        extraInSink.removeAll(ledgerIds);
        extraInSink.sort(String::compareTo);

        // 状态非 DELIVERED 却已在目标端，或状态 DELIVERED 但目标端缺失
        List<String> stateMismatch = new ArrayList<>();
        for (String id : sinkIds) {
            DeliveryState state = stateStore.find(id).orElse(null);
            if (state == null) {
                stateMismatch.add(id); // 目标端有、状态表无
            } else if (state.status() != DeliveryStatus.DELIVERED) {
                stateMismatch.add(id);
            }
        }
        stateMismatch.addAll(missingInSink);
        stateMismatch.sort(String::compareTo);

        List<String> deadLetters = new ArrayList<>(stateDead);
        deadLetters.sort(String::compareTo);

        boolean consistent = stateIds.equals(ledgerIds)
                && stateDelivered.equals(sinkIds)
                && missingInSink.isEmpty()
                && extraInSink.isEmpty()
                && stateMismatch.isEmpty();

        return new ReconciliationReport(consistent, ledgerIds.size(), sinkIds.size(), stateIds.size(),
                missingInSink, extraInSink, stateMismatch, deadLetters);
    }
}
