package com.github.lisama569.springauditeventpipelinejava.forward;

import java.util.List;
import java.util.Optional;

/**
 * 投递进度存储：记录每个事件当前状态与尝试次数，是重启恢复与对账的依据。
 * 所有状态变更均要求持久化（实现为 Durable 时崩溃/重启后不丢进度）。
 */
public interface DeliveryStateStore {

    /** 首次受理：若该事件尚无进度记录则登记为 PENDING，否则保持不变（恢复/补投/重复受理均幂等）。 */
    void registerIfAbsent(String eventId);

    Optional<DeliveryState> find(String eventId);

    void save(DeliveryState state);

    /** 全部进度快照（对账用，允许弱一致读取）。 */
    List<DeliveryState> all();

    /** 非终态（未送达且未进死信）的事件 id，用于重启后续投。 */
    List<String> pendingEventIds();
}
