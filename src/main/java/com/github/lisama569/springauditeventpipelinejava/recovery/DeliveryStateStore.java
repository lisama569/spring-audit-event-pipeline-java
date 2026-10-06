package com.github.lisama569.springauditeventpipelinejava.recovery;

import com.github.lisama569.springauditeventpipelinejava.model.DeliveryState;
import com.github.lisama569.springauditeventpipelinejava.model.DeliveryStatus;

import java.util.List;
import java.util.Optional;

/**
 * 投递状态持久化存储：记录每个事件的阶段与尝试次数，重启后据此恢复投递。
 * 状态变更是幂等更新（同 key 覆盖），落盘即 fsync。
 */
public interface DeliveryStateStore {

    /** 不存在则初始化为 PENDING（attempts=0）；已存在则不变。 */
    void initIfAbsent(String eventId);

    void save(String eventId, DeliveryState state);

    Optional<DeliveryState> find(String eventId);

    List<String> eventIdsByStatus(DeliveryStatus status);

    /** 返回全部状态记录（用于对账与进度汇总）。 */
    List<Record> all();

    /** 状态记录键值对。 */
    record Record(String eventId, DeliveryState state) {
    }
}
