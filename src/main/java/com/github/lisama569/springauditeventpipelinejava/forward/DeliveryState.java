package com.github.lisama569.springauditeventpipelinejava.forward;

/**
 * 单个事件的投递进度记录（对账依据）。
 * 对账单上的状态分组：DELIVERED=已送达；PENDING/RETRYING=等待/重试中；DEAD=彻底失败。
 *
 * @param eventId      事件唯一标识
 * @param status       当前状态
 * @param attempts     实际已发生的投递尝试次数（不含幂等跳过）
 * @param updatedAtMs  最近一次状态变更时间（毫秒）
 */
public record DeliveryState(String eventId, DeliveryStatus status, int attempts, long updatedAtMs) {

    public DeliveryState withStatus(DeliveryStatus newStatus, int newAttempts) {
        return new DeliveryState(eventId, newStatus, newAttempts, System.currentTimeMillis());
    }
}
