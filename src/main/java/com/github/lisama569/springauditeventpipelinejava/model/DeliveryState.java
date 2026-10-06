package com.github.lisama569.springauditeventpipelinejava.model;

/**
 * 单事件投递状态快照。
 *
 * @param status       当前阶段
 * @param attempts     已实际尝试投递次数
 * @param lastError    最近一次失败原因（可空）
 * @param updatedAtMs  状态最后更新时间（epoch millis）
 */
public record DeliveryState(DeliveryStatus status, int attempts, String lastError, long updatedAtMs) {
}
