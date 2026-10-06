package com.github.lisama569.springauditeventpipelinejava.model;

/**
 * 投递进度汇总：各阶段事件数量与已进行的投递尝试总数。
 */
public record DeliveryProgress(long total, long pending, long delivered, long dead, long totalAttempts) {
}
