package com.github.lisama569.springauditeventpipelinejava.model;

/**
 * 单事件投递阶段（持久化到投递状态存储）。
 * PENDING：等待/重试中；DELIVERED：已送达；DEAD：耗尽重试，等待人工处理。
 */
public enum DeliveryStatus {
    PENDING, DELIVERED, DEAD
}
