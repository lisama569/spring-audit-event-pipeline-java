package com.github.lisama569.springauditeventpipelinejava.forward;

/**
 * 单个事件的投递生命周期状态。
 * PENDING：已受理尚未投递成功（含等待重试）；
 * DELIVERED：已确认送达本地目标（恰好生效一次）；
 * DEAD：耗尽重试，等待人工处理。
 */
public enum DeliveryStatus {
    PENDING,
    RETRYING,
    DELIVERED,
    DEAD
}
