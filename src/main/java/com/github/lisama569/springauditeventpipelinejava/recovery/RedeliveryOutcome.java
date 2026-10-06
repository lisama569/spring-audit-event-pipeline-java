package com.github.lisama569.springauditeventpipelinejava.recovery;

/**
 * 一次补投/恢复提交的结果分类。
 * ALREADY_DELIVERED：事件此前已送达，幂等跳过（目标端无新增）；
 * RESUBMITTED_DELIVERED：此前未送达，本次补投尝试后确认送达；
 * RESUBMITTED_PENDING：此前未送达，已重新挂起尝试但当前仍在等待/重试/死信。
 */
public enum RedeliveryOutcome {
    ALREADY_DELIVERED,
    RESUBMITTED_DELIVERED,
    RESUBMITTED_PENDING
}
