package com.github.lisama569.springauditeventpipelinejava.forward;

import java.util.List;

/**
 * 投递对账报告：进度统计与本地目标端实际内容交叉核对的结果。
 *
 * @param delivered        进度中已送达事件数
 * @param pendingOrRetrying 等待/重试中的事件数
 * @param dead             彻底失败（死信）事件数
 * @param sinkDelivered    目标端实际记录数
 * @param consistent       进度集合与目标端集合是否完全一致（不重不漏）
 * @param onlyInProgress   进度记为已送达但目标端缺失的事件 id（有则说明不一致）
 * @param onlyInSink       目标端存在但进度未记为已送达的事件 id（有则说明不一致）
 * @param states           每个事件的明细（状态 + 尝试次数）
 */
public record DeliveryReport(int delivered,
                             int pendingOrRetrying,
                             int dead,
                             int sinkDelivered,
                             boolean consistent,
                             List<String> onlyInProgress,
                             List<String> onlyInSink,
                             List<DeliveryState> states) {
}
