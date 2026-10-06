package com.github.lisama569.springauditeventpipelinejava.model;

import java.util.List;

/**
 * 对账报告：账本、投递状态表与目标端三方一致性结果。
 *
 * @param consistent          三方是否完全对平
 * @param ledgerCount         账本事件总数
 * @param sinkCount           目标端记录总数
 * @param stateCount          有投递状态记录的事件数
 * @param missingInSink       状态为 DELIVERED 但目标端缺失的 eventId
 * @param extraInSink         目标端存在但账本不存在的 eventId
 * @param stateMismatch       状态与目标端不一致（状态未送达但目标端已有，或反之）的 eventId
 * @param deadLetters         处于 DEAD 阶段的 eventId
 */
public record ReconciliationReport(boolean consistent, long ledgerCount, long sinkCount, long stateCount,
                                   List<String> missingInSink, List<String> extraInSink,
                                   List<String> stateMismatch, List<String> deadLetters) {
}
