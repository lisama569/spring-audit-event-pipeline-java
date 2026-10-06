package com.github.lisama569.springauditeventpipelinejava.recovery;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;

/**
 * 补投/恢复使用的投递网关，由转发服务实现。
 * 必须幂等且互斥：已送达事件直接确认；同一事件在实时投递、恢复、补投并发时
 * 也不会被实际投递两次。
 */
public interface RedeliveryGateway {

    /**
     * 把一条事件交给投递通道（同步完成一次互斥的投递判定）。
     */
    RedeliveryOutcome redeliver(AuditEvent event);
}
