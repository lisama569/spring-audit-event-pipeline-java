package com.github.lisama569.springauditeventpipelinejava.forward;

import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceVerdict;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;

/**
 * 本地转发目标。实现必须对同一 eventId 幂等。
 */
public interface ForwardTarget {

    /** 成功返回；失败抛异常，由转发服务重试。重复投递不得产生重复数据。 */
    void deliver(AuditEvent event, ComplianceVerdict verdict) throws Exception;
}
