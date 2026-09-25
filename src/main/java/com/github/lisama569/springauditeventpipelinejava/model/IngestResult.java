package com.github.lisama569.springauditeventpipelinejava.model;

/** 采集结果：接受 / 重复去重 / 带原因拒绝。 */
public record IngestResult(Status status, RejectionReason reason, AuditEvent event) {

    public enum Status { ACCEPTED, DUPLICATE, REJECTED }

    public static IngestResult accepted(AuditEvent event) {
        return new IngestResult(Status.ACCEPTED, null, event);
    }

    public static IngestResult duplicate(AuditEvent existing) {
        return new IngestResult(Status.DUPLICATE, null, existing);
    }

    public static IngestResult rejected(RejectionReason reason) {
        return new IngestResult(Status.REJECTED, reason, null);
    }
}
