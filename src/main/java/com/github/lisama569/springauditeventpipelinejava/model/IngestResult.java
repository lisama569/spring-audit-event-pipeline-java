package com.github.lisama569.springauditeventpipelinejava.model;

/**
 * 采集结果：被接受、重复提交（幂等命中）或带可区分原因的拒绝。
 */
public record IngestResult(Status status, RejectionReason rejectionReason, AuditEvent event) {

    public enum Status {
        ACCEPTED,
        DUPLICATE,
        REJECTED
    }

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
