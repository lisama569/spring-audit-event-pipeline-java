package com.github.lisama569.springauditeventpipelinejava.recovery;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 一次补投任务的进度（可查询）。
 * scanned：范围内扫描到的事件数；
 * submitted：重新交给投递通道并实际触发尝试的事件数；
 * skippedDelivered：因当前已送达而幂等跳过的事件数。
 * 恒等式：scanned = submitted + skippedDelivered（终态时）。
 */
public class ReplayJob {

    private final String jobId;
    private final ReplayRequest request;
    private final AtomicLong scanned = new AtomicLong();
    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong skippedDelivered = new AtomicLong();
    private volatile ReplayStatus status = ReplayStatus.RUNNING;
    private volatile String error;

    public ReplayJob(String jobId, ReplayRequest request) {
        this.jobId = jobId;
        this.request = request;
    }

    public String jobId() {
        return jobId;
    }

    public ReplayRequest request() {
        return request;
    }

    public ReplayStatus status() {
        return status;
    }

    public long scanned() {
        return scanned.get();
    }

    public long submitted() {
        return submitted.get();
    }

    public long skippedDelivered() {
        return skippedDelivered.get();
    }

    public String error() {
        return error;
    }

    void incrementScanned() {
        scanned.incrementAndGet();
    }

    void incrementSubmitted() {
        submitted.incrementAndGet();
    }

    void incrementSkippedDelivered() {
        skippedDelivered.incrementAndGet();
    }

    void complete() {
        this.status = ReplayStatus.COMPLETED;
    }

    void fail(String error) {
        this.status = ReplayStatus.FAILED;
        this.error = error;
    }
}
