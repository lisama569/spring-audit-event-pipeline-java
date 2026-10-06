package com.github.lisama569.springauditeventpipelinejava.recovery;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 按范围/按 id 补投：从 {@link EventSource} 流式读取历史事件，逐条重新交给投递网关。
 *
 * <ul>
 *   <li><b>内存有界</b>：逐条读取、逐条同步提交，不整批载入；长跨度补投内存占用只与
 *       事件源单桶规模相关，不随事件总量线性膨胀。</li>
 *   <li><b>幂等不重</b>：已送达事件直接跳过；在途事件复用实时通道的在途尝试；
 *       死信事件重置计数后重新挂起。重复发起、中途失败重试、中断后重发，
 *       目标端都不会出现第二条。</li>
 *   <li><b>稳定顺序</b>：每个任务单线程按 (occurredAt, ledgerSeq) 顺序提交，
 *       同一批次的补投在目标端的相对次序稳定。</li>
 *   <li><b>并发安全</b>：补投与实时投递共用 {@link RedeliveryGateway} 的每事件互斥入口，
 *       各走各的工作线程、只在事件锁上汇合，不会互相卡死。</li>
 * </ul>
 */
public class ReplayService {

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);

    private final EventSource source;
    private final RedeliveryGateway gateway;
    private final ConcurrentHashMap<String, ReplayJob> jobs = new ConcurrentHashMap<>();
    private final AtomicLong jobSeq = new AtomicLong(0);

    public ReplayService(EventSource source, RedeliveryGateway gateway) {
        this.source = source;
        this.gateway = gateway;
    }

    /** 异步发起一次补投，立即返回任务句柄。 */
    public ReplayJob start(ReplayRequest request) {
        String jobId = "replay-" + jobSeq.incrementAndGet();
        ReplayJob job = new ReplayJob(jobId, request);
        jobs.put(jobId, job);
        Thread worker = new Thread(() -> run(job), "audit-replay-" + jobId);
        worker.setDaemon(true);
        worker.start();
        return job;
    }

    private void run(ReplayJob job) {
        try {
            ReplayRequest req = job.request();
            if (req.isRange()) {
                log.info("补投任务 {} 开始，业务时间范围 [{}, {}]", job.jobId(), req.from(), req.to());
                // 范围扫描在事件源侧逐桶流式进行，消费侧逐条同步处理，全程内存有界
                source.forEachInRange(req.from(), req.to(), event -> process(job, event));
            } else {
                List<String> ids = Optional.ofNullable(req.eventIds()).orElse(List.of());
                log.info("补投任务 {} 开始，指定事件 {} 个", job.jobId(), ids.size());
                source.forEachIds(ids, event -> process(job, event));
            }
            job.complete();
            log.info("补投任务 {} 完成：扫描={}，重新提交={}，已送达跳过={}",
                    job.jobId(), job.scanned(), job.submitted(), job.skippedDelivered());
        } catch (RuntimeException e) {
            job.fail(e.getMessage());
            log.error("补投任务 " + job.jobId() + " 失败，可重新发起（已送达的不会重复）", e);
        }
    }

    private void process(ReplayJob job, AuditEvent event) {
        job.incrementScanned();
        // 网关内部：已送达跳过、在途复用、死信重置；同一事件与实时路径互斥
        RedeliveryOutcome outcome = gateway.redeliver(event);
        switch (outcome) {
            case ALREADY_DELIVERED -> job.incrementSkippedDelivered();
            // 本次送达 / 仍在等待重试，都属于“重新提交”的范畴
            case RESUBMITTED_DELIVERED, RESUBMITTED_PENDING -> job.incrementSubmitted();
        }
        log.info("补投任务 {} 处理事件 {}，结果={}", job.jobId(), event.eventId(), outcome);
    }

    public Optional<ReplayJob> job(String jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    public List<ReplayJob> jobs() {
        return List.copyOf(jobs.values());
    }
}
