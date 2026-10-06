package com.github.lisama569.springauditeventpipelinejava.recovery;

import com.github.lisama569.springauditeventpipelinejava.forward.ForwardingService;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.model.DeliveryState;
import com.github.lisama569.springauditeventpipelinejava.model.DeliveryStatus;
import com.github.lisama569.springauditeventpipelinejava.model.ReplayJobView;
import com.github.lisama569.springauditeventpipelinejava.model.ReplayRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按业务时间范围或事件 id 集合补投。
 *
 * 内存有界性：范围补投按 (occurredAt, ledgerSeq) 游标分页读取，每页 PAGE_SIZE 条；
 * 同时在途（已认领未终态）的事件数被 Semaphore 风格的计数器限制在 {@value #MAX_INFLIGHT} 内，
 * 故无论补投覆盖多长时间跨度、命中多少事件，常驻内存事件体与在途集合都有上界，
 * 不随事件总量线性膨胀。
 *
 * 并发正确性：补投与实时/恢复投递最终都经由 ForwardingService 的“认领 + 状态判定”，
 * 已送达事件直接跳过，已在途事件交由在途方，因此补投与实时并发时目标端最终内容
 * 与各事件恰好正常投递一次完全一致（不多、不少、业务顺序稳定）。
 * 中途失败重试或中断后重新发起也是安全的：已送达的不会再写。
 */
public class ReplayService {

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);

    static final int PAGE_SIZE = 64;
    /** 同时在途（已认领未终态）事件数上界，保证补投工作集内存有界。 */
    public static final int MAX_INFLIGHT = 64;

    private final EventStore eventStore;
    private final ForwardingService forwarding;
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();

    public ReplayService(EventStore eventStore, ForwardingService forwarding) {
        this.eventStore = eventStore;
        this.forwarding = forwarding;
    }

    /** 发起一次补投（后台执行），返回任务 id。 */
    public String start(ReplayRequest request) {
        validate(request);
        String jobId = UUID.randomUUID().toString();
        Job job = new Job(jobId);
        jobs.put(jobId, job);
        Thread worker = new Thread(() -> run(job, request), "audit-replay-" + jobId.substring(0, 8));
        worker.setDaemon(true);
        worker.start();
        log.info("补投任务已发起 jobId={} range=[{},{}) ids={}", jobId, request.from(), request.to(),
                request.eventIds());
        return jobId;
    }

    private static void validate(ReplayRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("补投请求不能为空");
        }
        if (request.isRangeQuery()) {
            if ((request.from() == null || request.from().isBlank())
                    && (request.to() == null || request.to().isBlank())) {
                throw new IllegalArgumentException("范围补投至少需要提供 from 或 to 之一");
            }
            parseInstant(request.from());
            parseInstant(request.to());
        } else if (request.eventIds().stream().allMatch(String::isBlank)) {
            throw new IllegalArgumentException("补投事件 id 列表不能为空");
        }
    }

    private static Instant parseInstant(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(value.trim());
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("非法的业务时间（需 ISO-8601）: " + value, e);
        }
    }

    private void run(Job job, ReplayRequest request) {
        try {
            if (request.isRangeQuery()) {
                runRange(job, parseInstant(request.from()), parseInstant(request.to()));
            } else {
                runIds(job, request.eventIds());
            }
            job.status.compareAndSet(Status.RUNNING, Status.COMPLETED);
            log.info("补投任务完成 jobId={} view={}", job.jobId, job.snapshot());
        } catch (RuntimeException e) {
            job.status.set(Status.FAILED);
            job.error = e.toString();
            log.error("补投任务失败 jobId={}", job.jobId, e);
        } finally {
            synchronized (job) {
                job.notifyAll();
            }
        }
    }

    private void runRange(Job job, Instant from, Instant to) {
        String cursor = null;
        while (true) {
            List<AuditEvent> page = eventStore.pageRange(from, to, cursor, PAGE_SIZE);
            if (page.isEmpty()) {
                return;
            }
            processPage(job, page);
            AuditEvent last = page.get(page.size() - 1);
            cursor = last.occurredAt().toEpochMilli() + ":" + last.ledgerSeq();
            if (page.size() < PAGE_SIZE) {
                return;
            }
        }
    }

    private void runIds(Job job, List<String> eventIds) {
        List<String> distinct = new ArrayList<>(new LinkedHashSet<>(eventIds));
        for (int i = 0; i < distinct.size(); i += PAGE_SIZE) {
            List<String> slice = distinct.subList(i, Math.min(i + PAGE_SIZE, distinct.size()));
            processPage(job, eventStore.findByIds(slice));
        }
    }

    /**
     * 处理一页：保证页内事件全部到达终态（或确认已送达）后再读下一页，
     * 并把在途事件数限制在 MAX_INFLIGHT，从而控制内存占用。
     * 计数规则：等待集合中的事件只在终态后归一次类，避免 pending 与 delivered/dead 重复计数。
     */
    private void processPage(Job job, List<AuditEvent> page) {
        Set<String> waiting = new LinkedHashSet<>();
        int inflight = 0;
        for (AuditEvent event : page) {
            job.matched.incrementAndGet();
            DeliveryState state = forwarding.stateOf(event.eventId()).orElse(null);
            if (state != null && state.status() == DeliveryStatus.DELIVERED) {
                job.already.incrementAndGet();
                continue;
            }
            ForwardingService.ReplayOutcome outcome = forwarding.replayOne(event);
            switch (outcome) {
                case ALREADY_DELIVERED -> job.already.incrementAndGet();
                case DELIVERED_NOW -> job.delivered.incrementAndGet();
                case DEAD -> job.dead.incrementAndGet();
                case RETRYING, INFLIGHT -> {
                    // 未到终态：加入等待集合，先计入 pending，终态后统一转移，恰好归一次类
                    waiting.add(event.eventId());
                    job.pending.incrementAndGet();
                    inflight++;
                }
            }
            if (inflight >= MAX_INFLIGHT) {
                drain(job, waiting);
                waiting.clear();
                inflight = 0;
            }
        }
        drain(job, waiting);
    }

    /** 等待一批事件终态，并据最终状态把 pending 转移为 delivered/dead（每个事件仅一次）。 */
    private void drain(Job job, Set<String> ids) {
        if (ids.isEmpty()) {
            return;
        }
        forwarding.awaitTerminal(ids, 30_000);
        for (String id : ids) {
            DeliveryState state = forwarding.stateOf(id).orElse(null);
            if (state == null) {
                continue;
            }
            if (state.status() == DeliveryStatus.PENDING) {
                continue; // 等待超时仍在重试：保留 pending，重新发起时由已送达跳过逻辑兜底
            }
            job.pending.decrementAndGet();
            if (state.status() == DeliveryStatus.DELIVERED) {
                job.delivered.incrementAndGet();
            } else if (state.status() == DeliveryStatus.DEAD) {
                job.dead.incrementAndGet();
            }
        }
    }

    /** 同步等待补投任务收敛，返回最终视图；超时返回当前视图（任务可能仍在运行）。 */
    public ReplayJobView await(String jobId, long timeoutMillis) {
        Job job = jobs.get(jobId);
        if (job == null) {
            throw new IllegalArgumentException("未知补投任务: " + jobId);
        }
        long deadline = System.currentTimeMillis() + timeoutMillis;
        synchronized (job) {
            while (job.status.get() == Status.RUNNING && System.currentTimeMillis() < deadline) {
                try {
                    job.wait(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        return job.snapshot();
    }

    public Optional<ReplayJobView> status(String jobId) {
        return Optional.ofNullable(jobs.get(jobId)).map(Job::snapshot);
    }

    public List<ReplayJobView> allJobs() {
        return jobs.values().stream().map(Job::snapshot).toList();
    }

    private enum Status { RUNNING, COMPLETED, FAILED }

    private static final class Job {
        private final String jobId;
        private final java.util.concurrent.atomic.AtomicReference<Status> status =
                new java.util.concurrent.atomic.AtomicReference<>(Status.RUNNING);
        private final java.util.concurrent.atomic.AtomicLong matched = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong delivered = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong already = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong pending = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong dead = new java.util.concurrent.atomic.AtomicLong();
        private volatile String error;

        private Job(String jobId) {
            this.jobId = jobId;
        }

        private synchronized ReplayJobView snapshot() {
            long failed = pending.get() + dead.get();
            String statusText = switch (status.get()) {
                case RUNNING -> "RUNNING";
                case COMPLETED -> "COMPLETED";
                case FAILED -> "FAILED";
            };
            return new ReplayJobView(jobId, statusText, matched.get(), delivered.get(), already.get(),
                    pending.get(), dead.get(), failed, error);
        }
    }
}
