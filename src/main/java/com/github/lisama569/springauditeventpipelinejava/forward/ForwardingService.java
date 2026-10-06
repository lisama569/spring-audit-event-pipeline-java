package com.github.lisama569.springauditeventpipelinejava.forward;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.model.DeliveryState;
import com.github.lisama569.springauditeventpipelinejava.model.DeliveryStatus;
import com.github.lisama569.springauditeventpipelinejava.recovery.DeliveryStateStore;
import com.github.lisama569.springauditeventpipelinejava.recovery.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 可靠转发（可恢复 + 可与补投并发）。
 *
 * 投递判定模型（所有路径共用，保证恢复/实时/补投并发时不重不漏）：
 * 1. 每个事件的状态（PENDING/DELIVERED/DEAD + attempts）持久化在 {@link DeliveryStateStore}；
 * 2. 事件被实际投递前先“认领”（claim）：仅当状态为 PENDING 且未在途时认领成功，
 *    DELIVERED/DEAD/已在途一律跳过 —— 任意两个路径（恢复、实时、补投、重试）
 *    都不可能同时投递同一事件，同一次送达结果也不会在目标端出现两次；
 * 3. 每次尝试先调用目标端投递（目标端自身按 eventId 幂等），再据结果落状态：
 *    成功 -> DELIVERED；失败且达到上限 -> DEAD；否则继续延迟重试；
 * 4. 状态每次变更即 fsync：崩溃重启后 DELIVERED 不重投、PENDING 接着送、DEAD 留待人工。
 */
public class ForwardingService {

    private static final Logger log = LoggerFactory.getLogger(ForwardingService.class);

    /** 单个事件的最大投递尝试次数，达到后进入 DEAD（死信）等待人工处理。 */
    public static final int MAX_ATTEMPTS = 5;
    private static final long BASE_BACKOFF_MILLIS = 50;

    private final LocalAuditSink sink;
    private final DeliveryStateStore stateStore;
    private final EventStore eventStore;

    private final BlockingQueue<String> inbox = new LinkedBlockingQueue<>();
    private final DelayQueue<RetryTask> retryQueue = new DelayQueue<>();
    /** 已认领、尚未到达终态（DELIVERED/DEAD）的 eventId。 */
    private final Set<String> inflight = new LinkedHashSet<>();
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    /** 历史上同时在途事件数的峰值（用于验证补投内存有界）。 */
    private final AtomicInteger peakInflight = new AtomicInteger(0);

    private volatile Thread dispatcher;
    private volatile Thread retrier;
    private volatile boolean running = false;

    public ForwardingService(LocalAuditSink sink, DeliveryStateStore stateStore, EventStore eventStore) {
        this.sink = sink;
        this.stateStore = stateStore;
        this.eventStore = eventStore;
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        dispatcher = new Thread(this::dispatchLoop, "audit-forward-dispatcher");
        retrier = new Thread(this::retryLoop, "audit-forward-retrier");
        dispatcher.setDaemon(true);
        retrier.setDaemon(true);
        dispatcher.start();
        retrier.start();
        recoverPending();
    }

    // ---------- 实时投递 ----------

    /** 实时事件入队：初始化 PENDING 状态并进入主队列。 */
    public void enqueue(AuditEvent event) {
        stateStore.initIfAbsent(event.eventId());
        log.info("实时事件受理入队 eventId={} occurredAt={}", event.eventId(), event.occurredAt());
        inbox.offer(event.eventId());
        signal();
    }

    // ---------- 重启恢复 ----------

    /**
     * 启动恢复：把状态表中仍为 PENDING 的事件重新认领入队。
     * DELIVERED 不再投递；DEAD 保持死信等待人工，不自动重投。
     * 恢复事件与恢复期间新到的实时事件进入同一个队列与认领体系，互不阻塞、互不重复。
     */
    public synchronized void recoverPending() {
        int recovered = 0;
        for (String eventId : stateStore.eventIdsByStatus(DeliveryStatus.PENDING)) {
            if (claim(eventId)) {
                inbox.offer(eventId);
                recovered++;
                log.info("重启恢复：重新认领未送达事件 eventId={}", eventId);
            }
        }
        if (recovered > 0) {
            log.info("重启恢复完成，重新认领 {} 个未送达事件；已送达事件不重投", recovered);
        }
        signal();
    }

    // ---------- 补投原语 ----------

    /**
     * 补投单个事件（补投与实时/恢复共用同一投递原语）。
     * 与自动恢复不同，显式补投允许重新投递 DEAD 事件（人工修复目标端后的手动重投），
     * 会把状态重置为 PENDING；DELIVERED 永远跳过。
     *
     * @return 补投处理结果
     */
    public ReplayOutcome replayOne(AuditEvent event) {
        DeliveryState state = stateStore.find(event.eventId()).orElse(null);
        if (state != null && state.status() == DeliveryStatus.DELIVERED) {
            return ReplayOutcome.ALREADY_DELIVERED;
        }
        if (!claimForReplay(event.eventId())) {
            // 已在途（实时/恢复/另一次补投正在处理）：交给在途方，最终状态一致，不算新送达
            return ReplayOutcome.INFLIGHT;
        }
        attemptDelivery(event, 0);
        DeliveryState after = stateStore.find(event.eventId()).orElseThrow();
        return switch (after.status()) {
            case DELIVERED -> ReplayOutcome.DELIVERED_NOW;
            case DEAD -> ReplayOutcome.DEAD;
            case PENDING -> ReplayOutcome.RETRYING;
        };
    }

    /**
     * 补投认领：DELIVERED 拒绝；DEAD 允许复活（重置 PENDING）；
     * 无状态则初始化。仍受“单事件在途”互斥保护，补投与实时/恢复不会并发投递同一事件。
     */
    private synchronized boolean claimForReplay(String eventId) {
        DeliveryState state = stateStore.find(eventId).orElse(null);
        if (state != null && state.status() == DeliveryStatus.DELIVERED) {
            return false;
        }
        if (inflight.contains(eventId)) {
            return false;
        }
        if (state == null) {
            stateStore.initIfAbsent(eventId);
        } else if (state.status() == DeliveryStatus.DEAD) {
            // 人工发起的显式补投：清掉死信终态，重新进入投递流程（attempts 归零重新计数）
            stateStore.save(eventId, new DeliveryState(DeliveryStatus.PENDING, 0,
                    "显式补投重置死信事件", System.currentTimeMillis()));
            log.info("补投复活死信事件 eventId={}", eventId);
        }
        inflight.add(eventId);
        recordPeak();
        return true;
    }

    /** 结果分类：本次新送达 / 此前已送达 / 交由在途方 / 重试中 / 进入死信。 */
    public enum ReplayOutcome { DELIVERED_NOW, ALREADY_DELIVERED, INFLIGHT, RETRYING, DEAD }

    // ---------- 认领与投递 ----------

    /**
     * 认领事件：状态为 PENDING（或状态缺失）且当前未在途才成功。
     * DEAD 事件不在常规路径自动复活（补投仍可通过显式入队重投，见 replayOne 前置）。
     */
    private synchronized boolean claim(String eventId) {
        DeliveryState state = stateStore.find(eventId).orElse(null);
        if (state != null && state.status() == DeliveryStatus.DELIVERED) {
            return false;
        }
        if (state != null && state.status() == DeliveryStatus.DEAD) {
            return false;
        }
        if (inflight.contains(eventId)) {
            return false;
        }
        if (state == null) {
            stateStore.initIfAbsent(eventId);
        }
        inflight.add(eventId);
        recordPeak();
        return true;
    }

    private void recordPeak() {
        peakInflight.accumulateAndGet(inflight.size(), Math::max);
    }

    /** 历史峰值同时在途事件数（补投内存有界性的可观测指标）。 */
    public int peakInflight() {
        return peakInflight.get();
    }

    private void dispatchLoop() {
        while (running || !inbox.isEmpty()) {
            try {
                String eventId = inbox.poll(50, TimeUnit.MILLISECONDS);
                if (eventId == null) {
                    continue;
                }
                // 入队后可能已被补投等路径处理：终态则直接释放
                DeliveryState state = stateStore.find(eventId).orElse(null);
                if (state != null && state.status() != DeliveryStatus.PENDING) {
                    releaseIfTerminal(eventId);
                    continue;
                }
                if (!markActive(eventId)) {
                    continue; // 已在途（如恢复与实时重复入队）
                }
                AuditEvent event = eventStore.find(eventId).orElse(null);
                if (event == null) {
                    releaseInflight(eventId);
                    continue;
                }
                attemptDelivery(event, 0);
                activeWorkers.decrementAndGet();
                signal();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void retryLoop() {
        while (running || !retryQueue.isEmpty()) {
            try {
                RetryTask task = retryQueue.poll(50, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                AuditEvent event = eventStore.find(task.eventId).orElse(null);
                if (event == null) {
                    releaseInflight(task.eventId);
                    continue;
                }
                attemptDelivery(event, task.attempt);
                signal();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * 执行一次投递尝试并据结果推进状态机。attempt 为 0 表示首次（attempts 从状态中取续次），
     * 大于 0 表示重试路径带回的已尝试基数。
     */
    private void attemptDelivery(AuditEvent event, int retryAttemptBase) {
        String eventId = event.eventId();
        int before = stateStore.find(eventId).map(DeliveryState::attempts).orElse(0);
        int attempt = Math.max(before + 1, retryAttemptBase + 1);
        boolean ok;
        try {
            ok = sink.deliver(event);
        } catch (RuntimeException e) {
            ok = false;
            log.warn("投递抛出异常 eventId={} attempt={} error={}", eventId, attempt, e.toString());
        }
        if (ok) {
            stateStore.save(eventId, new DeliveryState(DeliveryStatus.DELIVERED, attempt, null,
                    System.currentTimeMillis()));
            releaseInflight(eventId);
            log.info("投递成功 eventId={} attempt={}", eventId, attempt);
            return;
        }
        if (attempt >= MAX_ATTEMPTS) {
            stateStore.save(eventId, new DeliveryState(DeliveryStatus.DEAD, attempt,
                    "目标端持续不可用，已达最大尝试次数 " + MAX_ATTEMPTS, System.currentTimeMillis()));
            releaseInflight(eventId);
            log.warn("投递进入死信 eventId={} attempts={}", eventId, attempt);
            return;
        }
        stateStore.save(eventId, new DeliveryState(DeliveryStatus.PENDING, attempt,
                "第 " + attempt + " 次投递失败，等待退避重试", System.currentTimeMillis()));
        long delay = BASE_BACKOFF_MILLIS * (1L << Math.min(attempt - 1, 6));
        retryQueue.offer(new RetryTask(eventId, attempt,
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay)));
        log.info("投递失败将重试 eventId={} attempt={} backoffMs={}", eventId, attempt, delay);
    }

    private synchronized boolean markActive(String eventId) {
        DeliveryState state = stateStore.find(eventId).orElse(null);
        if (state != null && state.status() != DeliveryStatus.PENDING) {
            inflight.remove(eventId);
            return false;
        }
        if (!inflight.contains(eventId)) {
            inflight.add(eventId);
        }
        activeWorkers.incrementAndGet();
        return true;
    }

    private synchronized void releaseInflight(String eventId) {
        inflight.remove(eventId);
    }

    private void releaseIfTerminal(String eventId) {
        DeliveryState state = stateStore.find(eventId).orElse(null);
        if (state != null && state.status() != DeliveryStatus.PENDING) {
            releaseInflight(eventId);
        }
    }

    // ---------- 等待收敛 ----------

    private final Object idleLock = new Object();

    private void signal() {
        synchronized (idleLock) {
            idleLock.notifyAll();
        }
    }

    /** 等待指定事件全部到达终态（DELIVERED/DEAD），供补投按页收敛使用。 */
    public void awaitTerminal(Set<String> eventIds, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        synchronized (idleLock) {
            while (System.currentTimeMillis() < deadline) {
                boolean allTerminal = eventIds.stream().allMatch(id ->
                        stateStore.find(id).map(s -> s.status() != DeliveryStatus.PENDING).orElse(true));
                if (allTerminal) {
                    return;
                }
                try {
                    idleLock.wait(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /** 阻塞等待所有已入队事件处理完成（成功或进入死信）。 */
    public void awaitQuiescent(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        synchronized (idleLock) {
            while (System.currentTimeMillis() < deadline) {
                if (inbox.isEmpty() && retryQueue.isEmpty()
                        && inflight.isEmpty() && activeWorkers.get() == 0) {
                    return;
                }
                try {
                    idleLock.wait(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        synchronized (this) {
            if (!inbox.isEmpty() || !retryQueue.isEmpty() || !inflight.isEmpty()
                    || activeWorkers.get() != 0) {
                throw new IllegalStateException("转发未在超时时间内收敛，inflight=" + inflight
                        + " retryQueue=" + retryQueue.size() + " active=" + activeWorkers.get());
            }
        }
    }

    // ---------- 状态查询（进度/对账/死信） ----------

    public Optional<DeliveryState> stateOf(String eventId) {
        return stateStore.find(eventId);
    }

    public DeliveryStateStore stateStore() {
        return stateStore;
    }

    public LocalAuditSink sink() {
        return sink;
    }

    public List<AuditEvent> deadLetters() {
        List<AuditEvent> dead = new ArrayList<>();
        for (String id : stateStore.eventIdsByStatus(DeliveryStatus.DEAD)) {
            eventStore.find(id).ifPresent(dead::add);
        }
        return List.copyOf(dead);
    }

    public int deadLetterCount() {
        return stateStore.eventIdsByStatus(DeliveryStatus.DEAD).size();
    }

    public synchronized void shutdown() {
        running = false;
        if (dispatcher != null) {
            dispatcher.interrupt();
        }
        if (retrier != null) {
            retrier.interrupt();
        }
    }

    private record RetryTask(String eventId, int attempt, long deadlineNanos) implements Delayed {
        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(deadlineNanos, ((RetryTask) other).deadlineNanos);
        }
    }
}
