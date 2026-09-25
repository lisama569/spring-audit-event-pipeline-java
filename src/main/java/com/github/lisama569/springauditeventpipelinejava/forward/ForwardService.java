package com.github.lisama569.springauditeventpipelinejava.forward;

import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceVerdict;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 可靠转发服务。
 * <ul>
 *   <li>单工作线程按事件序列号严格有序处理，重试也保持在原序列号位置，
 *       目标文件内顺序与账本序列号一致；</li>
 *   <li>达到重试上限的事件转入死信列表后，工作线程立即越过它处理后续事件，
 *       单条事件的永久失败不长期阻塞其它事件，吞吐不受破坏；</li>
 *   <li>越过顺序由目标端 eventId 幂等兜底，{@link #retryDeadLetters()} 重投不产生重复；</li>
 *   <li>enqueue 按 eventId 幂等去重，与目标端恢复去重共同保证不丢不重。</li>
 * </ul>
 */
public class ForwardService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ForwardService.class);

    private final ForwardTarget target;
    private final int maxAttempts;
    private final long backoffMillis;
    private final long gapGraceMillis;

    /** 全部待处理任务，键为事件序列号；重试只更新就绪时间，不移位。 */
    private final ConcurrentSkipListMap<Long, Item> pending = new ConcurrentSkipListMap<>();
    /** 下一个允许投递的序列号。用宽限窗口处理“已落账但尚未入队”的短暂缺口。 */
    private long nextSequence = 0;
    /** 队首序列号超过 nextSequence 时，等到该时间点仍缺号则越过缺口。 */
    private long gapWaitDeadlineNanos = 0;
    private static final long NO_GAP_WAIT = 0;
    private final Set<String> queuedIds = ConcurrentHashMap.newKeySet();
    private final List<Item> deadLetters = new ArrayList<>();
    private final AtomicLong inFlight = new AtomicLong(0);
    private final AtomicBoolean delivering = new AtomicBoolean(false);

    private final ReentrantLock wakeLock = new ReentrantLock();
    private final Condition wake = wakeLock.newCondition();
    private final Object flushLock = new Object();

    private volatile Thread worker;
    private volatile boolean running = true;

    public ForwardService(ForwardTarget target) {
        this(target, 5, 50L, 100L);
    }

    public ForwardService(ForwardTarget target, int maxAttempts, long backoffMillis) {
        this(target, maxAttempts, backoffMillis, 100L);
    }

    /**
     * @param gapGraceMillis 序列号缺口宽限：队首序列号不连续时最多等待多久再越过缺口
     */
    public ForwardService(ForwardTarget target, int maxAttempts, long backoffMillis, long gapGraceMillis) {
        this.target = target;
        this.maxAttempts = maxAttempts;
        this.backoffMillis = backoffMillis;
        this.gapGraceMillis = gapGraceMillis;
        start();
    }

    private void start() {
        worker = new Thread(this::run, "audit-forward-worker");
        worker.setDaemon(true);
        worker.start();
    }

    /** 幂等入队：重复 eventId 不会产生第二次投递任务。 */
    public void enqueue(AuditEvent event, ComplianceVerdict verdict) {
        if (!queuedIds.add(event.eventId())) {
            log.debug("转发任务已存在，重复入队被忽略: eventId={}", event.eventId());
            return;
        }
        inFlight.incrementAndGet();
        wakeLock.lock();
        try {
            pending.put(event.sequence(), new Item(event, verdict, 0, System.nanoTime()));
            wake.signalAll();
        } finally {
            wakeLock.unlock();
        }
    }

    private void run() {
        while (running) {
            try {
                wakeLock.lockInterruptibly();
                try {
                    Item head = awaitHeadLocked();
                    if (head == null) {
                        continue;
                    }
                    // 投递全程持锁：保证投递期间不会有更小序列号事件入队并抢先投递
                    attemptLocked(head);
                } finally {
                    wakeLock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t) {
                log.error("转发工作线程异常", t);
            }
        }
    }

    /**
     * 在已持有 {@link #wakeLock} 的前提下等待下一个可投递任务并移出 pending。
     * <p>队首序列号须连续；出现缺口（事件已落账但尚未入队）时在宽限窗口内等待补缺，
     * 窗口耗尽仍缺号则越过缺口（缺号事件不会丢：它一旦入队仍会被处理，目标端幂等）。
     */
    private Item awaitHeadLocked() throws InterruptedException {
        while (running) {
            Map.Entry<Long, Item> first = pending.firstEntry();
            if (first == null) {
                gapWaitDeadlineNanos = NO_GAP_WAIT;
                wake.await(50, TimeUnit.MILLISECONDS);
                continue;
            }
            long headSeq = first.getKey();
            long now = System.nanoTime();
            long deadline;
            if (headSeq > nextSequence) {
                if (gapWaitDeadlineNanos == NO_GAP_WAIT) {
                    gapWaitDeadlineNanos = now + TimeUnit.MILLISECONDS.toNanos(gapGraceMillis);
                }
                if (now < gapWaitDeadlineNanos) {
                    // 等待缺口事件入队
                    wake.awaitNanos(gapWaitDeadlineNanos - now);
                    continue;
                }
                log.warn("序列号缺口 [{}..{}) 等待 {}ms 未到，越过缺口继续投递",
                        nextSequence, headSeq, gapGraceMillis);
            }
            gapWaitDeadlineNanos = NO_GAP_WAIT;

            Item item = first.getValue();
            long readyDelay = item.readyAtNanos - System.nanoTime();
            if (readyDelay > 0) {
                wake.awaitNanos(readyDelay);
                continue;
            }
            pending.remove(headSeq);
            return item;
        }
        return null;
    }

    /** 投递结束（成功或死信）后推进已处理水位；缺号事件后到仍会按 pending 键正常投递。 */
    private void advanceLocked(long sequence) {
        if (sequence >= nextSequence) {
            nextSequence = sequence + 1;
        }
    }

    /** 在已持有 {@link #wakeLock} 的前提下投递并更新 pending/死信。 */
    private void attemptLocked(Item item) {
        delivering.set(true);
        try {
            boolean delivered;
            Exception failure = null;
            try {
                target.deliver(item.event, item.verdict);
                delivered = true;
            } catch (Exception e) {
                delivered = false;
                failure = e;
            }
            int attempts = item.attempts + 1;
            if (delivered) {
                queuedIds.remove(item.event.eventId());
                inFlight.decrementAndGet();
                advanceLocked(item.sequence);
                log.debug("投递完成: eventId={}, seq={}", item.event.eventId(), item.sequence);
                notifyFlush();
            } else if (attempts >= maxAttempts) {
                log.error("投递失败达到上限，转入死信: eventId={}, attempts={}",
                        item.event.eventId(), attempts, failure);
                synchronized (deadLetters) {
                    deadLetters.add(new Item(item.event, item.verdict, attempts, item.readyAtNanos));
                }
                pending.remove(item.sequence);
                queuedIds.remove(item.event.eventId());
                inFlight.decrementAndGet();
                advanceLocked(item.sequence);
                notifyFlush();
            } else {
                long delay = backoffMillis * (1L << Math.min(attempts - 1, 4));
                log.warn("投递失败，{}ms 后第{}次重试(保持序列号位置): eventId={}",
                        delay, attempts + 1, item.event.eventId(), failure);
                pending.put(item.sequence, new Item(item.event, item.verdict, attempts,
                        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay)));
            }
        } finally {
            delivering.set(false);
        }
    }

    /** 将死信事件重新入队（目标端幂等，重投安全）。 */
    public int retryDeadLetters() {
        List<Item> snapshot;
        synchronized (deadLetters) {
            snapshot = new ArrayList<>(deadLetters);
            deadLetters.clear();
        }
        wakeLock.lock();
        try {
            for (Item item : snapshot) {
                if (queuedIds.add(item.event.eventId())) {
                    inFlight.incrementAndGet();
                    pending.put(item.sequence, new Item(item.event, item.verdict, 0, System.nanoTime()));
                }
            }
            wake.signalAll();
        } finally {
            wakeLock.unlock();
        }
        return snapshot.size();
    }

    public int deadLetterCount() {
        synchronized (deadLetters) {
            return deadLetters.size();
        }
    }

    /** 等待所有非死信任务处理完毕（测试与本地验证用）。 */
    public boolean awaitQuiescence(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (flushLock) {
            while (inFlight.get() > 0 || delivering.get()) {
                if (System.nanoTime() >= deadline) {
                    return false;
                }
                flushLock.wait(20);
            }
        }
        return true;
    }

    private void notifyFlush() {
        synchronized (flushLock) {
            flushLock.notifyAll();
        }
    }

    @Override
    public void close() {
        running = false;
        wakeLock.lock();
        try {
            wake.signalAll();
        } finally {
            wakeLock.unlock();
        }
        if (worker != null) {
            worker.interrupt();
        }
    }

    private static final class Item {
        final AuditEvent event;
        final ComplianceVerdict verdict;
        final int attempts;
        final long readyAtNanos;
        final long sequence;

        Item(AuditEvent event, ComplianceVerdict verdict, int attempts, long readyAtNanos) {
            this.event = event;
            this.verdict = verdict;
            this.attempts = attempts;
            this.readyAtNanos = readyAtNanos;
            this.sequence = event.sequence();
        }
    }
}
