package com.github.lisama569.springauditeventpipelinejava.forward;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 可靠转发：
 * - 主队列按入队（落账）顺序派发；
 * - 失败事件进入延迟重试队列，指数退避，不阻塞主队列其它事件；
 * - 超过最大重试次数进入死信表，保证不丢失；
 * - 目标端幂等，重复投递不产生重复。
 */
public class ForwardingService {

    private static final int MAX_ATTEMPTS = 5;
    private static final long BASE_BACKOFF_MILLIS = 50;

    private final LocalAuditSink sink;
    private final BlockingQueue<AuditEvent> mainQueue = new LinkedBlockingQueue<>();
    private final DelayQueue<RetryTask> retryQueue = new DelayQueue<>();
    private final List<AuditEvent> deadLetters = new CopyOnWriteArrayList<>();
    private final AtomicInteger pending = new AtomicInteger(0);

    private volatile Thread dispatcher;
    private volatile Thread retrier;
    private volatile boolean running = false;

    public ForwardingService(LocalAuditSink sink) {
        this.sink = sink;
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
    }

    public void enqueue(AuditEvent event) {
        pending.incrementAndGet();
        mainQueue.offer(event);
    }

    private void dispatchLoop() {
        while (running || !mainQueue.isEmpty()) {
            try {
                AuditEvent event = mainQueue.poll(50, TimeUnit.MILLISECONDS);
                if (event == null) {
                    continue;
                }
                if (!sink.deliver(event)) {
                    scheduleRetry(event, 1);
                } else {
                    pending.decrementAndGet();
                }
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
                if (sink.deliver(task.event)) {
                    pending.decrementAndGet();
                } else if (task.attempt + 1 >= MAX_ATTEMPTS) {
                    deadLetters.add(task.event);
                    pending.decrementAndGet();
                } else {
                    scheduleRetry(task.event, task.attempt + 1);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void scheduleRetry(AuditEvent event, int attempt) {
        long delay = BASE_BACKOFF_MILLIS * (1L << Math.min(attempt - 1, 6));
        retryQueue.offer(new RetryTask(event, attempt, System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(delay)));
    }

    /** 阻塞等待所有已入队事件处理完成（成功或进入死信）。 */
    public void awaitQuiescent(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (pending.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        if (pending.get() > 0) {
            throw new IllegalStateException("转发未在超时时间内收敛，pending=" + pending.get());
        }
    }

    public int deadLetterCount() {
        return deadLetters.size();
    }

    public List<AuditEvent> deadLetters() {
        return List.copyOf(deadLetters);
    }

    public void shutdown() {
        running = false;
        if (dispatcher != null) {
            dispatcher.interrupt();
        }
        if (retrier != null) {
            retrier.interrupt();
        }
    }

    private record RetryTask(AuditEvent event, int attempt, long deadlineNanos) implements Delayed {
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
