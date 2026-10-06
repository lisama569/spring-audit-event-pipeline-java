package com.github.lisama569.springauditeventpipelinejava.forward;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.recovery.EventStore;
import com.github.lisama569.springauditeventpipelinejava.recovery.RedeliveryGateway;
import com.github.lisama569.springauditeventpipelinejava.recovery.RedeliveryOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 可靠转发（实时投递 / 重启恢复 / 按范围补投共用同一状态机）。
 *
 * <ul>
 *   <li>主队列按入队（落账）顺序派发；失败进入延迟重试队列（指数退避，最多 5 次），
 *       单条失败不阻塞其它事件；耗尽重试进入死信（持久化，等待人工处理）。</li>
 *   <li>每个 eventId 一把锁 + in-flight 标记：实时投递、恢复重投、补投并发时，
 *       同一事件的“尝试→落状态→排重试”全程串行，目标端幂等，绝不产生重复记录，
 *       尝试次数与状态也不会重复计数或漏计。</li>
 *   <li>所有状态变更经 {@link DeliveryStateStore} 持久化；重启后按事件存储与进度存储
 *       续投未完成事件，已送达/已死信事件不会再投。</li>
 * </ul>
 */
public class ForwardingService implements RedeliveryGateway {

    private static final Logger log = LoggerFactory.getLogger(ForwardingService.class);
    private static final int MAX_ATTEMPTS = 5;
    private static final long BASE_BACKOFF_MILLIS = 50;
    private static final int MAIN_QUEUE_CAPACITY = 10_000;

    private final LocalAuditSink sink;
    private final DeliveryStateStore stateStore;
    private final EventStore eventStore;

    private final BlockingQueue<AuditEvent> mainQueue = new LinkedBlockingQueue<>(MAIN_QUEUE_CAPACITY);
    private final DelayQueue<RetryTask> retryQueue = new DelayQueue<>();
    /** 未完成（在途 + 等待重试）事件计数，用于收敛等待；终态时递减。 */
    private final AtomicInteger pending = new AtomicInteger(0);

    private final ConcurrentHashMap<String, ReentrantLock> eventLocks = new ConcurrentHashMap<>();
    /** 已在主队列/重试队列/正在尝试中的事件，防止任何路径重复排队。 */
    private final ConcurrentHashMap.KeySetView<String, Boolean> inFlight = ConcurrentHashMap.newKeySet();
    /** 死信事件体引用（内存模式下无持久化事件存储时的解析来源；持久化模式优先从存储解析）。 */
    private final ConcurrentHashMap<String, AuditEvent> deadEventBodies = new ConcurrentHashMap<>();

    private volatile Thread dispatcher;
    private volatile Thread retrier;
    private volatile boolean running = false;

    public ForwardingService(LocalAuditSink sink) {
        this(sink, new InMemoryDeliveryStateStore(), null);
    }

    public ForwardingService(LocalAuditSink sink, DeliveryStateStore stateStore, EventStore eventStore) {
        this.sink = sink;
        this.stateStore = stateStore;
        this.eventStore = eventStore;
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        recover();
        running = true;
        dispatcher = new Thread(this::dispatchLoop, "audit-forward-dispatcher");
        retrier = new Thread(this::retryLoop, "audit-forward-retrier");
        dispatcher.setDaemon(true);
        retrier.setDaemon(true);
        dispatcher.start();
        retrier.start();
    }

    /**
     * 重启恢复：把事件存储中存在、但进度不是 DELIVERED 的事件重新排队续投。
     * DEAD 事件不自动续投（保留死信语义，可经补投重新挂起）。恢复在工作线程启动前完成扫描入队，
     * 随后实时事件与恢复事件进入同一主队列，由同一互斥入口处理，互不卡死、互不重复。
     */
    private void recover() {
        if (eventStore == null) {
            return;
        }
        int recovered = 0;
        List<String> recoveredIds = new ArrayList<>();
        eventStore.forEach(event -> {
            DeliveryState state = stateStore.find(event.eventId())
                    .orElseGet(() -> {
                        stateStore.registerIfAbsent(event.eventId());
                        return stateStore.find(event.eventId()).orElseThrow();
                    });
            if (state.status() == DeliveryStatus.DELIVERED) {
                return;
            }
            if (state.status() == DeliveryStatus.DEAD) {
                log.info("恢复：事件 {} 仍为死信，等待人工处理", event.eventId());
                return;
            }
            // 与实时路径同一套在途去重，避免恢复期间新到的同 id 事件重复排队
            if (inFlight.add(event.eventId())) {
                pending.incrementAndGet();
                try {
                    mainQueue.put(event);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    inFlight.remove(event.eventId());
                    pending.decrementAndGet();
                }
                recoveredIds.add(event.eventId());
            }
        });
        log.info("重启恢复完成，续投 {} 条未完成事件: {}", recoveredIds.size(), recoveredIds);
    }

    /** 实时受理入口：持久化事件体、登记进度、入主队列（全程幂等）。 */
    public void enqueue(AuditEvent event) {
        if (eventStore != null) {
            eventStore.append(event);
        }
        stateStore.registerIfAbsent(event.eventId());
        // 已在途（恢复/补投/并发重复受理）则不重复排队
        if (inFlight.add(event.eventId())) {
            pending.incrementAndGet();
            try {
                mainQueue.put(event); // 有界队列 + 背压，长跨度补投不会把内存打爆
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                inFlight.remove(event.eventId());
                pending.decrementAndGet();
                return;
            }
        }
    }

    /**
     * 补投/恢复网关入口：同步执行一次互斥投递尝试。
     * 已送达 -> 直接确认；在途 -> 等待当前尝试结束后按状态确认；死信 -> 重新挂起再投。
     * @return true 当且仅当事件此刻已确认送达
     */
    @Override
    public RedeliveryOutcome redeliver(AuditEvent event) {
        if (eventStore != null) {
            eventStore.append(event);
        }
        stateStore.registerIfAbsent(event.eventId());
        ReentrantLock lock = eventLocks.computeIfAbsent(event.eventId(), k -> new ReentrantLock());
        lock.lock();
        try {
            DeliveryState current = stateStore.find(event.eventId()).orElseThrow();
            if (current.status() == DeliveryStatus.DELIVERED) {
                log.info("补投：事件 {} 当前已送达，幂等跳过", event.eventId());
                return RedeliveryOutcome.ALREADY_DELIVERED;
            }
            boolean wasInFlight = inFlight.contains(event.eventId());
            boolean wasDead = current.status() == DeliveryStatus.DEAD;
            if (wasDead) {
                // 人工/补投重新挂起：重置尝试计数，重新给足重试预算
                stateStore.save(current.withStatus(DeliveryStatus.PENDING, 0));
            }
            if (wasInFlight) {
                // 实时通道正在处理该事件：不重复尝试，以其终态为准
                log.info("补投：事件 {} 实时投递在途，复用在途尝试", event.eventId());
                return RedeliveryOutcome.RESUBMITTED_PENDING;
            }
            pending.incrementAndGet();
            attemptNow(event, wasDead);
            return stateStore.find(event.eventId())
                    .map(s -> s.status() == DeliveryStatus.DELIVERED
                            ? RedeliveryOutcome.RESUBMITTED_DELIVERED
                            : RedeliveryOutcome.RESUBMITTED_PENDING)
                    .orElse(RedeliveryOutcome.RESUBMITTED_PENDING);
        } finally {
            lock.unlock();
        }
    }

    private void dispatchLoop() {
        while (running || !mainQueue.isEmpty()) {
            try {
                AuditEvent event = mainQueue.poll(50, TimeUnit.MILLISECONDS);
                if (event == null) {
                    continue;
                }
                attemptGuarded(event);
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
                attemptGuarded(task.event);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void attemptGuarded(AuditEvent event) {
        ReentrantLock lock = eventLocks.computeIfAbsent(event.eventId(), k -> new ReentrantLock());
        lock.lock();
        try {
            attemptNow(event, false);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 执行一次投递尝试并落定状态。调用方必须持有该事件锁。
     * 这是实时、重试、恢复、补投唯一的投递出口：
     * 尝试次数自增、状态迁移、重试/死信调度在此原子完成，计数不重不漏。
     *
     * @param rearmedFromDead 该事件是否刚由补投从死信重新挂起：
     *                        成功时正常回补 pending；再次死信时不得回补（死信不计 pending）
     */
    private void attemptNow(AuditEvent event, boolean rearmedFromDead) {
        String id = event.eventId();
        DeliveryState state = stateStore.find(id)
                .orElseGet(() -> {
                    stateStore.registerIfAbsent(id);
                    return stateStore.find(id).orElseThrow();
                });
        if (state.status() == DeliveryStatus.DELIVERED) {
            // 终态保护：任何路径都不可能把已送达事件再投一遍
            inFlight.remove(id);
            return;
        }
        int attempt = state.attempts() + 1;
        boolean ok;
        try {
            ok = sink.deliver(event);
        } catch (RuntimeException e) {
            log.warn("事件 {} 第 {} 次投递抛出异常，按失败处理: {}", id, attempt, e.toString());
            ok = false;
        }
        if (ok) {
            stateStore.save(state.withStatus(DeliveryStatus.DELIVERED, attempt));
            deadEventBodies.remove(id);
            inFlight.remove(id);
            pending.decrementAndGet();
            log.info("事件 {} 第 {} 次投递成功，状态=DELIVERED", id, attempt);
            return;
        }
        if (attempt >= MAX_ATTEMPTS) {
            stateStore.save(state.withStatus(DeliveryStatus.DEAD, attempt));
            deadEventBodies.put(id, event);
            inFlight.remove(id);
            if (!rearmedFromDead) {
                // 正常路径（实时/恢复）进入死信：它原在 pending 计数内，需要回补；
                // 死信重新挂起后再次耗尽：死信本不计 pending，不回补
                pending.decrementAndGet();
            }
            log.warn("事件 {} 第 {} 次投递仍失败，进入死信 DEAD", id, attempt);
            return;
        }
        stateStore.save(state.withStatus(DeliveryStatus.RETRYING, attempt));
        long delay = BASE_BACKOFF_MILLIS * (1L << Math.min(attempt - 1, 6));
        retryQueue.offer(new RetryTask(event, attempt,
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay)));
        log.info("事件 {} 第 {} 次投递失败，{}ms 后重试", id, attempt, delay);
    }

    /** 阻塞等待所有已受理事件处理完成（成功送达或进入死信）。 */
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

    /** 投递进度查询（对账依据）。 */
    public Optional<DeliveryState> stateOf(String eventId) {
        return stateStore.find(eventId);
    }

    public List<DeliveryState> states() {
        return stateStore.all();
    }

    /**
     * 对账：把进度中“已送达”集合与本地目标端实际记录集合交叉核对。
     * 高并发写入叠加失败重试后，两者必须一致：无重复计数、无漏计。
     */
    public DeliveryReport reconcile() {
        List<DeliveryState> all = stateStore.all();
        int delivered = 0;
        int pendingOrRetrying = 0;
        int dead = 0;
        java.util.Set<String> deliveredIds = new java.util.TreeSet<>();
        for (DeliveryState s : all) {
            switch (s.status()) {
                case DELIVERED -> {
                    delivered++;
                    deliveredIds.add(s.eventId());
                }
                case PENDING, RETRYING -> pendingOrRetrying++;
                case DEAD -> dead++;
            }
        }
        java.util.Set<String> sinkIds = new java.util.TreeSet<>(sink.deliveredEventIds());
        List<String> onlyInProgress = deliveredIds.stream().filter(id -> !sinkIds.contains(id)).sorted().toList();
        List<String> onlyInSink = sinkIds.stream().filter(id -> !deliveredIds.contains(id)).sorted().toList();
        boolean consistent = onlyInProgress.isEmpty() && onlyInSink.isEmpty()
                && delivered == sinkIds.size();
        return new DeliveryReport(delivered, pendingOrRetrying, dead, sinkIds.size(),
                consistent, onlyInProgress, onlyInSink,
                all.stream().sorted(java.util.Comparator.comparing(DeliveryState::eventId)).toList());
    }

    public int deadLetterCount() {
        return (int) stateStore.all().stream()
                .filter(s -> s.status() == DeliveryStatus.DEAD).count();
    }

    /** 死信事件（含跨重启恢复），按 ledgerSeq 不可得时按 eventId 稳定排序。 */
    public List<AuditEvent> deadLetters() {
        List<String> ids = stateStore.all().stream()
                .filter(s -> s.status() == DeliveryStatus.DEAD)
                .map(DeliveryState::eventId)
                .sorted()
                .toList();
        List<AuditEvent> result = new ArrayList<>();
        for (String id : ids) {
            if (eventStore != null) {
                Optional<AuditEvent> stored = eventStore.find(id);
                if (stored.isPresent()) {
                    result.add(stored.get());
                    continue;
                }
            }
            AuditEvent body = deadEventBodies.get(id);
            if (body != null) {
                result.add(body);
            }
        }
        return List.copyOf(result);
    }

    public void shutdown() {
        running = false;
        if (dispatcher != null) {
            dispatcher.interrupt();
        }
        if (retrier != null) {
            retrier.interrupt();
        }
        if (stateStore instanceof DurableDeliveryStateStore durable) {
            durable.close();
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
