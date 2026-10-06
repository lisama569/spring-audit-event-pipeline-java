package com.github.lisama569.springauditeventpipelinejava.tests;

import com.github.lisama569.springauditeventpipelinejava.api.dto.AuditEventRequest;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleEngine;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardingService;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditLedger;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.model.DeliveryState;
import com.github.lisama569.springauditeventpipelinejava.model.DeliveryStatus;
import com.github.lisama569.springauditeventpipelinejava.model.IngestResult;
import com.github.lisama569.springauditeventpipelinejava.model.ReconciliationReport;
import com.github.lisama569.springauditeventpipelinejava.model.ReplayJobView;
import com.github.lisama569.springauditeventpipelinejava.model.ReplayRequest;
import com.github.lisama569.springauditeventpipelinejava.pipeline.AuditPipeline;
import com.github.lisama569.springauditeventpipelinejava.recovery.DurableDeliveryStateStore;
import com.github.lisama569.springauditeventpipelinejava.recovery.DurableEventStore;
import com.github.lisama569.springauditeventpipelinejava.recovery.DurableLocalAuditSink;
import com.github.lisama569.springauditeventpipelinejava.recovery.Reconciler;
import com.github.lisama569.springauditeventpipelinejava.recovery.ReplayService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 组合场景：重启恢复、按范围补投、补投与实时并发、高并发对账。
 * 所有用例共用同一组持久化日志文件，通过“停服务 -> 新建组件”模拟进程重启。
 */
class RecoveryReplayReconciliationTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryReplayReconciliationTest.class);

    @TempDir
    Path dataDir;

    private DurableEventStore eventStore;
    private DurableDeliveryStateStore stateStore;
    private DurableLocalAuditSink sink;
    private AuditLedger ledger;
    private ForwardingService forwarding;
    private AuditPipeline pipeline;
    private Reconciler reconciler;
    private ReplayService replayService;

    /** 一组绑在一起的组件，关闭即模拟进程停止。 */
    private record Components(DurableEventStore eventStore, DurableDeliveryStateStore stateStore,
                              DurableLocalAuditSink sink, AuditLedger ledger,
                              ForwardingService forwarding, AuditPipeline pipeline,
                              Reconciler reconciler, ReplayService replayService) {
        void shutdown() {
            forwarding.shutdown();
        }
    }

    @BeforeEach
    void setUp() {
        Components c = boot(false);
        this.eventStore = c.eventStore();
        this.stateStore = c.stateStore();
        this.sink = c.sink();
        this.ledger = c.ledger();
        this.forwarding = c.forwarding();
        this.pipeline = c.pipeline();
        this.reconciler = c.reconciler();
        this.replayService = c.replayService();
    }

    @AfterEach
    void tearDown() {
        if (forwarding != null) {
            forwarding.shutdown();
        }
    }

    /** 从持久化日志启动一套全新组件（模拟进程重启）；可选择注入持续失败的 eventId。 */
    private Components boot(boolean recoverOnly) {
        DurableEventStore es = new DurableEventStore(dataDir.resolve("events.log"));
        DurableDeliveryStateStore ss = new DurableDeliveryStateStore(dataDir.resolve("delivery-state.log"));
        DurableLocalAuditSink sk = new DurableLocalAuditSink(dataDir.resolve("sink-delivered.log"));
        sk.recover();
        AuditLedger lg = new AuditLedger(es);
        RuleRegistry registry = new RuleRegistry();
        ForwardingService fs = new ForwardingService(sk, ss, es);
        AuditPipeline pp = new AuditPipeline(lg, registry, new RuleEngine(), fs);
        Reconciler rc = new Reconciler(lg, ss, sk);
        ReplayService rs = new ReplayService(es, fs);
        fs.start(); // start 即恢复：PENDING 接着送，DELIVERED 不重投
        return new Components(es, ss, sk, lg, fs, pp, rc, rs);
    }

    private IngestResult submit(String id, String occurredAt) {
        AuditEventRequest req = new AuditEventRequest(
                id, occurredAt, "alice", "db", "READ", Map.of());
        log.info("提交事件 eventId={} occurredAt={}", id, occurredAt);
        return pipeline.submit(req);
    }

    // ---- 用例将在后续小步中逐个补充 ----

    @Test
    void restartResumesPendingEventsWithoutDuplicatingDeliveredOnes() throws Exception {
        // 第一阶段：evt-0/evt-1 正常送达
        submit("evt-0", "2026-10-01T10:00:00Z");
        submit("evt-1", "2026-10-01T10:01:00Z");
        forwarding.awaitQuiescent(10_000);
        assertEquals(2, sink.deliveredCount());

        // 制造在途未送达：注入失败，让 evt-2/evt-3 已被尝试但仍处于重试窗口（PENDING）
        sink.failNextDeliveries(10);
        submit("evt-2", "2026-10-01T10:02:00Z");
        submit("evt-3", "2026-10-01T10:03:00Z");
        waitForStatus("evt-2", DeliveryStatus.PENDING, 1, 5_000);
        waitForStatus("evt-3", DeliveryStatus.PENDING, 1, 5_000);
        log.info("重启前目标端已送达={}，evt-2/evt-3 仍为 PENDING", sink.deliveredEventIds());

        // 模拟进程硬停（evt-2/evt-3 尚未送达、未到 DEAD）
        forwarding.shutdown();
        Components restarted = boot(true);
        try {
            // 重启后账本、目标端视图从磁盘重建：已送达的仍在
            assertEquals(4, restarted.ledger().size(), "账本重启后必须完整重建");
            assertEquals(List.of("evt-0", "evt-1"), restarted.sink().deliveredEventIds(),
                    "已送达事件重启后仍在目标端，且顺序稳定");

            // 恢复期间继续受理实时事件（目标端此时恢复正常），新事件正常送达
            IngestResult live = restarted.pipeline().submit(new AuditEventRequest(
                    "evt-live", "2026-10-01T10:04:00Z", "bob", "db", "READ", Map.of()));
            assertEquals(IngestResult.Status.ACCEPTED, live.status());

            // 恢复事件 + 实时事件一起收敛，互不卡死
            restarted.forwarding().awaitQuiescent(10_000);

            // 目标端最终内容 = 4 个事件各恰好送达一次；已送达的没有重复一条
            List<String> finalIds = restarted.sink().deliveredEventIds();
            assertEquals(List.of("evt-0", "evt-1", "evt-2", "evt-3", "evt-live"), finalIds,
                    "重启后未送达事件须接着送完，已送达的不得重复，顺序保持业务稳定");
            assertEquals(5, finalIds.stream().distinct().count(), "目标端不得出现重复 eventId");

            // 再次触发恢复也不重复
            restarted.forwarding().recoverPending();
            restarted.forwarding().awaitQuiescent(5_000);
            assertEquals(finalIds, restarted.sink().deliveredEventIds(),
                    "重复恢复不得改变目标端最终内容");

            // 进度与三方对平
            ReconciliationReport report = restarted.reconciler().reconcile();
            assertTrue(report.consistent(), "恢复后三方对账必须一致: " + report);
            assertEquals(5, restarted.reconciler().progress().delivered());
            log.info("恢复完成后目标端={}，对账一致={}", finalIds, report.consistent());
        } finally {
            restarted.shutdown();
        }
    }

    private void waitForStatus(String eventId, DeliveryStatus status, int minAttempts,
                               long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            DeliveryState state = forwarding.stateOf(eventId).orElse(null);
            if (state != null && state.status() == status && state.attempts() >= minAttempts) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("事件 " + eventId + " 在超时内未达到状态 " + status
                + " 且尝试次数 >= " + minAttempts);
    }

    private void waitForAttempts(String eventId, int minAttempts, long timeoutMillis)
            throws InterruptedException {
        waitForStatus(eventId,
                forwarding.stateOf(eventId).map(DeliveryState::status).orElse(DeliveryStatus.PENDING),
                minAttempts, timeoutMillis);
    }

    @Test
    void rangeReplayRedeliversExactlyOnceAndSkipsAlreadyDelivered() throws Exception {
        // 实时阶段：r-0/r-1 正常送达
        submit("r-0", "2026-09-10T08:00:00Z");
        submit("r-1", "2026-09-11T08:00:00Z");
        forwarding.awaitQuiescent(10_000);

        // 直接向事件存储写入 r-2/r-3/r-4，但不建立投递状态：
        // 模拟“已收下、投递状态初始化前崩溃/长期离线”的事件，自动恢复（只扫 PENDING）不会碰它们，
        // 由按范围补投负责送达。
        long seq = ledger.snapshotOrdered().stream()
                .mapToLong(AuditEvent::ledgerSeq).max().orElse(0);
        for (int i = 2; i <= 4; i++) {
            eventStore.append(new AuditEvent("r-" + i, java.time.Instant.parse("2026-09-"
                    + (10 + i - 1) + "T08:00:00Z"), "alice", "db", "READ",
                    Map.of(), seq + (i - 1), java.time.Instant.now()));
        }
        forwarding.shutdown();

        Components restarted = boot(true);
        try {
            assertEquals(5, restarted.ledger().size(), "持久化事件重启后进入账本");
            assertEquals(List.of("r-0", "r-1"), restarted.sink().deliveredEventIds(),
                    "无投递状态的事件不会被自动恢复投递");

            // 对覆盖全部 5 条的业务时间范围发起补投
            String jobId = restarted.replayService().start(new ReplayRequest(
                    "2026-09-10T00:00:00Z", "2026-09-15T00:00:00Z", null));
            ReplayJobView view = restarted.replayService().await(jobId, 10_000);
            log.info("范围补投任务结果={}", view);

            assertEquals("COMPLETED", view.status());
            assertEquals(5, view.matched());
            assertEquals(2, view.already(), "补投前已送达的 2 条应被跳过");
            assertEquals(3, view.delivered(), "未送达的 3 条应由补投送达");
            assertEquals(0, view.failed());

            // 目标端最终内容 == 5 个事件各自正常投递一次：不多、不少、业务顺序稳定
            assertEquals(List.of("r-0", "r-1", "r-2", "r-3", "r-4"),
                    restarted.sink().deliveredEventIds());

            // 再补投同一范围：全部已送达，目标端内容不变（幂等补投）
            String again = restarted.replayService().start(new ReplayRequest(
                    "2026-09-10T00:00:00Z", "2026-09-15T00:00:00Z", null));
            ReplayJobView view2 = restarted.replayService().await(again, 10_000);
            assertEquals(5, view2.matched());
            assertEquals(5, view2.already());
            assertEquals(0, view2.delivered());
            assertEquals(List.of("r-0", "r-1", "r-2", "r-3", "r-4"),
                    restarted.sink().deliveredEventIds(), "重复补投不得产生任何新记录");

            ReconciliationReport report = restarted.reconciler().reconcile();
            assertTrue(report.consistent(), "补投后三方对账必须一致: " + report);
            log.info("范围补投后目标端={}，对账一致={}",
                    restarted.sink().deliveredEventIds(), report.consistent());
        } finally {
            restarted.shutdown();
        }
    }

    @Test
    void replayByIdsRedeliversOnlyTheSpecifiedBatch() throws Exception {
        for (int i = 0; i < 6; i++) {
            submit("b-" + i, "2026-09-20T10:00:0" + i + "Z");
        }
        forwarding.awaitQuiescent(10_000);
        assertEquals(6, sink.deliveredCount());

        // 全部已送达：按指定一批补投，应全部跳过、目标端不变
        String jobId = replayService.start(new ReplayRequest(
                null, null, List.of("b-1", "b-3", "b-5", "b-1")));
        ReplayJobView view = replayService.await(jobId, 10_000);
        assertEquals("COMPLETED", view.status());
        assertEquals(3, view.matched(), "重复 id 去重后命中 3 条");
        assertEquals(3, view.already());
        assertEquals(6, sink.deliveredCount());
        log.info("按 id 补投结果={}", view);

        // 未知 id 不计入命中
        String jobId2 = replayService.start(new ReplayRequest(
                null, null, List.of("b-0", "ghost")));
        ReplayJobView view2 = replayService.await(jobId2, 10_000);
        assertEquals(1, view2.matched());
    }

    @Test
    void rangeReplayRunsConcurrentlyWithLiveTrafficWithoutLossOrDuplication() throws Exception {
        // 准备 500 个“已收下但未投递”的历史事件（无投递状态），跨越较大时间范围，
        // 数量远超补投分页大小(64)与在途窗口(64)，用于验证有界工作集与分页正确性
        int history = 500;
        for (int i = 0; i < history; i++) {
            java.time.Instant occurred = java.time.Instant.parse("2026-08-01T00:00:00Z")
                    .plusSeconds(i * 60L);
            eventStore.append(new AuditEvent("h-" + i, occurred, "alice", "db", "READ",
                    Map.of(), i + 1L, java.time.Instant.now()));
        }
        forwarding.shutdown();

        Components restarted = boot(true);
        try {
            assertEquals(0, restarted.sink().deliveredCount());

            // 一边发起全量范围补投（跨约 8 小时），一边多线程持续写入实时事件
            String jobId = restarted.replayService().start(new ReplayRequest(
                    "2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z", null));

            int liveThreads = 4;
            int livePerThread = 50;
            java.util.concurrent.ExecutorService pool =
                    java.util.concurrent.Executors.newFixedThreadPool(liveThreads);
            java.util.concurrent.CountDownLatch startGun = new java.util.concurrent.CountDownLatch(1);
            for (int t = 0; t < liveThreads; t++) {
                final int threadNo = t;
                pool.submit(() -> {
                    try {
                        startGun.await();
                        for (int i = 0; i < livePerThread; i++) {
                            String id = "live-" + threadNo + "-" + i;
                            restarted.pipeline().submit(new AuditEventRequest(id,
                                    "2026-08-01T12:" + String.format("%02d", i % 60) + ":00Z",
                                    "bob", "db", "READ", Map.of()));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            startGun.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS),
                    "实时写入必须在补投进行时持续推进，不能互相卡死");

            // 等补投任务与所有实时投递收敛
            ReplayJobView view = restarted.replayService().await(jobId, 60_000);
            log.info("并发补投任务结果={}", view);
            assertEquals("COMPLETED", view.status());
            restarted.forwarding().awaitQuiescent(30_000);

            int liveTotal = liveThreads * livePerThread;
            int expectedTotal = history + liveTotal;
            List<String> finalIds = restarted.sink().deliveredEventIds();
            log.info("并发结束后目标端记录数={}（历史 {} + 实时 {}）",
                    finalIds.size(), history, liveTotal);

            // 不重不漏：数量正确且无重复
            assertEquals(expectedTotal, finalIds.size(), "目标端记录数必须等于历史+实时");
            assertEquals(expectedTotal, finalIds.stream().distinct().count(),
                    "目标端不得出现任何重复 eventId");

            // 历史事件子集与实时事件子集都齐全
            java.util.Set<String> idSet = new java.util.HashSet<>(finalIds);
            for (int i = 0; i < history; i++) {
                assertTrue(idSet.contains("h-" + i), "历史事件 h-" + i + " 缺失");
            }

            // 业务顺序稳定：最终视图按 (occurredAt, ledgerSeq) 升序
            List<String> businessOrder = restarted.sink().deliveredEvents().stream()
                    .map(AuditEvent::eventId).toList();
            assertEquals(businessOrder, finalIds, "目标端最终视图必须为稳定的业务顺序");

            ReconciliationReport report = restarted.reconciler().reconcile();
            assertTrue(report.consistent(), "高并发补投/实时结束后三方对账必须一致: " + report);
            assertEquals(expectedTotal, restarted.reconciler().progress().delivered(),
                    "已送达计数必须与目标端实际内容一致");
        } finally {
            restarted.shutdown();
        }
    }

    @Test
    void reconciliationMatchesProgressToActualSinkUnderConcurrentWritesAndRetries() throws Exception {
        // 高并发写入，期间注入若干次失败触发重试，并指定一条永久失败进入死信
        int threads = 8;
        int perThread = 50;
        sink.failNextDeliveries(40); // 并发期间制造大量瞬时失败 -> 重试
        sink.failAlways("evt-dead"); // 一条永久失败 -> DEAD

        java.util.concurrent.ExecutorService pool =
                java.util.concurrent.Executors.newFixedThreadPool(threads);
        java.util.concurrent.CountDownLatch gun = new java.util.concurrent.CountDownLatch(1);
        java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int threadNo = t;
            futures.add(pool.submit(() -> {
                try {
                    gun.await();
                    for (int i = 0; i < perThread; i++) {
                        String id = (threadNo == 0 && i == 0) ? "evt-dead"
                                : "c-" + threadNo + "-" + i;
                        pipeline.submit(new AuditEventRequest(id,
                                "2026-09-25T10:" + String.format("%02d", i % 60) + ":00Z",
                                "alice", "db", "READ", Map.of()));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        gun.countDown();
        for (java.util.concurrent.Future<?> f : futures) {
            f.get(30, java.util.concurrent.TimeUnit.SECONDS);
        }
        pool.shutdown();

        forwarding.awaitQuiescent(60_000);

        int totalDistinct = threads * perThread; // 400 个不同 id（evt-dead 占用其中一个）
        var progress = reconciler.progress();
        ReconciliationReport report = reconciler.reconcile();
        log.info("高并发对账：progress={}，目标端记录数={}，死信={}，原始尝试次数={}",
                progress, sink.deliveredCount(), forwarding.deadLetters(), sink.rawAttemptCount());

        // 进度三分项之和等于总状态数
        assertEquals(progress.total(), progress.pending() + progress.delivered() + progress.dead(),
                "PENDING/DELIVERED/DEAD 计数之和必须等于总数，不重不漏");
        assertEquals(0, progress.pending(), "收敛后不应有仍在等待/重试的事件");
        assertEquals(1, progress.dead(), "永久失败事件恰好一条进入死信");
        assertEquals(totalDistinct - 1, progress.delivered(), "其余事件全部送达");
        assertEquals(totalDistinct, progress.total());

        // 目标端实物与已送达计数对平
        assertEquals(progress.delivered(), sink.deliveredCount(),
                "已送达计数必须等于目标端实际记录数");
        assertEquals(sink.deliveredCount(), sink.deliveredEventIds().stream().distinct().count(),
                "目标端不得有重复记录");

        // 三方对平；死信事件不出现在目标端
        assertTrue(report.consistent(), "账本/状态表/目标端三方必须对平: " + report);
        assertFalse(sink.deliveredEventIds().contains("evt-dead"), "死信事件不得进入目标端");
        assertEquals(List.of("evt-dead"), report.deadLetters());

        // 尝试次数：死信事件恰好耗尽 MAX_ATTEMPTS；累计尝试 >= 送达事件数（有重试就更大）
        DeliveryState deadState = forwarding.stateOf("evt-dead").orElseThrow();
        assertEquals(DeliveryStatus.DEAD, deadState.status());
        assertEquals(ForwardingService.MAX_ATTEMPTS, deadState.attempts(),
                "死信事件必须恰好尝试到最大次数");
        assertTrue(progress.totalAttempts() >= progress.delivered() + ForwardingService.MAX_ATTEMPTS,
                "累计尝试次数必须把成功与重试都计入，且不重复计数");

        // 每个成功事件的状态都是 DELIVERED 且至少尝试一次（随机抽查全部）
        for (String id : sink.deliveredEventIds()) {
            DeliveryState s = forwarding.stateOf(id).orElseThrow();
            assertEquals(DeliveryStatus.DELIVERED, s.status(), "目标端有的事件状态必须是 DELIVERED");
            assertTrue(s.attempts() >= 1, "尝试次数必须被记录");
        }
        log.info("高并发对账通过：已送达={}，死信={}，总尝试={}",
                progress.delivered(), progress.dead(), progress.totalAttempts());
    }

    @Test
    void replayRetriesAndCanBeSafelyRestartedWithoutRewritingDeliveredEvents() throws Exception {
        // 120 个未投递历史事件，超过补投分页/在途窗口，便于在任务进行中“中断”
        int total = 120;
        for (int i = 0; i < total; i++) {
            java.time.Instant occurred = java.time.Instant.parse("2026-07-01T00:00:00Z")
                    .plusSeconds(i * 60L);
            eventStore.append(new AuditEvent("x-" + i, occurred, "alice", "db", "READ",
                    Map.of(), i + 1L, java.time.Instant.now()));
        }
        forwarding.shutdown();

        Components restarted = boot(true);
        try {
            // 目标端前 30 次投递失败（触发补投重试/死信分化），然后恢复
            restarted.sink().failNextDeliveries(30);

            String jobId = restarted.replayService().start(new ReplayRequest(
                    "2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z", null));
            ReplayJobView view = restarted.replayService().await(jobId, 60_000);
            restarted.forwarding().awaitQuiescent(30_000);
            log.info("首次补投（含失败）结果={}，目标端记录数={}", view,
                    restarted.sink().deliveredCount());

            // 故障窗口内的事件一部分重试成功，被连续打满 5 次的事件进入 DEAD
            List<String> deadAfterFirst = restarted.stateStore()
                    .eventIdsByStatus(DeliveryStatus.DEAD);
            log.info("首次补投后死信={}", deadAfterFirst);
            ReconciliationReport report = restarted.reconciler().reconcile();
            assertTrue(report.consistent(), "补投+重试收敛后三方必须对平: " + report);

            // 模拟“中断之后重新发起”：再跑一次同样范围的补投（此时目标端已恢复）。
            // - 已送达事件必须跳过，目标端不重写、不重复；
            // - 首次进入 DEAD 的事件由显式补投复活并最终送达，整批事件补齐不遗漏。
            List<String> before = restarted.sink().deliveredEventIds();
            int rawBefore = restarted.sink().rawAttemptCount();
            String jobId2 = restarted.replayService().start(new ReplayRequest(
                    "2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z", null));
            ReplayJobView view2 = restarted.replayService().await(jobId2, 60_000);
            restarted.forwarding().awaitQuiescent(30_000);
            log.info("重新发起补投结果={}", view2);

            // 之前已送达的仍然各只有一条（before 是新结果列表的子集且无重复）
            List<String> after = restarted.sink().deliveredEventIds();
            assertEquals(total, after.size(), "两次补投后整批事件必须全部送达");
            assertEquals(total, after.stream().distinct().count(), "目标端不得出现重复");
            assertTrue(after.containsAll(before), "已送达事件必须保留");
            assertEquals(before.size(), view2.already(),
                    "第二次补投中首次已送达的事件必须全部计为跳过");
            assertEquals(deadAfterFirst.size(), view2.delivered(),
                    "首次死信的事件应由第二次显式补投复活并送达");

            // 最终无死信、三方对平
            assertEquals(List.of(), restarted.stateStore().eventIdsByStatus(DeliveryStatus.DEAD),
                    "重新补投成功后不应再有死信");
            ReconciliationReport finalReport = restarted.reconciler().reconcile();
            assertTrue(finalReport.consistent(), "重新补投后三方必须对平: " + finalReport);
            assertEquals(total, restarted.reconciler().progress().delivered());
            log.info("重新补投后目标端记录数={}（首次 {} + 死信补送 {}），原始尝试 {} -> {}",
                    after.size(), before.size(), deadAfterFirst.size(),
                    rawBefore, restarted.sink().rawAttemptCount());
        } finally {
            restarted.shutdown();
        }
    }

    @Test
    void longRangeReplayKeepsInflightMemoryBoundedRegardlessOfTotalEvents() throws Exception {
        // 2000 个未投递历史事件（远超分页 64 与在途窗口 64），目标端先持续失败，
        // 使大量事件停在 PENDING/重试阶段：这是补投在途集合最容易“随总量线性膨胀”的时机
        int total = 2000;
        for (int i = 0; i < total; i++) {
            java.time.Instant occurred = java.time.Instant.parse("2026-06-01T00:00:00Z")
                    .plusSeconds(i * 30L);
            eventStore.append(new AuditEvent("m-" + i, occurred, "alice", "db", "READ",
                    Map.of(), i + 1L, java.time.Instant.now()));
        }
        forwarding.shutdown();

        Components restarted = boot(true);
        try {
            // 持续失败：补投事件停在重试/PENDING（注意不要直接判死信——永久失败仍会每 5 次进 DEAD，
            // 这里用足够大的瞬时失败预算在任务前期制造堆积，随后恢复让任务收敛）
            restarted.sink().failNextDeliveries(100_000);

            String jobId = restarted.replayService().start(new ReplayRequest(
                    "2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", null));

            // 任务运行期间多次采样在途峰值
            int observedPeak = 0;
            long deadline = System.currentTimeMillis() + 8_000;
            while (System.currentTimeMillis() < deadline
                    && restarted.replayService().status(jobId)
                        .map(v -> v.status().equals("RUNNING")).orElse(false)) {
                observedPeak = Math.max(observedPeak, restarted.forwarding().peakInflight());
                Thread.sleep(20);
            }
            log.info("故障期间补投在途峰值观测={}（分页/在途窗口上界={}，事件总量={}）",
                    observedPeak, ReplayService.MAX_INFLIGHT, total);

            // 在途事件数被有界窗口卡住，与 2000 的事件总量无关
            // 允许少量浮动（dispatcher 恢复认领与重试队列），但必须远小于总量且贴近窗口上界
            assertTrue(observedPeak <= ReplayService.MAX_INFLIGHT + 8,
                    "长跨度补投的同时在途事件数必须有界，观测峰值=" + observedPeak);
            assertTrue(observedPeak >= 1, "故障期间应当确实积累过在途事件，采样才有效");

            // 恢复目标端：故障预算是进程内状态，重启一套全新组件即清零；再全量补投必须全部送达
            restarted.shutdown();
            Components healthy = boot(true);
            try {
                // 首次故障补投可能已把部分事件打成 DEAD：显式补投会复活死信，整批最终补齐
                String jobId2 = healthy.replayService().start(new ReplayRequest(
                        "2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", null));
                healthy.replayService().await(jobId2, 60_000);
                healthy.forwarding().awaitQuiescent(30_000);
                assertEquals(total, healthy.sink().deliveredCount(),
                        "目标端恢复后，全量补投必须把 2000 条全部送达，不重不漏");
                ReconciliationReport report = healthy.reconciler().reconcile();
                assertTrue(report.consistent(), "长跨度补投收敛后三方必须对平: " + report);
                log.info("长跨度补投收敛：目标端记录数={}，对账一致={}",
                        healthy.sink().deliveredCount(), report.consistent());
            } finally {
                healthy.shutdown();
            }
        } finally {
            // restarted 在走 healthy 路径时已 shutdown，重复调用安全
            restarted.forwarding().shutdown();
        }
    }
}
