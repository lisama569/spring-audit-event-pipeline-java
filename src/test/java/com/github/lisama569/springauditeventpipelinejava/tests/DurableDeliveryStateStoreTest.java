package com.github.lisama569.springauditeventpipelinejava.tests;

import com.github.lisama569.springauditeventpipelinejava.forward.DeliveryState;
import com.github.lisama569.springauditeventpipelinejava.forward.DeliveryStatus;
import com.github.lisama569.springauditeventpipelinejava.forward.DurableDeliveryStateStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 投递进度存储：重复登记幂等、状态变更跨重开可见、压缩（快照）后仍可完整恢复。 */
class DurableDeliveryStateStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void stateChangesSurviveReopenAndCompaction() {
        DurableDeliveryStateStore store = new DurableDeliveryStateStore(tempDir);
        store.registerIfAbsent("e1");
        store.registerIfAbsent("e1"); // 幂等
        DeliveryState initial = store.find("e1").orElseThrow();
        assertEquals(DeliveryStatus.PENDING, initial.status());
        assertEquals(0, initial.attempts());

        store.save(initial.withStatus(DeliveryStatus.RETRYING, 2));
        store.registerIfAbsent("e2");
        store.save(store.find("e2").orElseThrow().withStatus(DeliveryStatus.DELIVERED, 1));
        // 超过压缩阈值，触发快照 + 日志截断
        for (int i = 0; i < 300; i++) {
            String id = "bulk-" + i;
            store.registerIfAbsent(id);
            store.save(store.find(id).orElseThrow().withStatus(DeliveryStatus.DELIVERED, 1));
        }
        assertEquals(1, store.find("e1").orElseThrow().attempts() == 2 ? 1 : 0);
        store.close();

        DurableDeliveryStateStore reopened = new DurableDeliveryStateStore(tempDir);
        DeliveryState e1 = reopened.find("e1").orElseThrow();
        DeliveryState e2 = reopened.find("e2").orElseThrow();
        assertEquals(DeliveryStatus.RETRYING, e1.status());
        assertEquals(2, e1.attempts());
        assertEquals(DeliveryStatus.DELIVERED, e2.status());
        assertEquals(302, reopened.all().size(), "压缩前后所有事件的最终状态都必须可恢复");
        assertTrue(reopened.pendingEventIds().contains("e1"));
        assertEquals(301, reopened.all().stream()
                .filter(s -> s.status() == DeliveryStatus.DELIVERED).count());
        reopened.close();
    }
}
