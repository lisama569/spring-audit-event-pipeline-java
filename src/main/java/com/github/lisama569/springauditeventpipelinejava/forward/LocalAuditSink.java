package com.github.lisama569.springauditeventpipelinejava.forward;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地转发目标：幂等写入（按 eventId 去重），重复投递不产生重复数据。
 *
 * <p>两种模式：
 * <ul>
 *   <li>纯内存（{@link #LocalAuditSink()}）：适用于单测/无持久化部署；</li>
 *   <li>文件持久化（{@link #LocalAuditSink(Path)}）：已送达 id 以 JSONL 追加并 fsync，
 *       进程重启后目标端内容随之恢复。这样“进度记为已送达 ⇒ 目标端确有该记录”在
 *       整进程重启后仍然成立，对账不会错位，已送达事件也不会因重启被再写一遍。</li>
 * </ul>
 *
 * <p>崩溃一致性：投递时先把 id 落目标端文件（fsync），再由转发层落 DELIVERED 进度。
 * 最坏情况下崩溃于两步之间会产生一次多余的投递调用，但 id 已在目标端，幂等去重，
 * 最终仍是恰好一条。
 */
public class LocalAuditSink {

    private static final Logger log = LoggerFactory.getLogger(LocalAuditSink.class);

    private final Set<String> delivered = new LinkedHashSet<>();
    private final Set<String> alwaysFailing = new LinkedHashSet<>();
    /** 指定事件前 N 次投递失败（模拟该事件的瞬时故障），之后恢复。 */
    private final Map<String, Integer> failingFirstTimes = new java.util.HashMap<>();
    private final AtomicInteger injectedFailures = new AtomicInteger(0);

    private final Path deliveredFile; // null 表示纯内存模式
    private final ObjectMapper mapper = new ObjectMapper();

    public LocalAuditSink() {
        this.deliveredFile = null;
    }

    public LocalAuditSink(Path dataDir) {
        try {
            Path dir = dataDir.resolve("sink");
            Files.createDirectories(dir);
            this.deliveredFile = dir.resolve("delivered.jsonl");
            load();
        } catch (IOException e) {
            throw new UncheckedIOException("初始化本地目标失败: " + dataDir, e);
        }
    }

    private void load() throws IOException {
        if (deliveredFile == null || !Files.exists(deliveredFile)) {
            return;
        }
        int loaded = 0;
        try (BufferedReader reader = Files.newBufferedReader(deliveredFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    delivered.add(mapper.readValue(line, String.class));
                    loaded++;
                } catch (IOException e) {
                    // 仅尾部崩溃半行可能解析失败，该行未 fsync 完成，跳过安全
                    log.warn("忽略目标端损坏半行: {}", line);
                }
            }
        }
        log.info("本地目标恢复已送达记录 {} 条", loaded);
    }

    /**
     * 幂等投递。
     * @return true 成功（含已被去重的重复投递）；false 暂时失败，可重试。
     */
    public synchronized boolean deliver(AuditEvent event) {
        if (alwaysFailing.contains(event.eventId())) {
            return false;
        }
        Integer left = failingFirstTimes.get(event.eventId());
        if (left != null && left > 0) {
            failingFirstTimes.put(event.eventId(), left - 1);
            return false;
        }
        if (injectedFailures.getAndUpdate(c -> c > 0 ? c - 1 : 0) > 0) {
            return false;
        }
        if (delivered.contains(event.eventId())) {
            return true; // 幂等：重启后续投 / 并发重复调用都不产生新记录
        }
        if (deliveredFile != null) {
            persistId(event.eventId());
        }
        delivered.add(event.eventId());
        return true;
    }

    private void persistId(String eventId) {
        try (FileChannel ch = FileChannel.open(deliveredFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            byte[] bytes = (mapper.writeValueAsString(eventId) + System.lineSeparator())
                    .getBytes(StandardCharsets.UTF_8);
            ch.write(java.nio.ByteBuffer.wrap(bytes));
            ch.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("目标端持久化写入失败: " + eventId, e);
        }
    }

    public synchronized List<String> deliveredEventIds() {
        return List.copyOf(new ArrayList<>(delivered));
    }

    public synchronized int deliveredCount() {
        return delivered.size();
    }

    /** 测试用：注入接下来 count 次投递失败。 */
    public void failNextDeliveries(int count) {
        injectedFailures.addAndGet(count);
    }

    /** 测试用：指定事件的前 times 次投递失败，第 times+1 次起恢复成功。 */
    public synchronized void failFirstTimes(String eventId, int times) {
        failingFirstTimes.merge(eventId, times, Integer::sum);
    }

    /** 测试用：指定 eventId 的投递永远失败（模拟目标端持续性故障）。 */
    public synchronized void failAlways(String eventId) {
        alwaysFailing.add(eventId);
    }

    /** 测试用：目标端故障恢复，解除指定事件的永久失败标记。 */
    public synchronized void recover(String eventId) {
        alwaysFailing.remove(eventId);
    }
}
