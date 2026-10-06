package com.github.lisama569.springauditeventpipelinejava.forward;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * 文件持久化投递进度存储。
 *
 * <p>dataDir/delivery/ 下：
 * <ul>
 *   <li>{@code state-snapshot.json}：全部进度的快照（原子临时文件 + 原子 move 替换）；</li>
 *   <li>{@code state-journal.jsonl}：快照之后的状态变更追加日志，每次写入 fsync。</li>
 * </ul>
 *
 * <p>启动恢复：先读快照，再顺序回放日志，后写覆盖前写，最终状态无丢失。
 * 运行期变更累计达到阈值（或优雅关闭）时执行压缩：全量内存表写新快照并清空日志，
 * 保证日志长度有界。所有读写均在同一把锁内串行，保证进度计数不重不漏。
 */
public class DurableDeliveryStateStore implements DeliveryStateStore {

    private static final Logger log = LoggerFactory.getLogger(DurableDeliveryStateStore.class);
    private static final int COMPACTION_THRESHOLD = 256;

    private final Path dir;
    private final Path snapshotFile;
    private final Path journalFile;
    private final ObjectMapper mapper;

    private final java.util.concurrent.ConcurrentHashMap<String, DeliveryState> states =
            new java.util.concurrent.ConcurrentHashMap<>();
    private final Object lock = new Object();
    private int changesSinceCompaction = 0;

    public DurableDeliveryStateStore(Path dataDir) {
        try {
            this.dir = dataDir.resolve("delivery");
            this.snapshotFile = dir.resolve("state-snapshot.json");
            this.journalFile = dir.resolve("state-journal.jsonl");
            Files.createDirectories(dir);
            this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
            loadSnapshot();
            replayJournal();
        } catch (IOException e) {
            throw new UncheckedIOException("初始化投递进度存储失败: " + dataDir, e);
        }
    }

    @SuppressWarnings("unchecked")
    private void loadSnapshot() throws IOException {
        if (!Files.exists(snapshotFile)) {
            return;
        }
        List<DeliveryState> snapshot = mapper.readValue(
                Files.readString(snapshotFile, StandardCharsets.UTF_8),
                mapper.getTypeFactory().constructCollectionType(List.class, DeliveryState.class));
        for (DeliveryState state : snapshot) {
            states.put(state.eventId(), state);
        }
        log.info("投递进度快照载入完成，记录数={}", states.size());
    }

    private void replayJournal() throws IOException {
        if (!Files.exists(journalFile)) {
            return;
        }
        int replayed = 0;
        try (BufferedReader reader = Files.newBufferedReader(journalFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    DeliveryState state = mapper.readValue(line, DeliveryState.class);
                    states.put(state.eventId(), state);
                    replayed++;
                } catch (IOException e) {
                    // 只可能是日志尾部崩溃写坏的半行；它未 fsync 成功，丢弃安全
                    log.warn("忽略损坏的进度日志半行: {}", line);
                }
            }
        }
        log.info("投递进度日志回放完成，回放变更={}，当前记录数={}", replayed, states.size());
    }

    @Override
    public void registerIfAbsent(String eventId) {
        synchronized (lock) {
            if (states.containsKey(eventId)) {
                return;
            }
            persist(new DeliveryState(eventId, DeliveryStatus.PENDING, 0, System.currentTimeMillis()));
        }
    }

    @Override
    public Optional<DeliveryState> find(String eventId) {
        return Optional.ofNullable(states.get(eventId));
    }

    @Override
    public void save(DeliveryState state) {
        synchronized (lock) {
            persist(state);
        }
    }

    private void persist(DeliveryState state) {
        // 调用方已持锁
        String line;
        try {
            line = mapper.writeValueAsString(state);
        } catch (IOException e) {
            throw new UncheckedIOException("进度状态序列化失败: " + state.eventId(), e);
        }
        try (FileChannel ch = FileChannel.open(journalFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            byte[] bytes = (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
            ch.write(java.nio.ByteBuffer.wrap(bytes));
            ch.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("进度日志写入失败", e);
        }
        states.put(state.eventId(), state);
        changesSinceCompaction++;
        if (changesSinceCompaction >= COMPACTION_THRESHOLD) {
            compact();
        }
    }

    /** 全量内存表 -> 新快照（原子替换）-> 清空日志。调用方已持锁。 */
    private void compact() {
        try {
            List<DeliveryState> all = new ArrayList<>(states.values());
            all.sort(Comparator.comparing(DeliveryState::eventId));
            Path tmp = dir.resolve("state-snapshot.tmp-" + Thread.currentThread().threadId());
            Files.writeString(tmp, mapper.writeValueAsString(all), StandardCharsets.UTF_8);
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                ch.force(true);
            }
            Files.move(tmp, snapshotFile,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            try (FileChannel ch = FileChannel.open(journalFile,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                ch.force(true);
            }
            changesSinceCompaction = 0;
            log.debug("投递进度压缩完成，记录数={}", all.size());
        } catch (IOException e) {
            // 压缩失败不影响正确性（日志仍可回放），下次变更时再尝试
            log.warn("投递进度压缩失败，将依赖日志恢复", e);
        }
    }

    @Override
    public List<DeliveryState> all() {
        return List.copyOf(states.values());
    }

    @Override
    public List<String> pendingEventIds() {
        List<String> result = new ArrayList<>();
        for (DeliveryState s : states.values()) {
            if (s.status() == DeliveryStatus.PENDING || s.status() == DeliveryStatus.RETRYING) {
                result.add(s.eventId());
            }
        }
        return List.copyOf(result);
    }

    /** 优雅关闭：先压缩，缩短下次启动的恢复路径。 */
    public void close() {
        synchronized (lock) {
            compact();
        }
    }
}
