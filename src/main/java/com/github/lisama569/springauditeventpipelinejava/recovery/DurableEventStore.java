package com.github.lisama569.springauditeventpipelinejava.recovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * 文件持久化事件存储。
 *
 * <p>布局（dataDir 下）：
 * <ul>
 *   <li>{@code events/events-yyyyMMddHH.jsonl}：按事件业务时间（UTC 小时）分桶的追加式 JSONL，
 *       同一事件永远落到同一桶；迟到事件追加到旧桶文件，读取时桶内排序消化乱序。</li>
 *   <li>{@code events/index.tsv}：{@code eventId⇥桶名} 追加索引，启动时载入内存（只存 id→桶令牌，
 *       不存事件体），支持按 id 直查所在桶。</li>
 * </ul>
 *
 * <p>每次追加都 {@code fsync}（事件行与索引行），确认受理前落盘，重启不丢。
 * 范围查询按桶顺序流式读取，一次只在内存持有一个桶的数据（按小时有界），
 * 补投跨度再长，内存占用也只与单桶规模相关，不随事件总量线性膨胀。
 */
public class DurableEventStore extends EventStore {

    private static final Logger log = LoggerFactory.getLogger(DurableEventStore.class);
    private static final DateTimeFormatter BUCKET_FMT = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneOffset.UTC);

    private final Path dir;
    private final Path eventsDir;
    private final Path indexFile;
    private final ObjectMapper mapper;
    /** eventId -> 桶名。仅紧凑索引，事件体不进内存。 */
    private final Map<String, String> index = new ConcurrentHashMap<>();
    /** 每个桶文件一把锁，追加与整桶读取互斥（读到不会是写了一半的行）。 */
    private final ConcurrentHashMap<String, Object> bucketLocks = new ConcurrentHashMap<>();
    private final Object indexLock = new Object();

    public DurableEventStore(Path dataDir) {
        try {
            this.dir = dataDir;
            this.eventsDir = dataDir.resolve("events");
            this.indexFile = eventsDir.resolve("index.tsv");
            Files.createDirectories(eventsDir);
            this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
            loadIndex();
        } catch (IOException e) {
            throw new UncheckedIOException("初始化持久化事件存储失败: " + dataDir, e);
        }
    }

    private void loadIndex() throws IOException {
        if (!Files.exists(indexFile)) {
            return;
        }
        try (BufferedReader reader = Files.newBufferedReader(indexFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                int tab = line.indexOf('\t');
                if (tab < 0) {
                    log.warn("忽略损坏的索引行: {}", line);
                    continue;
                }
                index.put(line.substring(0, tab), line.substring(tab + 1));
            }
        }
        log.info("持久化事件索引载入完成，已索引事件数={}", index.size());
    }

    private static String bucketOf(Instant occurredAt) {
        return "events-" + BUCKET_FMT.format(occurredAt.truncatedTo(java.time.temporal.ChronoUnit.HOURS)) + ".jsonl";
    }

    private Object lockFor(String bucket) {
        return bucketLocks.computeIfAbsent(bucket, k -> new Object());
    }

    @Override
    public void append(AuditEvent event) {
        String bucket = bucketOf(event.occurredAt());
        // 幂等：已索引（本次运行或重启后）的事件直接忽略
        if (index.containsKey(event.eventId())) {
            return;
        }
        String line;
        try {
            line = mapper.writeValueAsString(event);
        } catch (IOException e) {
            throw new UncheckedIOException("事件序列化失败: " + event.eventId(), e);
        }
        synchronized (lockFor(bucket)) {
            // 双检：同一 eventId 并发追加只能有一次落盘
            if (index.containsKey(event.eventId())) {
                return;
            }
            Path file = eventsDir.resolve(bucket);
            appendFsync(file, line);
            // 事件行落盘后再写索引；崩溃在两者之间只会留下不可见的孤立行，无害
            synchronized (indexLock) {
                appendFsync(indexFile, event.eventId() + "\t" + bucket);
                index.put(event.eventId(), bucket);
            }
        }
    }

    private static void appendFsync(Path file, String line) {
        try (var ch = java.nio.channels.FileChannel.open(file,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            byte[] bytes = (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                ch.write(buffer);
            }
            ch.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("持久化写入失败: " + file, e);
        }
    }

    @Override
    public Optional<AuditEvent> find(String eventId) {
        String bucket = index.get(eventId);
        if (bucket == null) {
            return Optional.empty();
        }
        synchronized (lockFor(bucket)) {
            for (AuditEvent event : readBucket(bucket)) {
                if (event.eventId().equals(eventId)) {
                    return Optional.of(event);
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public void forEach(Consumer<AuditEvent> consumer) {
        for (String bucket : listBuckets(null, null)) {
            synchronized (lockFor(bucket)) {
                for (AuditEvent event : readSortedBucket(bucket, null, null)) {
                    consumer.accept(event);
                }
            }
        }
    }

    @Override
    public void forEachInRange(Instant from, Instant to, Consumer<AuditEvent> consumer) {
        if (from.isAfter(to)) {
            throw new IllegalArgumentException("时间范围非法: from=" + from + " > to=" + to);
        }
        for (String bucket : listBuckets(from, to)) {
            synchronized (lockFor(bucket)) {
                // 一次只载入一个桶：内存按单桶（小时）规模有界
                for (AuditEvent event : readSortedBucket(bucket, from, to)) {
                    consumer.accept(event);
                }
            }
        }
    }

    @Override
    public void forEachIds(List<String> eventIds, Consumer<AuditEvent> consumer) {
        // id 列表由调用方指定，规模有界；找到后按 (occurredAt, ledgerSeq) 给出稳定顺序
        List<AuditEvent> hits = new ArrayList<>();
        Set<String> unique = new HashSet<>(eventIds);
        for (String id : unique) {
            find(id).ifPresent(hits::add);
        }
        hits.sort(Comparator.comparing(AuditEvent::occurredAt).thenComparingLong(AuditEvent::ledgerSeq));
        hits.forEach(consumer);
    }

    /**
     * 读取一个桶的全部事件。桶文件只在桶锁内被追加/读取，
     * 读到的最后一行要么完整、要么因崩溃写坏被跳过（写坏的行不会有索引）。
     */
    private List<AuditEvent> readBucket(String bucket) {
        Path file = eventsDir.resolve(bucket);
        if (!Files.exists(file)) {
            return List.of();
        }
        List<AuditEvent> result = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    result.add(mapper.readValue(line, AuditEvent.class));
                } catch (IOException e) {
                    // 仅允许尾部写坏的半行（崩溃残留），记日志后跳过
                    log.warn("忽略无法解析的事件行（可能为崩溃残留的半行）bucket={} line={}", bucket, line);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("读取桶失败: " + file, e);
        }
        return result;
    }

    private List<AuditEvent> readSortedBucket(String bucket, Instant from, Instant to) {
        List<AuditEvent> events = readBucket(bucket);
        List<AuditEvent> filtered = new ArrayList<>(events.size());
        for (AuditEvent event : events) {
            if (from != null && event.occurredAt().isBefore(from)) {
                continue;
            }
            if (to != null && event.occurredAt().isAfter(to)) {
                continue;
            }
            filtered.add(event);
        }
        // 桶内排序消化迟到追加；桶之间按时间不重叠，逐桶顺序处理即全局有序
        filtered.sort(Comparator.comparing(AuditEvent::occurredAt).thenComparingLong(AuditEvent::ledgerSeq));
        return filtered;
    }

    private List<String> listBuckets(Instant from, Instant to) {
        try (Stream<Path> files = Files.list(eventsDir)) {
            List<String> buckets = files
                    .map(p -> p.getFileName().toString())
                    .filter(name -> name.startsWith("events-") && name.endsWith(".jsonl"))
                    .sorted()
                    .toList();
            if (from == null || to == null) {
                return buckets;
            }
            String firstBucket = bucketOf(from);
            String lastBucket = bucketOf(to);
            List<String> covered = new ArrayList<>();
            for (String b : buckets) {
                if (b.compareTo(firstBucket) >= 0 && b.compareTo(lastBucket) <= 0) {
                    covered.add(b);
                }
            }
            return covered;
        } catch (IOException e) {
            throw new UncheckedIOException("列出事件桶失败", e);
        }
    }
}
