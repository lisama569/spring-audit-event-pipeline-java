package com.github.lisama569.springauditeventpipelinejava.recovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 基于追加日志的事件持久化存储。
 * - 事件以 JSON 行落盘（每次追加 fsync），重启可完整重建账本；
 * - 内存仅维护紧凑索引（eventId/时间/序号/日志偏移），不常驻事件体；
 * - 补投按 (occurredAt, ledgerSeq) 游标分页，配合有界在途窗口，
 *   使补投过程中常驻内存的事件数不随命中事件总量线性膨胀。
 */
public class DurableEventStore implements EventStore {

    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();
    private static final Comparator<IndexEntry> ORDER =
            Comparator.comparing(IndexEntry::occurredAt).thenComparingLong(IndexEntry::ledgerSeq);

    private final DurableLog log;
    private final Path file;

    /** 按业务顺序排序的紧凑索引（写少读多，变更以 synchronized 复制替换）。 */
    private List<IndexEntry> ordered = List.of();
    private final Map<String, IndexEntry> byId = new HashMap<>();

    public DurableEventStore(Path file) {
        this.file = file;
        this.log = new DurableLog(file);
        buildIndex();
    }

    private void buildIndex() {
        List<IndexEntry> entries = new ArrayList<>();
        Map<String, IndexEntry> ids = new HashMap<>();
        long offset = 0;
        for (String line : log.readAll()) {
            int byteSize = Integer.BYTES + line.getBytes(StandardCharsets.UTF_8).length;
            AuditEvent event = parse(line);
            IndexEntry entry = new IndexEntry(event.eventId(), event.occurredAt(),
                    event.ledgerSeq(), offset);
            entries.add(entry);
            ids.put(entry.eventId(), entry); // 追加日志中 eventId 唯一（落账去重保证）
            offset += byteSize;
        }
        entries.sort(ORDER);
        this.ordered = List.copyOf(entries);
        this.byId.clear();
        this.byId.putAll(ids);
    }

    @Override
    public synchronized void append(AuditEvent event) {
        String line = write(event);
        long offset;
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            offset = ch.size();
        } catch (IOException e) {
            throw new UncheckedIOException("读取事件日志大小失败", e);
        }
        log.append(line);
        IndexEntry entry = new IndexEntry(event.eventId(), event.occurredAt(), event.ledgerSeq(), offset);
        byId.put(event.eventId(), entry);
        List<IndexEntry> next = new ArrayList<>(ordered);
        next.add(entry);
        next.sort(ORDER);
        ordered = List.copyOf(next);
    }

    @Override
    public synchronized List<AuditEvent> loadAll() {
        return log.readAll().stream().map(DurableEventStore::parse).toList();
    }

    @Override
    public synchronized Optional<AuditEvent> find(String eventId) {
        IndexEntry entry = byId.get(eventId);
        return entry == null ? Optional.empty() : Optional.of(readAt(entry));
    }

    @Override
    public synchronized List<AuditEvent> pageRange(Instant from, Instant to, String fromExclusive, int limit) {
        List<AuditEvent> page = new ArrayList<>();
        boolean afterCursor = fromExclusive == null;
        for (IndexEntry entry : ordered) {
            if (!afterCursor) {
                if (entry.cursor().equals(fromExclusive)) {
                    afterCursor = true;
                }
                continue;
            }
            if (from != null && entry.occurredAt().isBefore(from)) {
                continue;
            }
            if (to != null && !entry.occurredAt().isBefore(to)) {
                break; // 已按 occurredAt 升序，越过右端点即可停止
            }
            page.add(readAt(entry));
            if (page.size() >= limit) {
                break;
            }
        }
        return List.copyOf(page);
    }

    @Override
    public synchronized List<AuditEvent> findByIds(List<String> eventIds) {
        List<AuditEvent> events = new ArrayList<>();
        for (String id : eventIds) {
            IndexEntry entry = byId.get(id);
            if (entry != null) {
                events.add(readAt(entry));
            }
        }
        events.sort(Comparator.comparing(AuditEvent::occurredAt)
                .thenComparingLong(AuditEvent::ledgerSeq));
        return List.copyOf(events);
    }

    @Override
    public synchronized long count() {
        return byId.size();
    }

    private AuditEvent readAt(IndexEntry entry) {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            ch.position(entry.offset());
            ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES);
            readFully(ch, sizeBuffer);
            int length = sizeBuffer.getInt(0);
            ByteBuffer payload = ByteBuffer.allocate(length);
            readFully(ch, payload);
            return parse(StandardCharsets.UTF_8.decode(payload.flip()).toString());
        } catch (IOException e) {
            throw new UncheckedIOException("按偏移读取事件失败: " + entry.eventId(), e);
        }
    }

    private static String write(AuditEvent event) {
        try {
            return MAPPER.writeValueAsString(event);
        } catch (IOException e) {
            throw new UncheckedIOException("序列化事件失败", e);
        }
    }

    private static AuditEvent parse(String line) {
        try {
            return MAPPER.readValue(line, AuditEvent.class);
        } catch (IOException e) {
            throw new UncheckedIOException("反序列化事件失败: " + line, e);
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) == -1) {
                throw new IOException("意外的日志尾");
            }
        }
    }

    /** 紧凑索引项：只含定位与排序所需字段，事件体留在磁盘。 */
    private record IndexEntry(String eventId, Instant occurredAt, long ledgerSeq, long offset) {
        String cursor() {
            return occurredAt.toEpochMilli() + ":" + ledgerSeq;
        }
    }
}
