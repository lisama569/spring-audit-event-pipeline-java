package com.github.lisama569.springauditeventpipelinejava.recovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.springauditeventpipelinejava.model.DeliveryState;
import com.github.lisama569.springauditeventpipelinejava.model.DeliveryStatus;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 基于追加日志 + 启动压缩的投递状态存储。
 * 每次状态变更追加一条 JSON（同一 eventId 后者覆盖前者），读取时以最新一条为准；
 * 构造时折叠为最新值并原子重写压缩，避免日志无限增长。
 */
public class DurableDeliveryStateStore implements DeliveryStateStore {

    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    private final DurableLog log;
    private final Map<String, DeliveryState> states = new LinkedHashMap<>();

    public DurableDeliveryStateStore(Path file) {
        this.log = new DurableLog(file);
        rebuildAndCompact();
    }

    private void rebuildAndCompact() {
        for (String line : log.readAll()) {
            Entry entry = parse(line);
            states.put(entry.eventId(), entry.state()); // 后写覆盖
        }
        if (!states.isEmpty()) {
            log.rewriteAll(states.entrySet().stream().map(e -> write(e.getKey(), e.getValue())).toList());
        }
    }

    @Override
    public synchronized void initIfAbsent(String eventId) {
        if (!states.containsKey(eventId)) {
            DeliveryState initial = new DeliveryState(DeliveryStatus.PENDING, 0, null, System.currentTimeMillis());
            states.put(eventId, initial);
            log.append(write(eventId, initial));
        }
    }

    @Override
    public synchronized void save(String eventId, DeliveryState state) {
        states.put(eventId, state);
        log.append(write(eventId, state));
    }

    @Override
    public synchronized Optional<DeliveryState> find(String eventId) {
        return Optional.ofNullable(states.get(eventId));
    }

    @Override
    public synchronized List<String> eventIdsByStatus(DeliveryStatus status) {
        List<String> ids = new ArrayList<>();
        states.forEach((id, state) -> {
            if (state.status() == status) {
                ids.add(id);
            }
        });
        return List.copyOf(ids);
    }

    @Override
    public synchronized List<Record> all() {
        List<Record> all = new ArrayList<>();
        states.forEach((id, state) -> all.add(new Record(id, state)));
        return List.copyOf(all);
    }

    private static String write(String eventId, DeliveryState state) {
        try {
            return MAPPER.writeValueAsString(new Entry(eventId, state));
        } catch (IOException e) {
            throw new UncheckedIOException("序列化投递状态失败", e);
        }
    }

    private static Entry parse(String line) {
        try {
            return MAPPER.readValue(line, Entry.class);
        } catch (IOException e) {
            throw new UncheckedIOException("反序列化投递状态失败: " + line, e);
        }
    }

    /** 持久化记录格式。 */
    private record Entry(String eventId, DeliveryState state) {
    }
}
