package com.github.lisama569.springauditeventpipelinejava.forward;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** 进程内投递进度存储（原有语义，无重启持久化，用于兼容既有测试与无持久化部署）。 */
public class InMemoryDeliveryStateStore implements DeliveryStateStore {

    private final ConcurrentHashMap<String, DeliveryState> states = new ConcurrentHashMap<>();

    @Override
    public void registerIfAbsent(String eventId) {
        states.computeIfAbsent(eventId,
                id -> new DeliveryState(id, DeliveryStatus.PENDING, 0, System.currentTimeMillis()));
    }

    @Override
    public Optional<DeliveryState> find(String eventId) {
        return Optional.ofNullable(states.get(eventId));
    }

    @Override
    public void save(DeliveryState state) {
        states.put(state.eventId(), state);
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
}
