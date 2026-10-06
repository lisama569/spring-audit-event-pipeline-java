package com.github.lisama569.springauditeventpipelinejava.recovery;

import java.time.Instant;
import java.util.List;

/**
 * 补投请求：二选一。
 * - 按业务时间范围（闭区间 [from, to]）；
 * - 或指定一批 eventId。
 */
public record ReplayRequest(Instant from, Instant to, List<String> eventIds) {

    public static ReplayRequest range(Instant from, Instant to) {
        return new ReplayRequest(from, to, null);
    }

    public static ReplayRequest ids(List<String> eventIds) {
        return new ReplayRequest(null, null, List.copyOf(eventIds));
    }

    public boolean isRange() {
        return from != null && to != null;
    }
}
