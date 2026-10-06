package com.github.lisama569.springauditeventpipelinejava.model;

import java.util.List;

/**
 * 补投请求：二选一。
 * - 指定 eventIds：只补投这批事件；
 * - 指定业务时间范围 [from, to)：补投该范围内的事件。
 * 两者都为空时视为非法请求。
 */
public record ReplayRequest(String from, String to, List<String> eventIds) {

    public boolean isRangeQuery() {
        return eventIds == null || eventIds.isEmpty();
    }
}
