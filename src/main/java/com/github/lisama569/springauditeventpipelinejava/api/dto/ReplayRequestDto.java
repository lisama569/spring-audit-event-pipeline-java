package com.github.lisama569.springauditeventpipelinejava.api.dto;

import java.util.List;

/** 补投 REST 请求体：from/to 为 ISO-8601 时间（[from, to)），或给定 eventIds。 */
public record ReplayRequestDto(String from, String to, List<String> eventIds) {
}
