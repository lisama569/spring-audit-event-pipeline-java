package com.github.lisama569.springauditeventpipelinejava.forward;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceVerdict;
import com.github.lisama569.springauditeventpipelinejava.compliance.Violation;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * 将已落账事件追加转发到本地文件（JSON Lines）。
 * <ul>
 *   <li>启动时扫描文件中已存在的 eventId，重复投递直接跳过，目标端不产生重复数据；</li>
 *   <li>单 JVM 内 synchronized 串行写入 + flush，行级原子；</li>
 *   <li>{@link #failWhen(Predicate)} 为测试提供失败注入。</li>
 * </ul>
 */
public class LocalFileForwardTarget implements ForwardTarget {

    private static final Logger log = LoggerFactory.getLogger(LocalFileForwardTarget.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path file;
    private final Set<String> deliveredIds = new HashSet<>();

    private volatile Predicate<AuditEvent> failurePredicate = event -> false;

    public LocalFileForwardTarget(Path file) {
        this.file = file;
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            if (Files.notExists(file)) {
                Files.createFile(file);
            } else {
                recoverDeliveredIds();
            }
        } catch (IOException e) {
            throw new IllegalStateException("无法初始化转发目标文件: " + file, e);
        }
    }

    /** 注入失败条件（仅用于本地测试）。命中时下一次 deliver 抛异常。 */
    public void failWhen(Predicate<AuditEvent> predicate) {
        this.failurePredicate = predicate == null ? event -> false : predicate;
    }

    public synchronized boolean alreadyDelivered(String eventId) {
        return deliveredIds.contains(eventId);
    }

    @Override
    public synchronized void deliver(AuditEvent event, ComplianceVerdict verdict) throws Exception {
        if (deliveredIds.contains(event.eventId())) {
            log.debug("转发跳过（目标端已存在）: eventId={}", event.eventId());
            return;
        }
        if (failurePredicate.test(event)) {
            throw new IOException("注入的转发失败: eventId=" + event.eventId());
        }
        String line = MAPPER.writeValueAsString(toLine(event, verdict));
        Files.writeString(file, line + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.APPEND);
        deliveredIds.add(event.eventId());
        log.debug("转发成功: eventId={}", event.eventId());
    }

    private void recoverDeliveredIds() throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> row = MAPPER.readValue(line, Map.class);
                Object id = row.get("eventId");
                if (id != null) {
                    deliveredIds.add(id.toString());
                }
            }
        }
        log.info("转发目标恢复完成，已投递事件数={}", deliveredIds.size());
    }

    private Map<String, Object> toLine(AuditEvent event, ComplianceVerdict verdict) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("eventId", event.eventId());
        row.put("sequence", event.sequence());
        row.put("source", event.source());
        row.put("timestamp", event.timestamp().toString());
        row.put("actor", event.actor());
        row.put("resource", event.resource());
        row.put("action", event.action());
        row.put("attributes", event.attributes());
        row.put("ruleSetVersion", verdict.ruleSetVersion());
        row.put("compliant", verdict.compliant());
        List<Map<String, Object>> violations = new ArrayList<>();
        for (Violation v : verdict.violations()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ruleId", v.ruleId());
            m.put("ruleVersion", v.ruleVersion());
            m.put("severity", v.severity());
            m.put("explanation", v.explanation());
            violations.add(m);
        }
        row.put("violations", violations);
        return row;
    }
}
