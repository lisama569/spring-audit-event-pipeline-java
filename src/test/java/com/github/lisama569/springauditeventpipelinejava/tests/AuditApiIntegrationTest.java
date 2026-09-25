package com.github.lisama569.springauditeventpipelinejava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "audit.forward.file=build/test-forward-api.jsonl")
@AutoConfigureMockMvc
class AuditApiIntegrationTest {

    @Autowired
    private MockMvc mvc;

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void httpEntrypointIngestDuplicateRejectAndRules() throws Exception {
        Map<String, Object> valid = Map.of(
                "source", "http",
                "eventId", "api-evt-1",
                "timestamp", "2026-09-25T10:00:00Z",
                "actor", "alice",
                "resource", "db/customers",
                "action", "delete",
                "attributes", Map.of("ip", "10.0.0.1"));

        mvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(valid)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.sequence").isNumber());

        // 重复提交 -> 200 DUPLICATE，不产生新记录
        mvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(valid)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DUPLICATE"));

        // 非法时间戳 -> 422 + 可区分拒绝原因
        Map<String, Object> badTime = new java.util.HashMap<>(valid);
        badTime.put("eventId", "api-evt-2");
        badTime.put("timestamp", "yesterday");
        mvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(badTime)))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.status").value("REJECTED"))
                .andExpect(jsonPath("$.reason").value("INVALID_TIMESTAMP"));

        // 缺操作者 -> 422 + 另一拒绝原因
        Map<String, Object> noActor = new java.util.HashMap<>(valid);
        noActor.put("eventId", "api-evt-3");
        noActor.remove("actor");
        mvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(noActor)))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("MISSING_ACTOR"));

        // 查询账本：重复提交不产生第二条
        mvc.perform(get("/api/events"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].eventId").value("api-evt-1"));

        // 初始无规则 -> 合规；随后整体下发规则集
        mvc.perform(get("/api/verdicts/api-evt-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.compliant").value(true))
                .andExpect(jsonPath("$.ruleSetVersion").value(0));

        List<Map<String, Object>> rules = List.of(Map.of(
                "ruleId", "R-DEL",
                "version", 1,
                "description", "禁止删除",
                "actorPattern", "*",
                "resourcePattern", "*",
                "actionPattern", "delete",
                "severity", "HIGH"));
        mvc.perform(post("/api/rules")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(rules)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ruleSetVersion").value(1));

        // 坏规则（重复 ruleId 冲突）-> 400，旧规则集保留
        List<Map<String, Object>> conflicting = List.of(
                Map.of("ruleId", "X", "version", 1, "description", "a",
                        "actorPattern", "*", "resourcePattern", "*", "actionPattern", "read", "severity", "LOW"),
                Map.of("ruleId", "X", "version", 1, "description", "b",
                        "actorPattern", "*", "resourcePattern", "*", "actionPattern", "write", "severity", "LOW"));
        mvc.perform(post("/api/rules")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(conflicting)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value("RULES_REJECTED"));

        // 规则演进后新事件按新规则判定为违规
        Map<String, Object> newEvent = new java.util.HashMap<>(valid);
        newEvent.put("eventId", "api-evt-4");
        newEvent.put("timestamp", "2026-09-25T11:00:00Z");
        mvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(newEvent)))
                .andExpect(status().isCreated());
        mvc.perform(get("/api/verdicts/api-evt-4"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.compliant").value(false))
                .andExpect(jsonPath("$.ruleSetVersion").value(1))
                .andExpect(jsonPath("$.violations[0].ruleId").value("R-DEL"))
                .andExpect(jsonPath("$.violations[0].explanation").isNotEmpty());
    }
}
