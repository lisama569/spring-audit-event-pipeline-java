package com.github.lisama569.springauditeventpipelinejava.api;

import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceVerdict;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleParseException;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.compliance.VerdictRepository;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardService;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditEventStore;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditIngestService;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.model.IngestResult;
import com.github.lisama569.springauditeventpipelinejava.model.RawAuditEvent;
import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceRule;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * HTTP 入口：事件采集、规则演进、账本与判定结论查询、转发状态。
 */
@RestController
@RequestMapping("/api")
public class AuditController {

    private final AuditIngestService ingestService;
    private final AuditEventStore store;
    private final VerdictRepository verdictRepository;
    private final RuleRegistry ruleRegistry;
    private final ForwardService forwardService;

    public AuditController(AuditIngestService ingestService,
                           AuditEventStore store,
                           VerdictRepository verdictRepository,
                           RuleRegistry ruleRegistry,
                           ForwardService forwardService) {
        this.ingestService = ingestService;
        this.store = store;
        this.verdictRepository = verdictRepository;
        this.ruleRegistry = ruleRegistry;
        this.forwardService = forwardService;
    }

    /** 采集事件：201 接受 / 200 重复幂等 / 422 带可区分原因拒绝。 */
    @PostMapping("/events")
    public ResponseEntity<Map<String, Object>> ingest(@RequestBody RawAuditEvent raw) {
        IngestResult result = ingestService.ingest(raw);
        Map<String, Object> body = switch (result.status()) {
            case ACCEPTED -> Map.of("status", "ACCEPTED",
                    "eventId", result.event().eventId(),
                    "sequence", result.event().sequence());
            case DUPLICATE -> Map.of("status", "DUPLICATE",
                    "eventId", result.event().eventId(),
                    "sequence", result.event().sequence());
            case REJECTED -> Map.of("status", "REJECTED",
                    "reason", result.rejectionReason().name());
        };
        HttpStatus status = switch (result.status()) {
            case ACCEPTED -> HttpStatus.CREATED;
            case DUPLICATE -> HttpStatus.OK;
            case REJECTED -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
        return ResponseEntity.status(status).body(body);
    }

    /** 规则集整体更新：任一规则不可解析/冲突则 400 且旧规则集继续生效。 */
    @PostMapping("/rules")
    public ResponseEntity<Map<String, Object>> replaceRules(@RequestBody List<ComplianceRule> rules) {
        RuleRegistry.Snapshot snapshot = ruleRegistry.replaceRules(rules);
        return ResponseEntity.ok(Map.of(
                "ruleSetVersion", snapshot.version(),
                "ruleCount", snapshot.rules().size()));
    }

    @GetMapping("/rules")
    public RuleRegistry.Snapshot currentRules() {
        return ruleRegistry.current();
    }

    @GetMapping("/events")
    public List<AuditEvent> events() {
        return store.findAll();
    }

    @GetMapping("/events/{eventId}")
    public ResponseEntity<AuditEvent> event(@PathVariable String eventId) {
        return store.find(eventId).map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/verdicts")
    public List<ComplianceVerdict> verdicts() {
        return verdictRepository.findAll();
    }

    @GetMapping("/verdicts/{eventId}")
    public ResponseEntity<ComplianceVerdict> verdict(@PathVariable String eventId) {
        return verdictRepository.find(eventId).map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/forward/status")
    public Map<String, Object> forwardStatus() {
        return Map.of("deadLetterCount", forwardService.deadLetterCount());
    }

    @ExceptionHandler(RuleParseException.class)
    public ResponseEntity<Map<String, Object>> onRuleParse(RuleParseException e) {
        return ResponseEntity.badRequest().body(Map.of(
                "status", "RULES_REJECTED",
                "reason", e.getMessage()));
    }
}
