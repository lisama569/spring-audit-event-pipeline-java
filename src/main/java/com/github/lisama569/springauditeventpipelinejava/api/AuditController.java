package com.github.lisama569.springauditeventpipelinejava.api;

import com.github.lisama569.springauditeventpipelinejava.api.dto.AuditEventRequest;
import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceRule;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardingService;
import com.github.lisama569.springauditeventpipelinejava.forward.LocalAuditSink;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditLedger;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.model.IngestResult;
import com.github.lisama569.springauditeventpipelinejava.model.RuleVerdict;
import com.github.lisama569.springauditeventpipelinejava.pipeline.AuditPipeline;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** REST 入口：事件提交、规则管理、结论与转发状态查询。 */
@RestController
@RequestMapping("/api/audit")
public class AuditController {

    private final AuditPipeline pipeline;
    private final AuditLedger ledger;
    private final RuleRegistry registry;
    private final ForwardingService forwarding;
    private final LocalAuditSink sink;

    public AuditController(AuditPipeline pipeline, AuditLedger ledger, RuleRegistry registry,
                           ForwardingService forwarding, LocalAuditSink sink) {
        this.pipeline = pipeline;
        this.ledger = ledger;
        this.registry = registry;
        this.forwarding = forwarding;
        this.sink = sink;
    }

    @PostMapping("/events")
    public ResponseEntity<?> submit(@RequestBody AuditEventRequest request) {
        IngestResult result = pipeline.submit(request);
        return switch (result.status()) {
            case ACCEPTED -> ResponseEntity.ok(result);
            case DUPLICATE -> ResponseEntity.status(409).body(result);
            case REJECTED -> ResponseEntity.unprocessableEntity().body(result);
        };
    }

    @PostMapping("/rules")
    public ResponseEntity<?> registerRule(@RequestBody Map<String, String> body) {
        ComplianceRule rule = ComplianceRule.parse(body.get("spec"));
        registry.register(rule);
        return ResponseEntity.ok(rule);
    }

    @GetMapping("/events")
    public List<AuditEvent> events() {
        return ledger.snapshotOrdered();
    }

    @GetMapping("/verdicts/{eventId}")
    public List<RuleVerdict> verdicts(@PathVariable String eventId) {
        return pipeline.verdictsOf(eventId);
    }

    @GetMapping("/forward/status")
    public Map<String, Object> forwardStatus() {
        return Map.of(
                "delivered", sink.deliveredEventIds(),
                "deadLetters", forwarding.deadLetterCount());
    }
}
