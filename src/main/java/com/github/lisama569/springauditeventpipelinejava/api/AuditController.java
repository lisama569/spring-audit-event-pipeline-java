package com.github.lisama569.springauditeventpipelinejava.api;

import com.github.lisama569.springauditeventpipelinejava.api.dto.AuditEventRequest;
import com.github.lisama569.springauditeventpipelinejava.api.dto.ReplayRequestDto;
import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceRule;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardingService;
import com.github.lisama569.springauditeventpipelinejava.forward.LocalAuditSink;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditLedger;
import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;
import com.github.lisama569.springauditeventpipelinejava.model.DeliveryProgress;
import com.github.lisama569.springauditeventpipelinejava.model.DeliveryState;
import com.github.lisama569.springauditeventpipelinejava.model.IngestResult;
import com.github.lisama569.springauditeventpipelinejava.model.ReconciliationReport;
import com.github.lisama569.springauditeventpipelinejava.model.ReplayJobView;
import com.github.lisama569.springauditeventpipelinejava.model.ReplayRequest;
import com.github.lisama569.springauditeventpipelinejava.model.RuleVerdict;
import com.github.lisama569.springauditeventpipelinejava.pipeline.AuditPipeline;
import com.github.lisama569.springauditeventpipelinejava.recovery.Reconciler;
import com.github.lisama569.springauditeventpipelinejava.recovery.ReplayService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** REST 入口：事件提交、规则管理、结论、可靠转发、补投与对账查询。 */
@RestController
@RequestMapping("/api/audit")
public class AuditController {

    private final AuditPipeline pipeline;
    private final AuditLedger ledger;
    private final RuleRegistry registry;
    private final ForwardingService forwarding;
    private final LocalAuditSink sink;
    private final Reconciler reconciler;
    private final ReplayService replayService;

    public AuditController(AuditPipeline pipeline, AuditLedger ledger, RuleRegistry registry,
                           ForwardingService forwarding, LocalAuditSink sink,
                           Reconciler reconciler, ReplayService replayService) {
        this.pipeline = pipeline;
        this.ledger = ledger;
        this.registry = registry;
        this.forwarding = forwarding;
        this.sink = sink;
        this.reconciler = reconciler;
        this.replayService = replayService;
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

    // ---------- 可靠投递：进度与对账 ----------

    @GetMapping("/forward/status")
    public Map<String, Object> forwardStatus() {
        DeliveryProgress progress = reconciler.progress();
        return Map.of(
                "delivered", sink.deliveredEventIds(),
                "deadLetters", forwarding.deadLetterCount(),
                "progress", progress,
                "sinkRawAttempts", sink.rawAttemptCount());
    }

    /** 单事件投递状态：阶段（PENDING/DELIVERED/DEAD）、尝试次数、最近错误。 */
    @GetMapping("/delivery/events/{eventId}")
    public ResponseEntity<DeliveryState> deliveryState(@PathVariable String eventId) {
        DeliveryState state = reconciler.stateOf(eventId);
        return state == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(state);
    }

    /** 全量进度汇总。 */
    @GetMapping("/delivery/progress")
    public DeliveryProgress deliveryProgress() {
        return reconciler.progress();
    }

    /** 三方对账：账本 / 投递状态表 / 目标端 是否对平。 */
    @GetMapping("/delivery/reconcile")
    public ReconciliationReport reconcile() {
        return reconciler.reconcile();
    }

    // ---------- 补投 ----------

    /** 发起补投（按 [from,to) 业务时间范围或 eventIds），异步返回 jobId。 */
    @PostMapping("/replay")
    public ResponseEntity<?> replay(@RequestBody ReplayRequestDto dto) {
        try {
            ReplayRequest request = new ReplayRequest(dto.from(), dto.to(), dto.eventIds());
            String jobId = replayService.start(request);
            return ResponseEntity.accepted().body(Map.of("jobId", jobId));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    /** 查询补投任务进度。 */
    @GetMapping("/replay/{jobId}")
    public ResponseEntity<ReplayJobView> replayStatus(@PathVariable String jobId) {
        return replayService.status(jobId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
