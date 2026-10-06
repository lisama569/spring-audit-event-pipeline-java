package com.github.lisama569.springauditeventpipelinejava.api;

import com.github.lisama569.springauditeventpipelinejava.api.dto.AuditEventRequest;
import com.github.lisama569.springauditeventpipelinejava.compliance.ComplianceRule;
import com.github.lisama569.springauditeventpipelinejava.compliance.RuleRegistry;
import com.github.lisama569.springauditeventpipelinejava.forward.DeliveryReport;
import com.github.lisama569.springauditeventpipelinejava.forward.DeliveryState;
import com.github.lisama569.springauditeventpipelinejava.forward.ForwardingService;
import com.github.lisama569.springauditeventpipelinejava.forward.LocalAuditSink;
import com.github.lisama569.springauditeventpipelinejava.ingest.AuditLedger;
import com.github.lisama569.springauditeventpipelinejava.model.IngestResult;
import com.github.lisama569.springauditeventpipelinejava.model.RuleVerdict;
import com.github.lisama569.springauditeventpipelinejava.pipeline.AuditPipeline;
import com.github.lisama569.springauditeventpipelinejava.recovery.ReplayJob;
import com.github.lisama569.springauditeventpipelinejava.recovery.ReplayRequest;
import com.github.lisama569.springauditeventpipelinejava.recovery.ReplayService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** REST 入口：事件提交、规则管理、结论、转发状态、投递对账与补投。 */
@RestController
@RequestMapping("/api/audit")
public class AuditController {

    private final AuditPipeline pipeline;
    private final AuditLedger ledger;
    private final RuleRegistry registry;
    private final ForwardingService forwarding;
    private final LocalAuditSink sink;
    private final ReplayService replayService;

    public AuditController(AuditPipeline pipeline, AuditLedger ledger, RuleRegistry registry,
                           ForwardingService forwarding, LocalAuditSink sink, ReplayService replayService) {
        this.pipeline = pipeline;
        this.ledger = ledger;
        this.registry = registry;
        this.forwarding = forwarding;
        this.sink = sink;
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
    public List<com.github.lisama569.springauditeventpipelinejava.model.AuditEvent> events() {
        return ledger.snapshotOrdered();
    }

    @GetMapping("/verdicts/{eventId}")
    public List<RuleVerdict> verdicts(@PathVariable String eventId) {
        return pipeline.verdictsOf(eventId);
    }

    /** 旧版转发状态：保持原有 delivered/deadLetters 字段不变（向后兼容）。 */
    @GetMapping("/forward/status")
    public Map<String, Object> forwardStatus() {
        return Map.of(
                "delivered", sink.deliveredEventIds(),
                "deadLetters", forwarding.deadLetterCount());
    }

    /** 单个事件的投递进度：状态（已送达/等待重试/死信）与尝试次数。 */
    @GetMapping("/forward/states/{eventId}")
    public ResponseEntity<DeliveryState> stateOf(@PathVariable String eventId) {
        return forwarding.stateOf(eventId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** 全部投递进度明细。 */
    @GetMapping("/forward/states")
    public List<DeliveryState> states() {
        return forwarding.states().stream()
                .sorted(java.util.Comparator.comparing(DeliveryState::eventId))
                .toList();
    }

    /** 对账报告：进度统计与目标端实际内容交叉核对（consistent=true 表示不重不漏）。 */
    @GetMapping("/forward/reconcile")
    public DeliveryReport reconcile() {
        return forwarding.reconcile();
    }

    /**
     * 发起补投。body：
     * {"from":"2026-09-01T00:00:00Z","to":"2026-09-30T23:59:59Z"} 按业务时间闭区间；
     * {"eventIds":["e1","e2"]} 按指定事件集合。
     * 立即返回任务句柄，用 /replays/{jobId} 轮询进度。
     */
    @PostMapping("/replays")
    public ResponseEntity<?> startReplay(@RequestBody Map<String, Object> body) {
        ReplayRequest request;
        try {
            Object fromRaw = body.get("from");
            Object toRaw = body.get("to");
            Object idsRaw = body.get("eventIds");
            if (fromRaw != null && toRaw != null) {
                request = ReplayRequest.range(Instant.parse(fromRaw.toString()), Instant.parse(toRaw.toString()));
            } else if (idsRaw instanceof List<?> list) {
                List<String> ids = list.stream().map(Object::toString).toList();
                if (ids.isEmpty()) {
                    return ResponseEntity.badRequest().body(Map.of("error", "eventIds 不能为空"));
                }
                request = ReplayRequest.ids(ids);
            } else {
                return ResponseEntity.badRequest()
                        .body(Map.of("error", "必须提供 from+to（业务时间闭区间）或 eventIds"));
            }
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", "请求解析失败: " + e.getMessage()));
        }
        ReplayJob job = replayService.start(request);
        return ResponseEntity.accepted().body(jobView(job));
    }

    @GetMapping("/replays/{jobId}")
    public ResponseEntity<Map<String, Object>> replayJob(@PathVariable String jobId) {
        return replayService.job(jobId)
                .map(job -> ResponseEntity.ok(jobView(job)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/replays")
    public List<Map<String, Object>> replays() {
        return replayService.jobs().stream().map(AuditController::jobView).toList();
    }

    private static Map<String, Object> jobView(ReplayJob job) {
        return Map.of(
                "jobId", job.jobId(),
                "status", job.status(),
                "scanned", job.scanned(),
                "submitted", job.submitted(),
                "skippedDelivered", job.skippedDelivered(),
                "error", job.error() == null ? "" : job.error());
    }
}
