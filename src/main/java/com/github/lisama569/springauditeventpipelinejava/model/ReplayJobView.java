package com.github.lisama569.springauditeventpipelinejava.model;

/**
 * 补投任务进度视图。
 *
 * @param jobId       任务 id
 * @param status      RUNNING / COMPLETED / FAILED
 * @param matched     命中选择条件的事件数
 * @param delivered   本次补投新送达的事件数
 * @param already     补投前已送达而跳过的事件数
 * @param pending     仍在等待/重试中的事件数
 * @param dead        本次补投中耗尽重试进入死信的事件数
 * @param failed      尚未能成功的事件数（pending + dead）
 * @param error       任务级错误（可空）
 */
public record ReplayJobView(String jobId, String status, long matched, long delivered, long already,
                            long pending, long dead, long failed, String error) {
}
