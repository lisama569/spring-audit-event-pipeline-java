package com.github.lisama569.springauditeventpipelinejava.model;

/**
 * 事件被拒绝的可区分原因。
 */
public enum RejectionReason {
    MISSING_EVENT_ID,
    MISSING_TIMESTAMP,
    INVALID_TIMESTAMP,
    MISSING_ACTOR,
    MISSING_RESOURCE,
    MISSING_ACTION
}
