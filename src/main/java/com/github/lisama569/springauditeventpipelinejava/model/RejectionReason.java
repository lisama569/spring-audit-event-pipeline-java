package com.github.lisama569.springauditeventpipelinejava.model;

/** 可区分的采集拒绝原因。 */
public enum RejectionReason {
    MISSING_EVENT_ID,
    MISSING_TIMESTAMP,
    INVALID_TIMESTAMP,
    MISSING_ACTOR,
    MISSING_RESOURCE,
    MISSING_ACTION,
    INVALID_PAYLOAD
}
