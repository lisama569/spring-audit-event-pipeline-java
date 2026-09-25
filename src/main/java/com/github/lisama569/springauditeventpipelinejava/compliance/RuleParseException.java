package com.github.lisama569.springauditeventpipelinejava.compliance;

/**
 * 规则不可解析或规则集内部冲突时抛出：必须明确失败，禁止静默跳过。
 */
public class RuleParseException extends RuntimeException {
    public RuleParseException(String message) {
        super(message);
    }
}
