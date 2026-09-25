package com.github.lisama569.springauditeventpipelinejava.compliance;

import com.github.lisama569.springauditeventpipelinejava.model.AuditEvent;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * 合规规则：对事件某字段做比较，命中即违规。
 * 规则文本形式： "ruleId:vN field OP value"，例如 "r1:v1 action == DELETE"。
 * 支持的字段：actor / resource / action / occurredAt(数值为 epochMilli) / 其它视为 attributes 键。
 */
public record ComplianceRule(String id, int version, String field, Operator operator, String value, String description) {

    public enum Operator {
        EQ, NE, CONTAINS, GT, LT, REGEX;

        static Operator fromToken(String token) {
            return switch (token) {
                case "==" -> EQ;
                case "!=" -> NE;
                case "~" -> CONTAINS;
                case ">" -> GT;
                case "<" -> LT;
                case "=~" -> REGEX;
                default -> throw new RuleParseException("未知操作符: " + token);
            };
        }
    }

    /**
     * 解析文本规则，失败抛 RuleParseException（显式失败，不跳过）。
     * 形式：ruleId:vN field OP value [ # 描述 ]
     */
    public static ComplianceRule parse(String spec) {
        if (spec == null || spec.isBlank()) {
            throw new RuleParseException("规则为空");
        }
        String description = "";
        String body = spec;
        int hash = spec.indexOf('#');
        if (hash >= 0) {
            description = spec.substring(hash + 1).trim();
            body = spec.substring(0, hash).trim();
        }
        String[] tokens = body.split("\\s+");
        if (tokens.length != 4) {
            throw new RuleParseException("规则须为 'ruleId:vN field OP value' 四段形式: " + spec);
        }
        String[] idVer = tokens[0].split(":v");
        if (idVer.length != 2 || idVer[0].isBlank()) {
            throw new RuleParseException("规则标识须为 ruleId:vN 形式: " + tokens[0]);
        }
        final int version;
        try {
            version = Integer.parseInt(idVer[1]);
        } catch (NumberFormatException e) {
            throw new RuleParseException("规则版本不是整数: " + tokens[0]);
        }
        if (version <= 0) {
            throw new RuleParseException("规则版本必须为正整数: " + tokens[0]);
        }
        Operator operator = Operator.fromToken(tokens[2]);
        String field = tokens[1];
        String value = tokens[3];
        if (operator == Operator.REGEX) {
            try {
                Pattern.compile(value);
            } catch (PatternSyntaxException e) {
                throw new RuleParseException("正则不可解析: " + value + " (" + e.getMessage() + ")");
            }
        }
        if ((operator == Operator.GT || operator == Operator.LT) && !isNumeric(value)) {
            throw new RuleParseException("数值比较操作符要求数值右值: " + value);
        }
        return new ComplianceRule(idVer[0], version, field, operator, value, description);
    }

    private static boolean isNumeric(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /** 提取事件字段值（缺失字段返回 null）。 */
    private String fieldValue(AuditEvent event) {
        return switch (field) {
            case "eventId" -> event.eventId();
            case "actor" -> event.actor();
            case "resource" -> event.resource();
            case "action" -> event.action();
            case "occurredAt" -> String.valueOf(event.occurredAt().toEpochMilli());
            default -> event.attributes() == null ? null : event.attributes().get(field);
        };
    }

    /** 判断事件是否命中该规则（命中 = 违规）。 */
    public boolean matches(AuditEvent event) {
        String actual = fieldValue(event);
        if (actual == null) {
            return false;
        }
        return switch (operator) {
            case EQ -> actual.equals(value);
            case NE -> !actual.equals(value);
            case CONTAINS -> actual.contains(value);
            case GT -> compareNumeric(actual) > 0;
            case LT -> compareNumeric(actual) < 0;
            case REGEX -> Pattern.matches(value, actual);
        };
    }

    private double compareNumeric(String actual) {
        final double a;
        try {
            a = Double.parseDouble(actual);
        } catch (NumberFormatException e) {
            // 非数值字段不参与大小比较，视为不命中
            return -1;
        }
        return Double.compare(a, Double.parseDouble(value));
    }

    /** 生成可解释的命中说明。 */
    public String explain(AuditEvent event) {
        String actual = fieldValue(event);
        return "规则 " + id + ":v" + version
                + (description.isEmpty() ? "" : " (" + description + ")")
                + " 要求 " + field + " " + operator + " " + value
                + "，实际值=" + actual
                + "，判定=" + (matches(event) ? "VIOLATION" : "PASS");
    }
}
