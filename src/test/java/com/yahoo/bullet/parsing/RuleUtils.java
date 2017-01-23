/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.operations.AggregationOperations.AggregationType;
import com.yahoo.bullet.operations.FilterOperations.FilterType;
import com.yahoo.bullet.operations.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.operations.typesystem.Type;
import com.yahoo.bullet.tracing.AggregationRule;
import com.yahoo.bullet.tracing.FilterRule;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**.
 * This class deliberately doesn't use GSON to make JSON in order to avoid any GSON specific changes or
 * misconfiguration (new fields added etc.) in the main code and tries to emulate what the parser will get from an
 * external client making an actual call.
 */
public class RuleUtils {
    @SafeVarargs
    public static String makeGroupFilterRule(String field, List<String> values, FilterType operation,
                                             AggregationType aggregation, Integer size,
                                             List<GroupOperation> operations, Pair<String, String>... fields) {
        return "{" +
                "'filters' : [" + makeFilter(field, values, operation) + "], " +
                "'aggregation' : " + makeGroupAggregation(size, aggregation, operations, fields) +
                "}";
    }

    @SafeVarargs
    public static String makeGroupFilterRule(List<Clause> clauses, FilterType operation,
                                             AggregationType aggregation, Integer size,
                                             List<GroupOperation> operations, Pair<String, String>... fields) {
        return "{" +
                "'filters' : [" + makeFilter(clauses, operation) + "], " +
                "'aggregation' : " + makeGroupAggregation(size, aggregation, operations, fields) +
                "}";
    }

    @SafeVarargs
    public static String makeRawFullRule(String field, List<String> values, FilterType operation,
                                         AggregationType aggregation, Integer size,
                                         Pair<String, String>... projections) {
        return "{" +
               "'filters' : [" + makeFilter(field, values, operation) + "], " +
               "'projection' : " + makeProjections(projections) + ", " +
               "'aggregation' : " + makeSimpleAggregation(size, aggregation) +
               "}";
    }

    @SafeVarargs
    public static String makeRawFullRule(List<Clause> clauses, FilterType operation,
                                         AggregationType aggregation, Integer size,
                                         Pair<String, String>... projections) {
        return "{" +
               "'filters' : [" + makeFilter(clauses, operation) + "], " +
               "'projection' : " + makeProjections(projections) + ", " +
               "'aggregation' : " + makeSimpleAggregation(size, aggregation) +
               "}";
    }

    @SafeVarargs
    public static String makeProjectionFilterRule(String field, List<String> values, FilterType operation,
                                                  Pair<String, String>... projections) {
        return "{" +
               "'filters' : [" + makeFilter(field, values, operation) + "], " +
               "'projection': " + makeProjections(projections) +
               "}";
    }

    @SafeVarargs
    public static String makeProjectionFilterRule(List<Clause> clauses, FilterType operation,
                                                  Pair<String, String>... projections) {
        return "{" +
               "'filters' : [" + makeFilter(clauses, operation) + "], " +
               "'projection': " + makeProjections(projections) +
               "}";
    }

    public static String makeSimpleAggregationFilterRule(String field, List<String> values, FilterType operation,
                                                         AggregationType aggregation, Integer size) {
        return "{" +
               "'filters' : [" + makeFilter(field, values, operation) + "], " +
               "'aggregation' : " + makeSimpleAggregation(size, aggregation) +
               "}";
    }

    public static String makeSimpleAggregationFilterRule(List<Clause> clauses, FilterType operation,
                                                   AggregationType aggregation, Integer size) {
        return "{" +
               "'filters' : [" + makeFilter(clauses, operation) + "], " +
               "'aggregation' : " + makeSimpleAggregation(size, aggregation) +
               "}";
    }

    public static String makeFilterRule(String field, List<String> values, FilterType operation) {
        return "{'filters' : [" + makeFilter(field, values, operation) + "]}";
    }

    public static String makeFilterRule(List<Clause> values, FilterType operation) {
        return "{'filters' : [" + makeFilter(values, operation) + "]}";
    }

    public static String makeFilterRule(FilterType operation, Clause... values) {
        return "{'filters': [" + makeFilter(operation, values) + "]}";
    }

    public static String makeFieldFilterRule(String value) {
        return makeFilterRule("field", Collections.singletonList(value), FilterType.EQUALS);
    }

    @SafeVarargs
    public static String makeProjectionRule(Pair<String, String>... projections) {
        return "{'projection' : " + makeProjections(projections) + "}";
    }

    public static String makeAggregationRule(AggregationType operation, Integer size) {
        return "{'aggregation' : " + makeSimpleAggregation(size, operation) + "}";
    }

    @SafeVarargs
    public static String makeAggregationRule(AggregationType operation, Integer size, Map<String, String> attributes,
                                             Pair<String, String>... fields) {
        return "{'aggregation' : " + makeStringAttributesAggregation(size, operation, attributes, fields) + "}";
    }

    public static String makeFilter(String field, List<String> values, FilterType operation) {
        return "{" +
                "'field' : " + makeString(field) + ", " +
                "'operation' : " + makeString(getOperationFor(operation)) + ", " +
                "'values' : ['" + values.stream().reduce((a, b) -> a + "' , '" + b).orElse("") + "']" +
                "}";
    }

    public static String makeFilter(List<Clause> values, FilterType operation) {
        return "{" +
               "'operation' : " + makeString(getOperationFor(operation)) + ", " +
               "'clauses' : [" + values.stream().map(RuleUtils::toString).reduce((a, b) -> a + " , " + b).orElse("") + "]" +
               "}";
    }

    public static String makeFilter(FilterType operation, Clause... values) {
        return makeFilter(values == null ? Collections.emptyList() : Arrays.asList(values), operation);
    }

    @SafeVarargs
    public static String makeProjections(Pair<String, String>... pairs) {
        return "{'fields' : " + makeMap(pairs) + "}";
    }

    public static String makeSimpleAggregation(Integer size, AggregationType operation) {
        return "{'type' : '" + getOperationFor(operation) + "', 'size' : " + size + "}";
    }

    @SafeVarargs
    public static String makeGroupAggregation(Integer size, AggregationType operation, List<GroupOperation> operations,
                                              Pair<String, String>... fields) {
        return "{" +
                 "'type' : '" + getOperationFor(operation) + "', " +
                 "'fields' : " + makeGroupFields(fields) + ", " +
                 "'attributes' : {" +
                    "'operations' : [" +
                        operations.stream().map(RuleUtils::makeGroupOperation).reduce((a, b) -> a + " , " + b).orElse("") +
                    "]" +
                 "}, " +
                 "'size' : " + size +
               "}";
    }

    @SafeVarargs
    public static String makeStringAttributesAggregation(Integer size, AggregationType operation,
                                                         Map<String, String> attributes,
                                                         Pair<String, String>... fields) {
        return "{" +
                 "'type' : '" + getOperationFor(operation) + "', " +
                 "'fields' : " + makeGroupFields(fields) + ", " +
                 "'size' : " + size + ", " +
                 "'attributes' : " + makeMap(attributes) +
                "}";
    }

    @SafeVarargs
    public static String makeGroupFields(Pair<String, String>... fields) {
        if (fields == null) {
            return "null";
        }
        return makeMap(fields);

    }

    public static String makeGroupOperation(GroupOperation operation) {
        return "{" +
                 "'type' : " + makeString(operation.getType().getName()) + ", " +
                 "'field' : " + makeString(operation.getField()) + ", " +
                 "'newName' : " + makeString(operation.getNewName()) +
                "}";
    }

    public static String makeString(String field) {
        return field != null ? "'" + field + "'" : "null";

    }

    @SafeVarargs
    public static String makeMap(Pair<String, String>... pairs) {
        StringBuilder builder = new StringBuilder();
        builder.append("{");

        String delimiter = "";
        for (Pair<String, String> pair : pairs) {
            builder.append(delimiter)
                    .append("'").append(pair.getKey()).append("'")
                    .append(" : '").append(pair.getValue()).append("'");
            delimiter = ", ";
        }
        builder.append("}");
        return builder.toString();
    }

    public static String makeMap(Map<String, String> map) {
        if (map == null) {
            return "null";
        }
        return makeMap(map.entrySet().toArray(new Pair[0]));
    }

    public static Clause makeClause(FilterType operation, Clause... clauses) {
        LogicalClause clause = new LogicalClause();
        clause.setOperation(operation);
        if (clauses != null) {
            clause.setClauses(Arrays.asList(clauses));
        }
        return clause;
    }

    public static Clause makeClause(String field, List<String> values, FilterType operation) {
        FilterClause clause = new FilterClause();
        clause.setField(field);
        clause.setValues(values == null ? Collections.singletonList(Type.NULL_EXPRESSION) : values);
        clause.setOperation(operation);
        return clause;
    }

    // Again, not implementing toString in Clause to not tie the construction of the JSON to the src.
    public static String toString(Clause clause) {
        StringBuilder builder = new StringBuilder();
        if (clause instanceof FilterClause) {
            FilterClause filterClause = (FilterClause) clause;
            builder.append(makeFilter(filterClause.getField(), filterClause.getValues(), filterClause.getOperation()));
        } else if (clause instanceof LogicalClause) {
            LogicalClause logicalClause = (LogicalClause) clause;
            builder.append(makeFilter(logicalClause.getClauses(), logicalClause.getOperation()));
        }
        return builder.toString();
    }

    public static String getOperationFor(FilterType operation) {
        switch (operation) {
            case EQUALS:
                return "==";
            case NOT_EQUALS:
                return "!=";
            case GREATER_THAN:
                return ">";
            case LESS_THAN:
                return "<";
            case GREATER_EQUALS:
                return ">=";
            case LESS_EQUALS:
                return "<=";
            case AND:
                return "AND";
            case OR:
                return "OR";
            case NOT:
                return "NOT";
            default:
                return "";
        }
    }

    public static String getOperationFor(AggregationType operation) {
        switch (operation) {
            case TOP:
                return "TOP";
            case RAW:
                return "RAW";
            case GROUP:
                return "GROUP";
            case COUNT_DISTINCT:
                return "COUNT DISTINCT";
            default:
                return "";
        }
    }

    public static AggregationRule getAggregationRule(String ruleString, Map configuration) {
        try {
            return new AggregationRule(ruleString, configuration);
        } catch (ParsingException e) {
            throw new RuntimeException(e);
        }
    }

    public static FilterRule getFilterRule(String input, Map configuration) {
        try {
            return new FilterRule(input, configuration);
        } catch (ParsingException e) {
            throw new RuntimeException(e);
        }
    }
}
