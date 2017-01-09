/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations;

import com.google.gson.annotations.SerializedName;
import com.yahoo.bullet.operations.aggregations.CountDistinct;
import com.yahoo.bullet.operations.aggregations.GroupAll;
import com.yahoo.bullet.operations.aggregations.GroupOperation;
import com.yahoo.bullet.operations.aggregations.Raw;
import com.yahoo.bullet.operations.aggregations.Strategy;
import com.yahoo.bullet.parsing.Aggregation;
import lombok.Getter;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

public class AggregationOperations {
    public enum AggregationType {
        // The alternate value of DISTINCT for GROUP is allowed since having no GROUP operations is implicitly
        // a DISTINCT
        @SerializedName(value = "GROUP", alternate = { "DISTINCT" })
        GROUP,
        @SerializedName("COUNT DISTINCT")
        COUNT_DISTINCT,
        @SerializedName("TOP")
        TOP,
        @SerializedName("PERCENTILE")
        PERCENTILE,
        // The alternate value of LIMIT for RAW is allowed to preserve backward compatibility.
        @SerializedName(value = "RAW", alternate = { "LIMIT" })
        RAW
    }

    @Getter
    public enum GroupOperationType {
        COUNT("COUNT"),
        SUM("SUM"),
        MIN("MIN"),
        MAX("MAX"),
        AVG("AVG"),
        // COUNT_FIELD operation is only used internally in conjunction with AVG and won't be returned.
        COUNT_FIELD("COUNT_FIELD");

        private String name;

        GroupOperationType(String name) {
            this.name = name;
        }

        /**
         * Checks to see if this String represents this enum.
         *
         * @param name The String version of the enum.
         * @return true if the name represents this enum.
         */
        public boolean isMe(String name) {
            return this.name.equals(name);
        }
    }

    public interface AggregationOperator extends BiFunction<Number, Number, Number> {
    }

    // If either argument is null, a NullPointerException will be thrown.
    public static final AggregationOperator MIN = (x, y) -> x.doubleValue() <  y.doubleValue() ? x : y;
    public static final AggregationOperator MAX = (x, y) -> x.doubleValue() >  y.doubleValue() ? x : y;
    public static final AggregationOperator SUM = (x, y) -> x.doubleValue() + y.doubleValue();
    public static final AggregationOperator COUNT = (x, y) -> x.longValue() + y.longValue();

    /**
     * Checks to see if a {@link Map} contains items.
     *
     * @param map The map to check.
     * @return a boolean denoting if this map contains items.
     */
    public static boolean isEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    /**
     * Checks to see if a {@link Collection} contains items.
     *
     * @param collection The collection to check.
     * @return a boolean denoting if this list contains items.
     */
    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }

    /**
     * Returns a new {@link Strategy} instance that can handle the provided type of aggregation.
     *
     * @param aggregation The {@link Aggregation} to get a {@link Strategy} for.
     * @return the created instance of a strategy that can implement the provided AggregationType or null if it cannot.
     */
    public static Strategy getStrategyFor(Aggregation aggregation) {
        Objects.requireNonNull(aggregation);
        AggregationType type = aggregation.getType();

        if (type == AggregationType.RAW) {
            return new Raw(aggregation);
        }

        Map<String, String> fields = aggregation.getFields();
        boolean noFields = isEmpty(fields);

        if (type == AggregationType.COUNT_DISTINCT && !noFields) {
            return new CountDistinct(aggregation);
        }

        Set<GroupOperation> operations = aggregation.getGroupOperations();
        boolean noOperations = isEmpty(operations);
        if (type == AggregationType.GROUP && noFields && !noOperations) {
            return new GroupAll(aggregation);
        }

        return null;
    }
}
