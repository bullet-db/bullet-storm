/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.Utilities;
import com.yahoo.bullet.operations.AggregationOperations.AggregationType;
import com.yahoo.bullet.operations.AggregationOperations.GroupOperationType;
import com.yahoo.bullet.operations.aggregations.CountDistinct;
import com.yahoo.bullet.operations.aggregations.Distribution;
import com.yahoo.bullet.operations.aggregations.GroupAll;
import com.yahoo.bullet.operations.aggregations.GroupBy;
import com.yahoo.bullet.operations.aggregations.Raw;
import com.yahoo.bullet.operations.aggregations.Strategy;
import com.yahoo.bullet.operations.aggregations.grouping.GroupOperation;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.yahoo.bullet.parsing.Error.makeError;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@Getter @Setter
public class Aggregation implements Configurable, Validatable {
    @Expose
    private Integer size;
    @Expose
    private AggregationType type;
    @Expose
    private Map<String, Object> attributes;
    @Expose
    private Map<String, String> fields;

    // Parsed fields

    @Setter(AccessLevel.NONE)
    private Set<GroupOperation> groupOperations;

    @Setter(AccessLevel.NONE)
    private List<Double> distributionPoints;

    @Setter(AccessLevel.NONE)
    private Strategy strategy;

    // In case any strategies need it.
    private Map configuration;

    // TODO: Move this to a Validation object tied in properly with Strategies when all are added.
    public static final Set<AggregationType> SUPPORTED_AGGREGATION_TYPES =
            new HashSet<>(asList(AggregationType.GROUP, AggregationType.COUNT_DISTINCT, AggregationType.RAW,
                                 AggregationType.DISTRIBUTION));

    public static final String TYPE_NOT_SUPPORTED_ERROR_PREFIX = "Aggregation type not supported";
    public static final String TYPE_NOT_SUPPORTED_RESOLUTION = "Current supported aggregation types are: RAW, GROUP, " +
                                                               "COUNT DISTINCT";

    public static final String GROUP_OPERATION_REQUIRES_FIELD = "Group operation requires a field: ";
    public static final String OPERATION_REQUIRES_FIELD_RESOLUTION = "Please add a field for this operation.";

    public static final Error REQUIRES_FIELD_ERROR =
            makeError("This aggregation type requires at least one field", OPERATION_REQUIRES_FIELD_RESOLUTION);
    public static final Error REQUIRES_FIELD_OR_OPERATION_ERROR =
            makeError("This aggregation type requires at least one field or operation", "Please add a field or an operation");
    public static final Error REQUIRES_POINTS_ERROR =
            makeError("The DISTRIBUTION type requires at least one point",
                      "Please add a list of numeric points or specify a start, end and increment to generate points");

    public static final Integer DEFAULT_SIZE = 1;
    public static final Integer DEFAULT_MAX_SIZE = 512;

    public static final String DEFAULT_FIELD_SEPARATOR = "|";


    /**
     * Default constructor. GSON recommended
     */
    public Aggregation() {
        type = AggregationType.RAW;
    }

    @Override
    public void configure(Map configuration) {
        this.configuration = configuration;

        Number defaultSize = (Number) configuration.getOrDefault(BulletConfig.AGGREGATION_DEFAULT_SIZE, DEFAULT_SIZE);
        Number maximumSize = (Number) configuration.getOrDefault(BulletConfig.AGGREGATION_MAX_SIZE, DEFAULT_MAX_SIZE);
        int sizeDefault = defaultSize.intValue();
        int sizeMaximum = maximumSize.intValue();

        // Null or negative, then default, else min of size and max
        size = (size == null || size < 0) ? sizeDefault : Math.min(size, sizeMaximum);

        // Parse any group operations first before Strategy.
        if (type == AggregationType.GROUP) {
            groupOperations = GroupOperation.getOperations(attributes);
        }

        // Parse any distribution ranges before Strategy.
        if (type == AggregationType.DISTRIBUTION) {
            distributionPoints = Distribution.getPoints(attributes, configuration);
        }

        strategy = findStrategy();
    }

    @Override public Optional<List<Error>> validate() {
        // Supported aggregation types should be documented in TYPE_NOT_SUPPORTED_RESOLUTION
        if (!SUPPORTED_AGGREGATION_TYPES.contains(type)) {
            String typeSuffix = type == null ? "" : ": " + type;
            return Optional.of(singletonList(makeError(TYPE_NOT_SUPPORTED_ERROR_PREFIX + typeSuffix,
                                                       TYPE_NOT_SUPPORTED_RESOLUTION)));
        }
        boolean noFields = Utilities.isEmpty(fields);
        boolean noOperations = Utilities.isEmpty(groupOperations);
        if (noFields) {
            if (type == AggregationType.COUNT_DISTINCT || type == AggregationType.DISTRIBUTION) {
                return Optional.of(singletonList(REQUIRES_FIELD_ERROR));
            }
            if (type == AggregationType.GROUP && noOperations) {
                return Optional.of(singletonList(REQUIRES_FIELD_OR_OPERATION_ERROR));
            }
        }

        if (!noOperations) {
            for (GroupOperation operation : groupOperations) {
                if (operation.getField() == null && operation.getType() != GroupOperationType.COUNT) {
                    return Optional.of(singletonList(makeError(GROUP_OPERATION_REQUIRES_FIELD + operation.getType(),
                                                               OPERATION_REQUIRES_FIELD_RESOLUTION)));
                }
            }
        }
        boolean noPoints = Utilities.isEmpty(distributionPoints);
        if (noPoints && type == AggregationType.DISTRIBUTION) {
            return Optional.of(singletonList(REQUIRES_POINTS_ERROR));
        }
        return Optional.empty();
    }

    /**
     * Returns a new {@link Strategy} instance that can handle this aggregation.
     *
     * @return the created instance of a strategy that can implement the provided AggregationType or null if it cannot.
     */
    Strategy findStrategy() {
        if (type == AggregationType.RAW) {
            return new Raw(this);
        }

        if (type == AggregationType.DISTRIBUTION) {
            return new Distribution(this);
        }

        boolean haveFields = !Utilities.isEmpty(fields);

        if (type == AggregationType.COUNT_DISTINCT && haveFields) {
            return new CountDistinct(this);
        }

        boolean haveOperations = !Utilities.isEmpty(groupOperations);
        if (type == AggregationType.GROUP) {
            if (haveFields) {
                return new GroupBy(this);
            }
            // We don't have fields
            if (haveOperations) {
                return new GroupAll(this);
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", fields: " + fields + ", attributes: " + attributes + "}";
    }

}
