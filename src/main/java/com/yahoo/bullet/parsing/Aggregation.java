/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.AggregationOperations.AggregationType;
import com.yahoo.bullet.operations.AggregationOperations.GroupOperationType;
import com.yahoo.bullet.operations.aggregations.CountDistinct;
import com.yahoo.bullet.operations.aggregations.GroupAll;
import com.yahoo.bullet.operations.aggregations.GroupBy;
import com.yahoo.bullet.operations.aggregations.Raw;
import com.yahoo.bullet.operations.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.operations.aggregations.Strategy;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

    @Setter(AccessLevel.NONE)
    private Set<GroupOperation> groupOperations;

    @Setter(AccessLevel.NONE)
    private Strategy strategy;

    // In case any strategies need it.
    private Map configuration;

    // TODO: Move this to a Validation object tied in properly with Strategies when all are added.
    public static final Set<AggregationType> SUPPORTED_AGGREGATION_TYPES = new HashSet<>(asList(AggregationType.GROUP,
                                                                                                AggregationType.COUNT_DISTINCT,
                                                                                                AggregationType.RAW));

    public static final Set<GroupOperationType> SUPPORTED_GROUP_OPERATIONS = new HashSet<>(asList(GroupOperationType.COUNT,
                                                                                                  GroupOperationType.AVG,
                                                                                                  GroupOperationType.MAX,
                                                                                                  GroupOperationType.MIN,
                                                                                                  GroupOperationType.SUM));

    public static final String TYPE_NOT_SUPPORTED_ERROR_PREFIX = "Aggregation type not supported";
    public static final String TYPE_NOT_SUPPORTED_RESOLUTION = "Current supported aggregation types are: RAW, GROUP, " +
            "                                                   COUNT DISTINCT";

    public static final String SUPPORTED_GROUP_OPERATIONS_RESOLUTION =
            "Currently supported operations are: COUNT, AVG, MIN, MAX, SUM";

    public static final String GROUP_OPERATION_REQUIRES_FIELD = "Group operation requires a field: ";
    public static final String OPERATION_REQUIRES_FIELD_RESOLUTION = "Please add a field for this operation.";

    public static final Error REQUIRES_FIELD_ERROR =
            makeError("This aggregation type requires at least one field", OPERATION_REQUIRES_FIELD_RESOLUTION);

    public static final Integer DEFAULT_SIZE = 1;
    public static final Integer DEFAULT_MAX_SIZE = 512;

    public static final String DEFAULT_FIELD_SEPARATOR = "|";

    public static final String OPERATIONS = "operations";
    public static final String OPERATION_TYPE = "type";
    public static final String OPERATION_FIELD = "field";
    public static final String OPERATION_NEW_NAME = "newName";

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

        // Parse any group operations first before finding Strategy.
        groupOperations = getOperations();

        strategy = findStrategy();
    }

    @Override
    public Optional<List<Error>> validate() {
        if (type == AggregationType.COUNT_DISTINCT) {
            if (isEmpty(fields)) {
                return Optional.of(singletonList(REQUIRES_FIELD_ERROR));
            }
        }
        // Supported aggregation types should be documented in TYPE_NOT_SUPPORTED_RESOLUTION
        if (!SUPPORTED_AGGREGATION_TYPES.contains(type)) {
            String typeSuffix = type == null ? "" : ": " + type;
            return Optional.of(singletonList(makeError(TYPE_NOT_SUPPORTED_ERROR_PREFIX + typeSuffix,
                                                       TYPE_NOT_SUPPORTED_RESOLUTION)));
        }
        if (!groupOperations.isEmpty()) {
            for (GroupOperation operation : groupOperations) {
                if (operation.getField() == null && operation.getType() != GroupOperationType.COUNT) {
                    return Optional.of(singletonList(makeError(GROUP_OPERATION_REQUIRES_FIELD + operation.getType(),
                                                               OPERATION_REQUIRES_FIELD_RESOLUTION)));
                }
            }
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

        boolean haveFields = !isEmpty(fields);

        if (type == AggregationType.COUNT_DISTINCT && haveFields) {
            return new CountDistinct(this);
        }

        boolean haveOperations = !isEmpty(groupOperations);
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

    private static boolean isEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    private static boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }

    private Set<GroupOperation> getOperations() {
        if (isEmpty(attributes)) {
            return Collections.emptySet();
        }
        return parseOperations(attributes.get(OPERATIONS));
    }

    private Set<GroupOperation> parseOperations(Object object) {
        if (object == null) {
            return Collections.emptySet();
        }
        try {
            // Unchecked cast needed.
            List<Map<String, String>> operations = (List<Map<String, String>>) object;
            // Return a list of distinct, non-null, GroupOperations
            return operations.stream().map(Aggregation::makeGroupOperation)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
        } catch (ClassCastException cce) {
            return Collections.emptySet();
        }
    }

    private static GroupOperation makeGroupOperation(Map<String, String> data) {
        String type = data.get(OPERATION_TYPE);
        Optional<GroupOperationType> operation = SUPPORTED_GROUP_OPERATIONS.stream().filter(t -> t.isMe(type)).findFirst();
        // May or may not be present
        String field = data.get(OPERATION_FIELD);
        // May or may not be present
        String newName = data.get(OPERATION_NEW_NAME);
        // Unknown GroupOperations are ignored.
        return operation.isPresent() ? new GroupOperation(operation.get(), field, newName) : null;
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", fields: " + fields + ", attributes: " + attributes + "}";
    }

}
