/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations.grouping;

import com.yahoo.bullet.Utilities;
import com.yahoo.bullet.operations.AggregationOperations.GroupOperationType;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

/**
 * This class captures an operation that will be performed on an entire group - counts, sums, mins etc.
 * Other than count, all other operations include a field name on which the operation is applied.
 */
@AllArgsConstructor @Getter
public class GroupOperation implements Serializable {
    public static final long serialVersionUID = 40039294765462402L;

    public static final Set<GroupOperationType> SUPPORTED_GROUP_OPERATIONS = new HashSet<>(asList(GroupOperationType.COUNT,
                                                                                                  GroupOperationType.AVG,
                                                                                                  GroupOperationType.MAX,
                                                                                                  GroupOperationType.MIN,
                                                                                                  GroupOperationType.SUM));

    public static final String OPERATIONS = "operations";
    public static final String OPERATION_TYPE = "type";
    public static final String OPERATION_FIELD = "field";
    public static final String OPERATION_NEW_NAME = "newName";

    private final GroupOperationType type;
    private final String field;
    // Ignored purposefully for hashCode and equals
    private final String newName;

    @Override
    public int hashCode() {
        // Not relying on Enum hashcode
        String typeString = type == null ? null : type.getName();
        return Objects.hash(typeString, field);
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof GroupOperation)) {
            return false;
        }
        GroupOperation other = (GroupOperation) object;
        if (type != other.type) {
            return false;
        }
        if (field == null && other.field == null) {
            return true;
        }
        return field != null && field.equals(other.field);
    }

    /**
     * Parses a {@link Set} of group operation from an Object that is expected to be a {@link List} of {@link Map}
     *
     * @param attributes An Map that contains an object that is the representation of List of group operations.
     * @return A {@link Set} of GroupOperation or {@link Collections#emptySet()}.
     */
    @SuppressWarnings("unchecked")
    public static Set<GroupOperation> getOperations(Map<String, Object> attributes) {
        if (Utilities.isEmpty(attributes)) {
            return Collections.emptySet();
        }

        Object object = attributes.get(OPERATIONS);
        if (object == null) {
            return Collections.emptySet();
        }

        try {
            // Unchecked cast needed.
            List<Map<String, String>> operations = (List<Map<String, String>>) object;
            // Return a list of distinct, non-null, GroupOperations
            return operations.stream().map(GroupOperation::makeGroupOperation)
                    .filter(Objects::nonNull).collect(Collectors.toSet());
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
}
