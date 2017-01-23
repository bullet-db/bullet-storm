/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations.grouping;

import com.yahoo.bullet.operations.AggregationOperations.GroupOperationType;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Objects;

/**
 * This class captures an operation that will be performed on an entire group - counts, sums, mins etc.
 * Other than count, all other operations include a field name on which the operation is applied.
 */
@AllArgsConstructor @Getter
public class GroupOperation implements Serializable {
    public static final long serialVersionUID = 40039294765462402L;

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
}
