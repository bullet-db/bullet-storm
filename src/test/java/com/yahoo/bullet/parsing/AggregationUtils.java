/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.operations.AggregationOperations;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;

public class AggregationUtils {
    public static Map<String, String> makeGroupOperation(AggregationOperations.GroupOperationType type, String field, String newName) {
        Map<String, String> map = new HashMap<>();
        if (type != null) {
            map.put(Aggregation.OPERATION_TYPE, type.getName());
        }
        if (field != null) {
            map.put(Aggregation.OPERATION_FIELD, field);
        }
        if (newName != null) {
            map.put(Aggregation.OPERATION_NEW_NAME, newName);
        }
        return map;
    }

    @SafeVarargs
    public static Map<String, Object> makeAttributes(Map<String, String>... maps) {
        Map<String, Object> map = new HashMap<>();
        map.put(Aggregation.OPERATIONS, asList(maps));
        return map;
    }
}
