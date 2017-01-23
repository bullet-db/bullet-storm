/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.AggregationOperations;
import com.yahoo.bullet.result.Metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class AggregationUtils {
    public static Map<String, String> makeGroupFields(List<String> fields) {
        if (fields != null) {
            return fields.stream().collect(Collectors.toMap(Function.identity(), Function.identity()));
        }
        return new HashMap<>();
    }

    public static Map<String, String> makeGroupFields(String... fields) {
        if (fields == null) {
            return new HashMap<>();
        }
        return makeGroupFields(asList(fields));
    }

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
        return makeAttributes(asList(maps));
    }

    @SafeVarargs
    public static Map<Object, Object> addParsedMetadata(Map<Object, Object> configuration, Map.Entry<Metadata.Concept, String>... metadata) {
        return addParsedMetadata(configuration, metadata != null ? asList(metadata) : null);
    }

    public static Map<Object, Object> addParsedMetadata(Map<Object, Object> configuration, List<Map.Entry<Metadata.Concept, String>> metadata) {
        if (metadata != null) {
            Map<String, String> metadataKeys = new HashMap<>();
            for (Map.Entry<Metadata.Concept, String> e : metadata) {
                metadataKeys.put(e.getKey().getName(), e.getValue());
            }
            configuration.put(BulletConfig.RESULT_METADATA_METRICS_MAPPING, metadataKeys);
            configuration.put(BulletConfig.RESULT_METADATA_METRICS_MAPPING, metadataKeys);
        }
        return configuration;
    }

    public static Map<String, Object> makeAttributes(List<Map<String, String>> maps) {
        Map<String, Object> map = new HashMap<>();
        map.put(Aggregation.OPERATIONS, maps);
        return map;
    }
}
