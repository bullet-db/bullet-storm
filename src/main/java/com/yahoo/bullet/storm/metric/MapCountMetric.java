/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.metric;

import lombok.Getter;
import org.apache.storm.metric.api.IMetric;

import java.util.HashMap;
import java.util.Map;

/**
 * Metric that holds a map of counts.
 */
public class MapCountMetric implements IMetric {
    @Getter
    private Map<String, Long> counts = new HashMap<>();

    /**
     * Adds a long (can be negative) to the current count for the given key.
     *
     * @param key The value of the dimension.
     * @param value The count to add.
     */
    public void add(String key, long value) {
        Long count = counts.getOrDefault(key, 0L);
        count += value;
        counts.put(key, count);
    }

    /**
     * Sets the current count for the given key to the given value.
     *
     * @param key The value of the dimension.
     * @param value The value to set.
     */
    public void set(String key, long value) {
        counts.put(key, value);
    }

    /**
     * Clears all counts.
     */
    public void clear() {
        counts.clear();
    }

    @Override
    public Object getValueAndReset() {
        return counts;
    }
}
