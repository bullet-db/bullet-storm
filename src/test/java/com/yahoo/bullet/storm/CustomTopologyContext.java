/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import org.apache.storm.metric.api.IMetric;
import org.apache.storm.task.TopologyContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class CustomTopologyContext extends TopologyContext {
    private Map<Integer, Map<String, IMetric>> registeredMetrics;

    public CustomTopologyContext() {
        super(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        registeredMetrics = new HashMap<>();
    }

    @Override
    public <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs) {
        Map<String, IMetric> metrics = registeredMetrics.getOrDefault(timeBucketSizeInSecs, new HashMap<>());
        metrics.put(name, metric);
        registeredMetrics.putIfAbsent(timeBucketSizeInSecs, metrics);
        return metric;
    }

    @Override
    public IMetric getRegisteredMetricByName(String name) {
        Optional<Map.Entry<String, IMetric>> metric = registeredMetrics.values().stream()
                                                                       .flatMap(m -> m.entrySet().stream())
                                                                       .filter(e -> e.getKey().equals(name))
                                                                       .findFirst();
        return metric.isPresent() ? metric.get().getValue() : null;
    }

    public IMetric getRegisteredMetricInTimeBucket(Integer timeBucket, String name) {
        Optional<Map.Entry<String, IMetric>> metric = registeredMetrics.getOrDefault(timeBucket, Collections.emptyMap())
                                                                       .entrySet().stream()
                                                                       .filter(e -> e.getKey().equals(name))
                                                                       .findFirst();
        return metric.isPresent() ? metric.get().getValue() : null;
    }

    private Number fetchResult(IMetric metric) {
        return metric == null ? null : (Number) metric.getValueAndReset();
    }

    public Double getDoubleMetric(String name) {
        return (Double) fetchResult(getRegisteredMetricByName(name));
    }

    public Double getDoubleMetric(Integer timeBucket, String name) {
        return (Double) fetchResult(getRegisteredMetricInTimeBucket(timeBucket, name));
    }

    public Long getLongMetric(String name) {
        return (Long) fetchResult(getRegisteredMetricByName(name));
    }

    public Long getLongMetric(Integer timeBucket, String name) {
        return (Long) fetchResult(getRegisteredMetricInTimeBucket(timeBucket, name));
    }
}

