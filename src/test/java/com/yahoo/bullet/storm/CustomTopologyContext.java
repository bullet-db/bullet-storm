package com.yahoo.bullet.storm;

import backtype.storm.metric.api.IMetric;
import backtype.storm.task.TopologyContext;

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

    private Long fetchCount(IMetric metric) {
        return metric == null ? null : (Long) ((AbsoluteCountMetric) metric).getValueAndReset();
    }

    public Long getCountForMetric(String name) {
        return fetchCount(getRegisteredMetricByName(name));
    }

    public Long getCountForMetric(Integer timeBucket, String name) {
        return fetchCount(getRegisteredMetricInTimeBucket(timeBucket, name));
    }
}

