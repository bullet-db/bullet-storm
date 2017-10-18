/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import org.apache.storm.Config;
import org.apache.storm.metric.LoggingMetricsConsumer;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class SigarLoggingMetricsConsumer extends LoggingMetricsConsumer {
    public static final Map<String, String> METRICS = singletonMap("CPU", "org.apache.storm.metrics.sigar.CPUMetric");

    /**
     * Registers the Sigar CPUMetric and the LoggingMetricsConsumer with a parallelism of 1.
     *
     * @param stormConfig The Storm {@link Config} to add to.
     * @param bulletStormConfig The Bullet {@link BulletStormConfig} to get information from.
     */
    public static void register(Config stormConfig, BulletStormConfig bulletStormConfig) {
        stormConfig.registerMetricsConsumer(LoggingMetricsConsumer.class);
        Map<String, String> metrics = (Map<String, String>) stormConfig.getOrDefault(Config.TOPOLOGY_WORKER_METRICS, new HashMap<>());
        metrics.putAll(METRICS);
        stormConfig.put(Config.TOPOLOGY_WORKER_METRICS, metrics);
    }
}
