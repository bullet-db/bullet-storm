/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.Config;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Arrays.asList;

@Slf4j
public class BulletStormConfig extends BulletConfig {
    public static final String TOPOLOGY_SCHEDULER = "bullet.topology.scheduler";
    public static final String TOPOLOGY_FUNCTION = "bullet.topology.function";
    public static final String TOPOLOGY_NAME = "bullet.topology.name";
    public static final String TOPOLOGY_WORKERS = "bullet.topology.workers";
    public static final String TOPOLOGY_DEBUG = "bullet.topology.debug";
    public static final String TOPOLOGY_METRICS_ENABLE = "bullet.topology.metrics.enable";
    public static final String TOPOLOGY_METRICS_CLASSES = "bullet.topology.metrics.classes";
    public static final String TOPOLOGY_METRICS_BUILT_IN_ENABLE = "bullet.topology.metrics.built.in.enable";
    public static final String TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING = "bullet.topology.metrics.built.in.emit.interval.mapping";
    public static final String QUERY_SPOUT_PARALLELISM = "bullet.topology.query.spout.parallelism";
    public static final String QUERY_SPOUT_CPU_LOAD = "bullet.topology.query.spout.cpu.load";
    public static final String QUERY_SPOUT_MEMORY_ON_HEAP_LOAD = "bullet.topology.query.spout.memory.on.heap.load";
    public static final String QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD = "bullet.topology.query.spout.memory.off.heap.load";
    public static final String FILTER_BOLT_PARALLELISM = "bullet.topology.filter.bolt.parallelism";
    public static final String FILTER_BOLT_CPU_LOAD = "bullet.topology.filter.bolt.cpu.load";
    public static final String FILTER_BOLT_MEMORY_ON_HEAP_LOAD = "bullet.topology.filter.bolt.memory.on.heap.load";
    public static final String FILTER_BOLT_MEMORY_OFF_HEAP_LOAD = "bullet.topology.filter.bolt.memory.off.heap.load";
    public static final String JOIN_BOLT_PARALLELISM = "bullet.topology.join.bolt.parallelism";
    public static final String JOIN_BOLT_CPU_LOAD = "bullet.topology.join.bolt.cpu.load";
    public static final String JOIN_BOLT_MEMORY_ON_HEAP_LOAD = "bullet.topology.join.bolt.memory.on.heap.load";
    public static final String JOIN_BOLT_MEMORY_OFF_HEAP_LOAD = "bullet.topology.join.bolt.memory.off.heap.load";
    public static final String JOIN_BOLT_ERROR_TICK_TIMEOUT = "bullet.topology.join.bolt.error.tick.timeout";
    public static final String JOIN_BOLT_QUERY_TICK_TIMEOUT = "bullet.topology.join.bolt.query.tick.timeout";
    public static final String RESULT_BOLT_PARALLELISM = "bullet.topology.result.bolt.parallelism";
    public static final String RESULT_BOLT_CPU_LOAD = "bullet.topology.result.bolt.cpu.load";
    public static final String RESULT_BOLT_MEMORY_ON_HEAP_LOAD = "bullet.topology.result.bolt.memory.on.heap.load";
    public static final String RESULT_BOLT_MEMORY_OFF_HEAP_LOAD = "bullet.topology.result.bolt.memory.off.heap.load";
    public static final String TICK_INTERVAL_SECS = "bullet.topology.tick.interval.secs";

    public static Set<String> TOPOLOGY_SUBMISSION_SETTINGS =
            new HashSet<>(asList(QUERY_SPOUT_PARALLELISM, QUERY_SPOUT_CPU_LOAD, QUERY_SPOUT_MEMORY_ON_HEAP_LOAD,
                                 QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD, FILTER_BOLT_PARALLELISM, FILTER_BOLT_CPU_LOAD,
                                 FILTER_BOLT_MEMORY_ON_HEAP_LOAD, FILTER_BOLT_MEMORY_OFF_HEAP_LOAD, JOIN_BOLT_PARALLELISM,
                                 JOIN_BOLT_CPU_LOAD, JOIN_BOLT_MEMORY_ON_HEAP_LOAD, JOIN_BOLT_MEMORY_OFF_HEAP_LOAD,
                                 RESULT_BOLT_PARALLELISM, RESULT_BOLT_CPU_LOAD, RESULT_BOLT_MEMORY_ON_HEAP_LOAD,
                                 RESULT_BOLT_MEMORY_OFF_HEAP_LOAD, TOPOLOGY_SCHEDULER, TOPOLOGY_NAME, TOPOLOGY_WORKERS,
                                 TOPOLOGY_DEBUG, TOPOLOGY_METRICS_ENABLE));

    public static final String DEFAULT_STORM_CONFIGURATION = "bullet_storm_defaults.yaml";

    /**
     * Constructor that loads specific file augmented with defaults.
     *
     * @param file YAML file to load.
     * @throws IOException if an error occurred with the file loading.
     */
    public BulletStormConfig(String file) throws IOException {
        // Load Bullet defaults
        super(null);
        // Load Bullet Storm settings
        Config stormDefaults = new Config(file, DEFAULT_STORM_CONFIGURATION);
        // Merge Storm settings onto Bullet defaults
        merge(stormDefaults);
        log.info("Bullet Storm merged settings:\n {}", getAll(Optional.empty()));
    }

    /**
     * Gets all the settings besides the {@link #TOPOLOGY_SUBMISSION_SETTINGS}.
     *
     * @return A {@link Map} of the other settings.
     */
    public Map<String, Object> getNonTopologySubmissionSettings() {
        return getAllBut(Optional.of(TOPOLOGY_SUBMISSION_SETTINGS));
    }
}
