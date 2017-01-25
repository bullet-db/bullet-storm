/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Arrays.asList;

@Slf4j
public class BulletConfig extends Config {
    public static final String TOPOLOGY_SCHEDULER = "topology.scheduler";
    public static final String TOPOLOGY_FUNCTION = "topology.function";
    public static final String TOPOLOGY_NAME = "topology.name";
    public static final String TOPOLOGY_WORKERS = "topology.workers";
    public static final String TOPOLOGY_DEBUG = "topology.debug";
    public static final String TOPOLOGY_METRICS_ENABLE = "topology.metrics.enable";
    public static final String DRPC_SPOUT_PARALLELISM = "topology.drpc.spout.parallelism";
    public static final String DRPC_SPOUT_CPU_LOAD = "topology.drpc.spout.cpu.load";
    public static final String DRPC_SPOUT_MEMORY_ON_HEAP_LOAD = "topology.drpc.spout.memory.on.heap.load";
    public static final String DRPC_SPOUT_MEMORY_OFF_HEAP_LOAD = "topology.drpc.spout.memory.off.heap.load";
    public static final String PREPARE_BOLT_PARALLELISM = "topology.prepare.bolt.parallelism";
    public static final String PREPARE_BOLT_CPU_LOAD = "topology.prepare.bolt.cpu.load";
    public static final String PREPARE_BOLT_MEMORY_ON_HEAP_LOAD = "topology.prepare.bolt.memory.on.heap.load";
    public static final String PREPARE_BOLT_MEMORY_OFF_HEAP_LOAD = "topology.prepare.bolt.memory.off.heap.load";
    public static final String FILTER_BOLT_PARALLELISM = "topology.filter.bolt.parallelism";
    public static final String FILTER_BOLT_CPU_LOAD = "topology.filter.bolt.cpu.load";
    public static final String FILTER_BOLT_MEMORY_ON_HEAP_LOAD = "topology.filter.bolt.memory.on.heap.load";
    public static final String FILTER_BOLT_MEMORY_OFF_HEAP_LOAD = "topology.filter.bolt.memory.off.heap.load";
    public static final String JOIN_BOLT_PARALLELISM = "topology.join.bolt.parallelism";
    public static final String JOIN_BOLT_CPU_LOAD = "topology.join.bolt.cpu.load";
    public static final String JOIN_BOLT_MEMORY_ON_HEAP_LOAD = "topology.join.bolt.memory.on.heap.load";
    public static final String JOIN_BOLT_MEMORY_OFF_HEAP_LOAD = "topology.join.bolt.memory.off.heap.load";
    public static final String JOIN_BOLT_ERROR_TICK_TIMEOUT = "topology.join.bolt.error.tick.timeout";
    public static final String JOIN_BOLT_RULE_TICK_TIMEOUT = "topology.join.bolt.rule.tick.timeout";
    public static final String RETURN_BOLT_PARALLELISM = "topology.return.bolt.parallelism";
    public static final String RETURN_BOLT_CPU_LOAD = "topology.return.bolt.cpu.load";
    public static final String RETURN_BOLT_MEMORY_ON_HEAP_LOAD = "topology.return.bolt.memory.on.heap.load";
    public static final String RETURN_BOLT_MEMORY_OFF_HEAP_LOAD = "topology.return.bolt.memory.off.heap.load";
    public static final String TICK_INTERVAL_SECS = "topology.tick.interval.secs";

    public static final String SPECIFICATION_DEFAULT_DURATION = "rule.default.duration";
    public static final String SPECIFICATION_MAX_DURATION = "rule.max.duration";
    public static final String AGGREGATION_DEFAULT_SIZE = "rule.aggregation.default.size";
    public static final String AGGREGATION_MAX_SIZE = "rule.aggregation.max.size";
    public static final String AGGREGATION_COMPOSITE_FIELD_SEPARATOR = "rule.aggregation.composite.field.separator";

    public static final String RAW_AGGREGATION_MAX_SIZE = "rule.aggregation.max.size";
    public static final String RAW_AGGREGATION_MICRO_BATCH_SIZE = "rule.aggregation.raw.micro.batch.size";

    public static final String COUNT_DISTINCT_AGGREGATION_SKETCH_ENTRIES = "rule.aggregation.count.distinct.sketch.entries";
    public static final String COUNT_DISTINCT_AGGREGATION_SKETCH_SAMPLING = "rule.aggregation.count.distinct.sketch.sampling";
    public static final String COUNT_DISTINCT_AGGREGATION_SKETCH_FAMILY = "rule.aggregation.count.distinct.sketch.family";
    public static final String COUNT_DISTINCT_AGGREGATION_SKETCH_RESIZE_FACTOR = "rule.aggregation.count.distinct.sketch.resize.factor";

    public static final String GROUP_AGGREGATION_SKETCH_ENTRIES = "rule.aggregation.group.sketch.entries";
    public static final String GROUP_AGGREGATION_SKETCH_SAMPLING = "rule.aggregation.group.sketch.sampling";
    public static final String GROUP_AGGREGATION_SKETCH_RESIZE_FACTOR = "rule.aggregation.group.sketch.resize.factor";

    public static final String RECORD_INJECT_TIMESTAMP = "record.inject.timestamp.enable";
    public static final String RECORD_INJECT_TIMESTAMP_KEY = "record.inject.timestamp.key";

    public static final String RESULT_METADATA_ENABLE = "result.metadata.enable";
    public static final String RESULT_METADATA_METRICS = "result.metadata.metrics";
    public static final String RESULT_METADATA_METRICS_CONCEPT_KEY = "name";
    public static final String RESULT_METADATA_METRICS_NAME_KEY = "key";

    public static final String RESULT_METADATA_METRICS_MAPPING = "result.metadata.metrics.mapping";

    public static Set<String> TOPOLOGY_SUBMISSION_SETTINGS =
            new HashSet<>(asList(DRPC_SPOUT_PARALLELISM, DRPC_SPOUT_CPU_LOAD, DRPC_SPOUT_MEMORY_ON_HEAP_LOAD,
                                 DRPC_SPOUT_MEMORY_OFF_HEAP_LOAD, PREPARE_BOLT_PARALLELISM, PREPARE_BOLT_CPU_LOAD,
                                 PREPARE_BOLT_MEMORY_ON_HEAP_LOAD, PREPARE_BOLT_MEMORY_OFF_HEAP_LOAD,
                                 FILTER_BOLT_PARALLELISM, FILTER_BOLT_CPU_LOAD, FILTER_BOLT_MEMORY_ON_HEAP_LOAD,
                                 FILTER_BOLT_MEMORY_OFF_HEAP_LOAD, JOIN_BOLT_PARALLELISM, JOIN_BOLT_CPU_LOAD,
                                 JOIN_BOLT_MEMORY_ON_HEAP_LOAD, JOIN_BOLT_MEMORY_OFF_HEAP_LOAD,
                                 RETURN_BOLT_PARALLELISM, RETURN_BOLT_CPU_LOAD, RETURN_BOLT_MEMORY_ON_HEAP_LOAD,
                                 RETURN_BOLT_MEMORY_OFF_HEAP_LOAD, TOPOLOGY_SCHEDULER, TOPOLOGY_FUNCTION,
                                 TOPOLOGY_NAME, TOPOLOGY_WORKERS, TOPOLOGY_DEBUG, TOPOLOGY_METRICS_ENABLE));
    /**
     * Constructor that loads specific file augmented with defaults.
     *
     * @param file YAML file to load.
     * @throws IOException if an error occurred with the file loading.
     */
    public BulletConfig(String file) throws IOException {
        super(file);
    }

    /**
     * Default constructor.
     *
     * @throws IOException if an error occurred with loading the default config.
     */
    public BulletConfig() throws IOException {
        super();
    }

    /**
     * Gets all the settings besides the {@link #TOPOLOGY_SUBMISSION_SETTINGS}.
     *
     * @return A {@link Map} of the other settings.
     */
    public Map<String, Object> getBulletSettingsOnly() {
        return getAllBut(Optional.of(TOPOLOGY_SUBMISSION_SETTINGS));
    }
}
