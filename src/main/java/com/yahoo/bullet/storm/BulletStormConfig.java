/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Config;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.common.Validator;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.yahoo.bullet.storm.TopologyConstants.BUILT_IN_METRICS;

@Slf4j
public class BulletStormConfig extends BulletConfig implements Serializable {
    private static final long serialVersionUID = -1778598395631221122L;

    // Settings

    public static final String TOPOLOGY_NAME = "bullet.topology.name";
    public static final String TOPOLOGY_METRICS_ENABLE = "bullet.topology.metrics.enable";
    public static final String TOPOLOGY_METRICS_BUILT_IN_ENABLE = "bullet.topology.metrics.built.in.enable";
    public static final String TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING = "bullet.topology.metrics.built.in.emit.interval.mapping";
    public static final String TOPOLOGY_METRICS_CLASSES = "bullet.topology.metrics.classes";
    public static final String QUERY_SPOUT_PARALLELISM = "bullet.topology.query.spout.parallelism";
    public static final String QUERY_SPOUT_CPU_LOAD = "bullet.topology.query.spout.cpu.load";
    public static final String QUERY_SPOUT_MEMORY_ON_HEAP_LOAD = "bullet.topology.query.spout.memory.on.heap.load";
    public static final String QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD = "bullet.topology.query.spout.memory.off.heap.load";
    public static final String TICK_SPOUT_CPU_LOAD = "bullet.topology.tick.spout.cpu.load";
    public static final String TICK_SPOUT_MEMORY_ON_HEAP_LOAD = "bullet.topology.tick.spout.memory.on.heap.load";
    public static final String TICK_SPOUT_MEMORY_OFF_HEAP_LOAD = "bullet.topology.tick.spout.memory.off.heap.load";
    public static final String FILTER_BOLT_PARALLELISM = "bullet.topology.filter.bolt.parallelism";
    public static final String FILTER_BOLT_CPU_LOAD = "bullet.topology.filter.bolt.cpu.load";
    public static final String FILTER_BOLT_MEMORY_ON_HEAP_LOAD = "bullet.topology.filter.bolt.memory.on.heap.load";
    public static final String FILTER_BOLT_MEMORY_OFF_HEAP_LOAD = "bullet.topology.filter.bolt.memory.off.heap.load";
    public static final String JOIN_BOLT_PARALLELISM = "bullet.topology.join.bolt.parallelism";
    public static final String JOIN_BOLT_CPU_LOAD = "bullet.topology.join.bolt.cpu.load";
    public static final String JOIN_BOLT_MEMORY_ON_HEAP_LOAD = "bullet.topology.join.bolt.memory.on.heap.load";
    public static final String JOIN_BOLT_MEMORY_OFF_HEAP_LOAD = "bullet.topology.join.bolt.memory.off.heap.load";
    public static final String RESULT_BOLT_PARALLELISM = "bullet.topology.result.bolt.parallelism";
    public static final String RESULT_BOLT_CPU_LOAD = "bullet.topology.result.bolt.cpu.load";
    public static final String RESULT_BOLT_MEMORY_ON_HEAP_LOAD = "bullet.topology.result.bolt.memory.on.heap.load";
    public static final String RESULT_BOLT_MEMORY_OFF_HEAP_LOAD = "bullet.topology.result.bolt.memory.off.heap.load";
    public static final String LOOP_BOLT_PARALLELISM = "bullet.topology.loop.bolt.parallelism";
    public static final String LOOP_BOLT_CPU_LOAD = "bullet.topology.loop.bolt.cpu.load";
    public static final String LOOP_BOLT_MEMORY_ON_HEAP_LOAD = "bullet.topology.loop.bolt.memory.on.heap.load";
    public static final String LOOP_BOLT_MEMORY_OFF_HEAP_LOAD = "bullet.topology.loop.bolt.memory.off.heap.load";
    public static final String TICK_SPOUT_INTERVAL = "bullet.topology.tick.spout.interval.ms";
    public static final String FILTER_BOLT_STATS_REPORT_TICKS = "bullet.topology.filter.bolt.stats.report.ticks";
    public static final String JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS = "bullet.topology.join.bolt.query.post.finish.buffer.ticks";
    public static final String JOIN_BOLT_WINDOW_PRE_START_DELAY_TICKS = "bullet.topology.join.bolt.query.pre.start.delay.ticks";
    public static final String LOOP_BOLT_PUBSUB_OVERRIDES = "bullet.topology.loop.bolt.pubsub.overrides";

    // Defaults

    public static final String DEFAULT_TOPOLOGY_NAME = "bullet-topology";
    public static final boolean DEFAULT_TOPOLOGY_METRICS_ENABLE = false;
    public static final boolean DEFAULT_TOPOLOGY_METRICS_BUILT_IN_ENABLE = false;
    public static final Map<String, Number> DEFAULT_TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING = new HashMap<>();
    public static final String DEFAULT_BUILT_IN_METRICS_INTERVAL_KEY = "default";
    static {
        DEFAULT_TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING.put("bullet_active_queries", 10L);
        DEFAULT_TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING.put(DEFAULT_BUILT_IN_METRICS_INTERVAL_KEY, 10L);
    }
    public static final List<String> DEFAULT_TOPOLOGY_METRICS_CLASSES = new ArrayList<>();
    static {
        DEFAULT_TOPOLOGY_METRICS_CLASSES.add(SigarLoggingMetricsConsumer.class.getName());
    }
    public static final int DEFAULT_QUERY_SPOUT_PARALLELISM = 2;
    public static final double DEFAULT_QUERY_SPOUT_CPU_LOAD = 20.0;
    public static final double DEFAULT_QUERY_SPOUT_MEMORY_ON_HEAP_LOAD = 256.0;
    public static final double DEFAULT_QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD = 160.0;
    public static final double DEFAULT_TICK_SPOUT_CPU_LOAD = 20.0;
    public static final double DEFAULT_TICK_SPOUT_MEMORY_ON_HEAP_LOAD = 128.0;
    public static final double DEFAULT_TICK_SPOUT_MEMORY_OFF_HEAP_LOAD = 160.0;
    public static final Number DEFAULT_FILTER_BOLT_PARALLELISM = 16;
    public static final double DEFAULT_FILTER_BOLT_CPU_LOAD = 100.0;
    public static final double DEFAULT_FILTER_BOLT_MEMORY_ON_HEAP_LOAD = 256.0;
    public static final double DEFAULT_FILTER_BOLT_MEMORY_OFF_HEAP_LOAD = 160.0;
    public static final int DEFAULT_JOIN_BOLT_PARALLELISM = 2;
    public static final double DEFAULT_JOIN_BOLT_CPU_LOAD = 100.0;
    public static final double DEFAULT_JOIN_BOLT_MEMORY_ON_HEAP_LOAD = 512.0;
    public static final double DEFAULT_JOIN_BOLT_MEMORY_OFF_HEAP_LOAD = 160.0;
    public static final int DEFAULT_RESULT_BOLT_PARALLELISM = 2;
    public static final double DEFAULT_RESULT_BOLT_CPU_LOAD = 20.0;
    public static final double DEFAULT_RESULT_BOLT_MEMORY_ON_HEAP_LOAD = 256.0;
    public static final double DEFAULT_RESULT_BOLT_MEMORY_OFF_HEAP_LOAD = 160.0;
    public static final int DEFAULT_LOOP_BOLT_PARALLELISM = 2;
    public static final double DEFAULT_LOOP_BOLT_CPU_LOAD = 20.0;
    public static final double DEFAULT_LOOP_BOLT_MEMORY_ON_HEAP_LOAD = 256.0;
    public static final double DEFAULT_LOOP_BOLT_MEMORY_OFF_HEAP_LOAD = 160.0;
    public static final int DEFAULT_TICK_SPOUT_INTERVAL = 100;
    public static final int DEFAULT_FILTER_BOLT_STATS_REPORT_TICKS = 3000;
    public static final int DEFAULT_JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS = 3;
    public static final int DEFAULT_JOIN_BOLT_QUERY_PRE_START_DELAY_TICKS = 2;
    public static final Map<String, Object> DEFAULT_LOOP_BOLT_PUBSUB_OVERRIDES = Collections.emptyMap();

    // Other constants

    // Used automatically by the Storm code. Not for user setting.
    // This is the key to place the Storm configuration as
    public static final String STORM_CONFIG = "bullet.topology.storm.config";
    // This is the key to place the TopologyContext as
    public static final String STORM_CONTEXT = "bullet.topology.storm.context";

    public static final String CUSTOM_STORM_SETTING_PREFIX = "bullet.topology.custom.";

    // The number of tick spouts in the topology. This should be 1 since it is broadcast to all filter and join bolts.
    public static final int TICK_SPOUT_PARALLELISM = 1;
    // The smallest value that Tick Interval can be
    public static final int TICK_INTERVAL_MINIMUM = 10;

    public static final double SMALLEST_WINDOW_MIN_EMIT_EVERY_MULTIPLE = 2.0;
    public static final int PRE_START_DELAY_BUFFER_TICKS = 2;

    public static final String DEFAULT_STORM_CONFIGURATION = "bullet_storm_defaults.yaml";

    //  Validations

    private static final Validator VALIDATOR = BulletConfig.getValidator();

    static {
        VALIDATOR.define(TOPOLOGY_NAME)
                 .defaultTo(DEFAULT_TOPOLOGY_NAME)
                 .checkIf(Validator::isString);

        VALIDATOR.define(TOPOLOGY_METRICS_ENABLE)
                 .defaultTo(DEFAULT_TOPOLOGY_METRICS_ENABLE)
                 .checkIf(Validator::isBoolean);
        VALIDATOR.define(TOPOLOGY_METRICS_BUILT_IN_ENABLE)
                 .defaultTo(DEFAULT_TOPOLOGY_METRICS_BUILT_IN_ENABLE)
                 .checkIf(Validator::isBoolean);
        VALIDATOR.define(TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING)
                 .checkIf(Validator::isMap)
                 .checkIf(BulletStormConfig::isMetricMapping)
                 .defaultTo(DEFAULT_TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING);
        VALIDATOR.define(TOPOLOGY_METRICS_CLASSES)
                 .checkIf(Validator::isList)
                 .checkIf(BulletStormConfig::areMetricsConsumerClasses)
                 .defaultTo(DEFAULT_TOPOLOGY_METRICS_CLASSES);

        VALIDATOR.define(QUERY_SPOUT_PARALLELISM)
                 .checkIf(Validator::isPositiveInt)
                 .defaultTo(DEFAULT_QUERY_SPOUT_PARALLELISM)
                 .castTo(Validator::asInt);
        VALIDATOR.define(QUERY_SPOUT_CPU_LOAD)
                 .checkIf(Validator::isPositive)
                 .checkIf(Validator::isFloat)
                 .defaultTo(DEFAULT_QUERY_SPOUT_CPU_LOAD)
                 .castTo(Validator::asDouble);
        VALIDATOR.define(QUERY_SPOUT_MEMORY_ON_HEAP_LOAD)
                 .checkIf(Validator::isPositive)
                 .checkIf(Validator::isFloat)
                 .defaultTo(DEFAULT_QUERY_SPOUT_MEMORY_ON_HEAP_LOAD)
                 .castTo(Validator::asDouble);
        VALIDATOR.define(QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD)
                 .checkIf(Validator::isPositive)
                 .checkIf(Validator::isFloat)
                 .defaultTo(DEFAULT_QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD)
                 .castTo(Validator::asDouble);

        VALIDATOR.define(TICK_SPOUT_CPU_LOAD)
                .checkIf(Validator::isPositive)
                .checkIf(Validator::isFloat)
                .defaultTo(DEFAULT_TICK_SPOUT_CPU_LOAD)
                .castTo(Validator::asDouble);
        VALIDATOR.define(TICK_SPOUT_MEMORY_ON_HEAP_LOAD)
                .checkIf(Validator::isPositive)
                .checkIf(Validator::isFloat)
                .defaultTo(DEFAULT_TICK_SPOUT_MEMORY_ON_HEAP_LOAD)
                .castTo(Validator::asDouble);
        VALIDATOR.define(TICK_SPOUT_MEMORY_OFF_HEAP_LOAD)
                .checkIf(Validator::isPositive)
                .checkIf(Validator::isFloat)
                .defaultTo(DEFAULT_TICK_SPOUT_MEMORY_OFF_HEAP_LOAD)
                .castTo(Validator::asDouble);

        VALIDATOR.define(FILTER_BOLT_PARALLELISM)
                 .checkIf(Validator::isPositiveInt)
                 .defaultTo(DEFAULT_FILTER_BOLT_PARALLELISM)
                 .castTo(Validator::asInt);
        VALIDATOR.define(FILTER_BOLT_CPU_LOAD)
                 .checkIf(Validator::isPositive)
                 .checkIf(Validator::isFloat)
                 .defaultTo(DEFAULT_FILTER_BOLT_CPU_LOAD)
                 .castTo(Validator::asDouble);
        VALIDATOR.define(FILTER_BOLT_MEMORY_ON_HEAP_LOAD)
                 .checkIf(Validator::isPositive)
                 .checkIf(Validator::isFloat)
                 .defaultTo(DEFAULT_FILTER_BOLT_MEMORY_ON_HEAP_LOAD)
                 .castTo(Validator::asDouble);
        VALIDATOR.define(FILTER_BOLT_MEMORY_OFF_HEAP_LOAD)
                 .checkIf(Validator::isPositive)
                 .checkIf(Validator::isFloat)
                 .defaultTo(DEFAULT_FILTER_BOLT_MEMORY_OFF_HEAP_LOAD)
                 .castTo(Validator::asDouble);

        VALIDATOR.define(JOIN_BOLT_PARALLELISM)
                 .checkIf(Validator::isPositiveInt)
                 .defaultTo(DEFAULT_JOIN_BOLT_PARALLELISM)
                 .castTo(Validator::asInt);
        VALIDATOR.define(JOIN_BOLT_CPU_LOAD)
                 .checkIf(Validator::isPositive)
                 .checkIf(Validator::isFloat)
                 .defaultTo(DEFAULT_JOIN_BOLT_CPU_LOAD)
                 .castTo(Validator::asDouble);
        VALIDATOR.define(JOIN_BOLT_MEMORY_ON_HEAP_LOAD)
                 .checkIf(Validator::isPositive)
                 .checkIf(Validator::isFloat)
                 .defaultTo(DEFAULT_JOIN_BOLT_MEMORY_ON_HEAP_LOAD)
                 .castTo(Validator::asDouble);
        VALIDATOR.define(JOIN_BOLT_MEMORY_OFF_HEAP_LOAD)
                 .checkIf(Validator::isPositive)
                 .checkIf(Validator::isFloat)
                 .defaultTo(DEFAULT_JOIN_BOLT_MEMORY_OFF_HEAP_LOAD)
                 .castTo(Validator::asDouble);

        VALIDATOR.define(RESULT_BOLT_PARALLELISM)
                 .checkIf(Validator::isPositiveInt)
                 .defaultTo(DEFAULT_RESULT_BOLT_PARALLELISM)
                 .castTo(Validator::asInt);
        VALIDATOR.define(RESULT_BOLT_CPU_LOAD)
                 .checkIf(Validator::isPositive)
                 .checkIf(Validator::isFloat)
                 .defaultTo(DEFAULT_RESULT_BOLT_CPU_LOAD)
                 .castTo(Validator::asDouble);
        VALIDATOR.define(RESULT_BOLT_MEMORY_ON_HEAP_LOAD)
                 .checkIf(Validator::isPositive)
                 .checkIf(Validator::isFloat)
                 .defaultTo(DEFAULT_RESULT_BOLT_MEMORY_ON_HEAP_LOAD)
                 .castTo(Validator::asDouble);
        VALIDATOR.define(RESULT_BOLT_MEMORY_OFF_HEAP_LOAD)
                 .checkIf(Validator::isPositive)
                 .checkIf(Validator::isFloat)
                 .defaultTo(DEFAULT_RESULT_BOLT_MEMORY_OFF_HEAP_LOAD)
                 .castTo(Validator::asDouble);

        VALIDATOR.define(LOOP_BOLT_PARALLELISM)
                .checkIf(Validator::isPositiveInt)
                .defaultTo(DEFAULT_LOOP_BOLT_PARALLELISM)
                .castTo(Validator::asInt);
        VALIDATOR.define(LOOP_BOLT_CPU_LOAD)
                .checkIf(Validator::isPositive)
                .checkIf(Validator::isFloat)
                .defaultTo(DEFAULT_LOOP_BOLT_CPU_LOAD)
                .castTo(Validator::asDouble);
        VALIDATOR.define(LOOP_BOLT_MEMORY_ON_HEAP_LOAD)
                .checkIf(Validator::isPositive)
                .checkIf(Validator::isFloat)
                .defaultTo(DEFAULT_LOOP_BOLT_MEMORY_ON_HEAP_LOAD)
                .castTo(Validator::asDouble);
        VALIDATOR.define(LOOP_BOLT_MEMORY_OFF_HEAP_LOAD)
                .checkIf(Validator::isPositive)
                .checkIf(Validator::isFloat)
                .defaultTo(DEFAULT_LOOP_BOLT_MEMORY_OFF_HEAP_LOAD)
                .castTo(Validator::asDouble);

        VALIDATOR.define(TICK_SPOUT_INTERVAL)
                 .checkIf(Validator::isPositiveInt)
                 .checkIf(Validator.isInRange(TICK_INTERVAL_MINIMUM, Double.POSITIVE_INFINITY))
                 .defaultTo(DEFAULT_TICK_SPOUT_INTERVAL)
                 .castTo(Validator::asInt);

        VALIDATOR.define(FILTER_BOLT_STATS_REPORT_TICKS)
                 .checkIf(Validator::isPositiveInt)
                 .defaultTo(DEFAULT_FILTER_BOLT_STATS_REPORT_TICKS)
                 .castTo(Validator::asInt);

        VALIDATOR.define(JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS)
                 .checkIf(Validator::isPositiveInt)
                 .defaultTo(DEFAULT_JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS)
                 .castTo(Validator::asInt);

        VALIDATOR.define(JOIN_BOLT_WINDOW_PRE_START_DELAY_TICKS)
                 .checkIf(Validator::isPositiveInt)
                 .defaultTo(DEFAULT_JOIN_BOLT_QUERY_PRE_START_DELAY_TICKS)
                 .castTo(Validator::asInt);

        VALIDATOR.define(LOOP_BOLT_PUBSUB_OVERRIDES)
                 .checkIf(Validator::isMap)
                 .checkIf(BulletStormConfig::isMapWithStringKeys)
                 .defaultTo(DEFAULT_LOOP_BOLT_PUBSUB_OVERRIDES);

        VALIDATOR.relate("Built-in metrics enabled but no intervals provided", TOPOLOGY_METRICS_BUILT_IN_ENABLE,
                          TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING)
                 .checkIf(BulletStormConfig::areNeededIntervalsProvided);

        VALIDATOR.evaluate("Minimum window emit every should be >= pre-start buffer delay + " + PRE_START_DELAY_BUFFER_TICKS + " ticks",
                           TICK_SPOUT_INTERVAL, BulletStormConfig.JOIN_BOLT_WINDOW_PRE_START_DELAY_TICKS,
                           BulletConfig.WINDOW_MIN_EMIT_EVERY)
                 .checkIf(BulletStormConfig::isStartDelayEnough)
                 .orFail();
    }

    /**
     * Constructor that loads the defaults.
     */
    public BulletStormConfig() {
        super(DEFAULT_STORM_CONFIGURATION);
        VALIDATOR.validate(this);
    }

    /**
     * Constructor that loads specific file augmented with defaults.
     *
     * @param file YAML file to load.
     */
    public BulletStormConfig(String file) {
        this(new Config(file));
    }

    /**
     * Constructor that loads the defaults and augments it with defaults.
     *
     * @param other The other config to wrap.
     */
    public BulletStormConfig(Config other) {
        // Load Bullet and Storm defaults. Then merge the other.
        super(DEFAULT_STORM_CONFIGURATION);
        merge(other);
        VALIDATOR.validate(this);
    }

    @Override
    public BulletStormConfig validate() {
        VALIDATOR.validate(this);
        return this;
    }

    /**
     * Returns a copy of the {@link Validator} used by this config. This validator also includes the definitions
     * in the {@link BulletConfig#getValidator()} validator.
     *
     * @return The validator used by this class.
     */
    public static Validator getValidator() {
        return VALIDATOR.copy();
    }

    /**
     * Gets all the custom settings defined with {@link #CUSTOM_STORM_SETTING_PREFIX}. The prefix is removed.
     *
     * @return A {@link Map} of these custom settings.
     */
    public Map<String, Object> getCustomStormSettings() {
        return getAllWithPrefix(Optional.empty(), CUSTOM_STORM_SETTING_PREFIX, true);
    }

    @SuppressWarnings("unchecked")
    private static boolean isMetricMapping(Object metricMap) {
        try {
            Map<String, Number> map = (Map<String, Number>) metricMap;
            return map.entrySet().stream().allMatch(BulletStormConfig::isMetricInterval);
        } catch (ClassCastException e) {
            log.warn("Interval mapping is not a map of metric string names: {} to numbers", BUILT_IN_METRICS);
            return false;
        }
    }

    private static boolean isMetricInterval(Map.Entry<String, Number> entry) {
        String metric = entry.getKey();
        Number interval = entry.getValue();
        if (!BUILT_IN_METRICS.contains(metric) || !Validator.isPositiveInt(interval)) {
            log.warn("{} is not a valid metric interval mapping. Supported metrics: {}", entry, BUILT_IN_METRICS);
            return false;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static boolean areMetricsConsumerClasses(Object metricClassList) {
        try {
            List<String> classes = (List<String>) metricClassList;
            return classes.stream().allMatch(ReflectionUtils::isIMetricsConsumer);
        } catch (ClassCastException e) {
            log.warn("Metrics classes is not provided as a list of strings: {}", metricClassList);
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static boolean isMapWithStringKeys(Object maybeMap) {
        try {
            Map<String, Object> map = (Map<String, Object>) maybeMap;
            return map.keySet().stream().noneMatch(String::isEmpty);
        } catch (ClassCastException e) {
            log.warn("{} is not a valid map of non-empty strings to objects", maybeMap);
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static boolean areNeededIntervalsProvided(Object builtInEnable, Object intervalMapping) {
        boolean enabled = (boolean) builtInEnable;
        // return false when enabled and map is empty
        return !(enabled && Utilities.isEmpty((Map<String, Number>) intervalMapping));
    }

    @SuppressWarnings("unchecked")
    private static boolean isStartDelayEnough(List<Object> objects) {
        int tickInterval = (Integer) objects.get(0);
        int preStartDelayTicks = (Integer) objects.get(1);
        int minEmitEvery = (Integer) objects.get(2);

        return minEmitEvery >= tickInterval * (preStartDelayTicks + PRE_START_DELAY_BUFFER_TICKS);
    }

}
