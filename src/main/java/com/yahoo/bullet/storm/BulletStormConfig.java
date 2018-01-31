/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Config;
import com.yahoo.bullet.common.Validator;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.metric.api.IMetricsConsumer;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.yahoo.bullet.storm.TopologyConstants.BUILT_IN_METRICS;
import static java.util.Arrays.asList;

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
    public static final String TICK_SPOUT_INTERVAL = "bullet.topology.tick.spout.interval";
    public static final String JOIN_BOLT_WINDOW_TICK_TIMEOUT = "bullet.topology.join.bolt.window.tick.timeout";

    // Defaults

    public static final String DEFAULT_TOPOLOGY_NAME = "bullet-topology";
    public static final boolean DEFAULT_TOPOLOGY_METRICS_ENABLE = false;
    public static final boolean DEFAULT_TOPOLOGY_METRICS_BUILT_IN_ENABLE = false;
    public static final Map<String, Number> DEFAULT_TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING = new HashMap<>();
    public static final String DEFAULT_BUILT_IN_METRICS_INTERVAL_KEY = "default";
    static {
        DEFAULT_TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING.put("bullet_active_queries", 10);
        DEFAULT_TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING.put(DEFAULT_BUILT_IN_METRICS_INTERVAL_KEY, 10);
    }
    public static final List<String> DEFAULT_TOPOLOGY_METRICS_CLASSES = new ArrayList<>();
    static {
        DEFAULT_TOPOLOGY_METRICS_CLASSES.add(SigarLoggingMetricsConsumer.class.getName());
    }
    public static final int DEFAULT_QUERY_SPOUT_PARALLELISM = 2;
    public static final double DEFAULT_QUERY_SPOUT_CPU_LOAD = 20.0;
    public static final double DEFAULT_QUERY_SPOUT_MEMORY_ON_HEAP_LOAD = 256.0;
    public static final double DEFAULT_QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD = 160.0;
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
    public static final double DEFAULT_RESULT_BOLT_MEMORY_ON_HEAP_LOAD = 128.0;
    public static final double DEFAULT_RESULT_BOLT_MEMORY_OFF_HEAP_LOAD = 160.0;
    public static final int DEFAULT_TICK_SPOUT_INTERVAL = 200;
    public static final int DEFAULT_JOIN_BOLT_WINDOW_TICK_TIMEOUT = 5;

    //  Validations

    private static final Validator VALIDATOR = new Validator();
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

        VALIDATOR.define(TICK_SPOUT_INTERVAL)
                 .checkIf(Validator::isPositiveInt)
                 .defaultTo(DEFAULT_TICK_SPOUT_INTERVAL)
                 .castTo(Validator::asInt);

        VALIDATOR.define(JOIN_BOLT_WINDOW_TICK_TIMEOUT)
                 .checkIf(Validator::isPositiveInt)
                 .defaultTo(DEFAULT_JOIN_BOLT_WINDOW_TICK_TIMEOUT)
                 .castTo(Validator::asInt);

        VALIDATOR.relate("Minimum window emit every should be >= 2 * tick interval", TICK_SPOUT_INTERVAL, BulletConfig.WINDOW_MIN_EMIT_EVERY)
                 .checkIf(BulletStormConfig::isAtleastDouble);
    }

    // Other constants

    // Used automatically by the Storm code. Not for user setting.
    // This is the key to place the Storm configuration as
    public static final String STORM_CONFIG = "bullet.topology.storm.config";
    // This is the key to place the TopologyContext as
    public static final String STORM_CONTEXT = "bullet.topology.storm.context";

    // The name of the register method in a custom metrics class.
    public static final String REGISTER_METHOD = "register";

    public static Set<String> TOPOLOGY_SUBMISSION_SETTINGS =
        new HashSet<>(asList(QUERY_SPOUT_PARALLELISM, QUERY_SPOUT_CPU_LOAD, QUERY_SPOUT_MEMORY_ON_HEAP_LOAD,
                             QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD, FILTER_BOLT_PARALLELISM, FILTER_BOLT_CPU_LOAD,
                             FILTER_BOLT_MEMORY_ON_HEAP_LOAD, FILTER_BOLT_MEMORY_OFF_HEAP_LOAD, JOIN_BOLT_PARALLELISM,
                             JOIN_BOLT_CPU_LOAD, JOIN_BOLT_MEMORY_ON_HEAP_LOAD, JOIN_BOLT_MEMORY_OFF_HEAP_LOAD,
                             RESULT_BOLT_PARALLELISM, RESULT_BOLT_CPU_LOAD, RESULT_BOLT_MEMORY_ON_HEAP_LOAD,
                             RESULT_BOLT_MEMORY_OFF_HEAP_LOAD, TOPOLOGY_NAME, TOPOLOGY_METRICS_ENABLE));

    public static final String DEFAULT_STORM_CONFIGURATION = "bullet_storm_defaults.yaml";

    /**
     * Constructor that loads specific file augmented with defaults.
     *
     * @param file YAML file to load.
     */
    public BulletStormConfig(String file) {
        this(new Config(file));
    }

    /**
     * Constructor that loads the defaults and augments it with defaults. *
     * @param other The other config to wrap.
     */
    public BulletStormConfig(Config other) {
        // Load Bullet and Storm defaults. Then merge the other.
        super(DEFAULT_STORM_CONFIGURATION);
        merge(other);
        // Call validate since we merged
        validate();
        log.info("Settings:\n {}", this.toString());
    }

    @Override
    public BulletConfig validate() {
        super.validate();
        VALIDATOR.validate(this);
        return this;
    }

    /**
     * Gets all the settings besides the {@link #TOPOLOGY_SUBMISSION_SETTINGS}.
     *
     * @return A {@link Map} of the other settings.
     */
    public Map<String, Object> getNonTopologySubmissionSettings() {
        return getAllBut(Optional.of(TOPOLOGY_SUBMISSION_SETTINGS));
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
            return classes.stream().allMatch(BulletStormConfig::isIMetricsConsumer);
        } catch (ClassCastException e) {
            log.warn("Metrics classes is not provided as a list of strings: {}", metricClassList);
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static boolean isIMetricsConsumer(String clazz) {
        try {
            Class<? extends IMetricsConsumer> consumer = (Class<? extends IMetricsConsumer>) Class.forName(clazz);
            Method method = consumer.getMethod(REGISTER_METHOD, Config.class, BulletStormConfig.class);
            if (method == null) {
                log.warn("The {} method was not found in class: {}", REGISTER_METHOD, clazz);
                return false;
            }
        } catch (Exception e) {
            log.warn("The given class: {} was not a proper IMetricsConsumer with a {} method", clazz, REGISTER_METHOD);
            log.warn("Exception: {}", e);
            return false;
        }
        return true;
    }

    private static boolean isAtleastDouble(Object minEvery, Object tickInterval) {
        return ((Number) minEvery).doubleValue() >= 2.0 * ((Number) tickInterval).doubleValue();
    }
}
