/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Config;
import com.yahoo.bullet.storm.testing.CustomIMetricsConsumer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class BulletStormConfigTest {
    @Test
    public void testDefaultInitialization() {
        BulletStormConfig config = new BulletStormConfig();
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_NAME), BulletStormConfig.DEFAULT_TOPOLOGY_NAME);
        Assert.assertEquals(config.get(BulletStormConfig.QUERY_MAX_DURATION), BulletStormConfig.DEFAULT_QUERY_MAX_DURATION);
        Assert.assertEquals(config.get(BulletStormConfig.AGGREGATION_MAX_SIZE), BulletStormConfig.DEFAULT_AGGREGATION_MAX_SIZE);
    }

    @Test
    public void testNoFiles() {
        BulletStormConfig config = new BulletStormConfig((String) null);
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_NAME), BulletStormConfig.DEFAULT_TOPOLOGY_NAME);
        Assert.assertEquals(config.get(BulletStormConfig.QUERY_MAX_DURATION), BulletStormConfig.DEFAULT_QUERY_MAX_DURATION);
        Assert.assertEquals(config.get(BulletStormConfig.AGGREGATION_MAX_SIZE), BulletStormConfig.DEFAULT_AGGREGATION_MAX_SIZE);
        config = new BulletStormConfig("");
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_NAME), BulletStormConfig.DEFAULT_TOPOLOGY_NAME);
    }

    @Test
    public void testCustomConfig() {
        BulletStormConfig config = new BulletStormConfig("src/test/resources/test_config.yaml");
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_NAME), "test");
        Assert.assertEquals(config.get(BulletStormConfig.QUERY_DEFAULT_DURATION), 1000L);
        Assert.assertEquals(config.get(BulletStormConfig.QUERY_MAX_DURATION), 10000L);
        Assert.assertEquals(config.get("fake.setting"), "bar");

        // Defaulted
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_ENABLE), false);
        Assert.assertEquals(config.get(BulletStormConfig.AGGREGATION_MAX_SIZE), BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE);
        Assert.assertEquals(config.get(BulletStormConfig.QUERY_SPOUT_CPU_LOAD), BulletStormConfig.DEFAULT_QUERY_SPOUT_CPU_LOAD);
    }

    @Test
    public void testGettingNonStormSettingsOnly() {
        BulletStormConfig config = new BulletStormConfig((Config) null);
        config.set(BulletStormConfig.CUSTOM_STORM_SETTING_PREFIX + "storm.foo", "bar");
        config.set(BulletStormConfig.CUSTOM_STORM_SETTING_PREFIX + "bar", "baz");
        Map<String, Object> settings = config.getCustomStormSettings();
        Assert.assertEquals(settings.size(), 2);
        Assert.assertEquals(settings.get("storm.foo"), "bar");
        Assert.assertEquals(settings.get("bar"), "baz");
    }

    @Test
    public void testInvalidMetricMapping() {
        BulletStormConfig config = new BulletStormConfig((Config) null);

        // Not present
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING, null);
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING),
                            BulletStormConfig.DEFAULT_TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING);

        // Wrong type
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING, 1L);
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING),
                                       BulletStormConfig.DEFAULT_TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING);

        // Bad interval type
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING,
                   singletonMap(TopologyConstants.LATENCY_METRIC, "foo"));
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING),
                                       BulletStormConfig.DEFAULT_TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING);

        // Bad Metric
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING,
                   singletonMap("foo", 50));
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING),
                                       BulletStormConfig.DEFAULT_TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING);

        // Proper mapping
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING,
                   singletonMap(TopologyConstants.LATENCY_METRIC, 60));
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING),
                            singletonMap(TopologyConstants.LATENCY_METRIC, 60));
    }

    @Test
    public void testIntervalMappingNotPresent() {
        BulletStormConfig config = new BulletStormConfig((Config) null);
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING, new HashMap<>());
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING),
                            BulletStormConfig.DEFAULT_TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING);

        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, false);
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING, new HashMap<>());
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING), emptyMap());
    }

    @Test
    public void testTickIntervalIsLowEnough() {
        BulletStormConfig config = new BulletStormConfig((Config) null);

        config.set(BulletStormConfig.TICK_SPOUT_INTERVAL, 1000);
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TICK_SPOUT_INTERVAL), BulletStormConfig.DEFAULT_TICK_SPOUT_INTERVAL);

        config.set(BulletStormConfig.TICK_SPOUT_INTERVAL, BulletStormConfig.DEFAULT_TICK_SPOUT_INTERVAL);
        config.set(BulletConfig.WINDOW_MIN_EMIT_EVERY, 200);
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TICK_SPOUT_INTERVAL), BulletStormConfig.DEFAULT_TICK_SPOUT_INTERVAL);
        Assert.assertEquals(config.get(BulletConfig.WINDOW_MIN_EMIT_EVERY), BulletConfig.DEFAULT_WINDOW_MIN_EMIT_EVERY);

        config.set(BulletStormConfig.TICK_SPOUT_INTERVAL, 100);
        config.set(BulletConfig.WINDOW_MIN_EMIT_EVERY, 150);
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TICK_SPOUT_INTERVAL), BulletStormConfig.DEFAULT_TICK_SPOUT_INTERVAL);
        Assert.assertEquals(config.get(BulletConfig.WINDOW_MIN_EMIT_EVERY), BulletConfig.DEFAULT_WINDOW_MIN_EMIT_EVERY);

        config.set(BulletStormConfig.TICK_SPOUT_INTERVAL, 100);
        config.set(BulletConfig.WINDOW_MIN_EMIT_EVERY, 200);
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TICK_SPOUT_INTERVAL), 100);
        Assert.assertEquals(config.get(BulletConfig.WINDOW_MIN_EMIT_EVERY), 200);

        config.set(BulletStormConfig.TICK_SPOUT_INTERVAL, 100);
        config.set(BulletConfig.WINDOW_MIN_EMIT_EVERY, 5000);
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TICK_SPOUT_INTERVAL), 100);
        Assert.assertEquals(config.get(BulletConfig.WINDOW_MIN_EMIT_EVERY), 5000);

        // Too low
        config.set(BulletStormConfig.TICK_SPOUT_INTERVAL, 1);
        config.set(BulletConfig.WINDOW_MIN_EMIT_EVERY, 5000);
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TICK_SPOUT_INTERVAL), BulletStormConfig.DEFAULT_TICK_SPOUT_INTERVAL);
        Assert.assertEquals(config.get(BulletConfig.WINDOW_MIN_EMIT_EVERY), 5000);

        config.set(BulletStormConfig.TICK_SPOUT_INTERVAL, BulletStormConfig.TICK_INTERVAL_MINIMUM);
        config.set(BulletConfig.WINDOW_MIN_EMIT_EVERY, 5000);
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TICK_SPOUT_INTERVAL), BulletStormConfig.TICK_INTERVAL_MINIMUM);
        Assert.assertEquals(config.get(BulletConfig.WINDOW_MIN_EMIT_EVERY), 5000);
    }

    @Test
    public void testLoopBoltOverridesIsAMapWithStringKeys() {
        BulletStormConfig config = new BulletStormConfig((Config) null);

        config.set(BulletStormConfig.LOOP_BOLT_PUBSUB_OVERRIDES, null);
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.LOOP_BOLT_PUBSUB_OVERRIDES), BulletStormConfig.DEFAULT_LOOP_BOLT_PUBSUB_OVERRIDES);

        config.set(BulletStormConfig.LOOP_BOLT_PUBSUB_OVERRIDES, new HashMap<>());
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.LOOP_BOLT_PUBSUB_OVERRIDES), BulletStormConfig.DEFAULT_LOOP_BOLT_PUBSUB_OVERRIDES);

        config.set(BulletStormConfig.LOOP_BOLT_PUBSUB_OVERRIDES, singletonMap(1, "foo"));
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.LOOP_BOLT_PUBSUB_OVERRIDES), BulletStormConfig.DEFAULT_LOOP_BOLT_PUBSUB_OVERRIDES);

        config.set(BulletStormConfig.LOOP_BOLT_PUBSUB_OVERRIDES, singletonMap("foo", singletonList("bar")));
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.LOOP_BOLT_PUBSUB_OVERRIDES), singletonMap("foo", singletonList("bar")));

        Map<String, Object> overrides = new HashMap<>();
        overrides.put("foo", 1L);
        overrides.put("bar", new ArrayList<>());
        config.set(BulletStormConfig.LOOP_BOLT_PUBSUB_OVERRIDES, overrides);
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.LOOP_BOLT_PUBSUB_OVERRIDES), overrides);
    }

    @Test
    public void testProperMetricsConsumers() {
        BulletStormConfig config = new BulletStormConfig((Config) null);
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_CLASSES), BulletStormConfig.DEFAULT_TOPOLOGY_METRICS_CLASSES);

        // Test removing all metrics
        config.set(BulletStormConfig.TOPOLOGY_METRICS_ENABLE, true);
        config.set(BulletStormConfig.TOPOLOGY_METRICS_CLASSES, new ArrayList<>());
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_CLASSES), new ArrayList<>());

        config.set(BulletStormConfig.TOPOLOGY_METRICS_CLASSES, singletonList(1));
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_CLASSES), BulletStormConfig.DEFAULT_TOPOLOGY_METRICS_CLASSES);

        config.set(BulletStormConfig.TOPOLOGY_METRICS_CLASSES, singletonList("foo"));
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_CLASSES), BulletStormConfig.DEFAULT_TOPOLOGY_METRICS_CLASSES);

        config.set(BulletStormConfig.TOPOLOGY_METRICS_CLASSES, singletonList(CustomIMetricsConsumer.class.getName()));
        config.validate();
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_CLASSES), singletonList(CustomIMetricsConsumer.class.getName()));
    }
}
