/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Config;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;

public class BulletStormConfigTest {
    @Test
    public void testNoFiles() {
        BulletStormConfig config = new BulletStormConfig((String) null);
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_NAME), BulletStormConfig.DEFAULT_TOPOLOGY_NAME);
        Assert.assertEquals(config.get(BulletStormConfig.QUERY_MAX_DURATION), Long.MAX_VALUE);
        Assert.assertEquals(config.get(BulletStormConfig.AGGREGATION_MAX_SIZE), 512L);
        config = new BulletStormConfig("");
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_NAME), BulletStormConfig.DEFAULT_TOPOLOGY_NAME);
    }

    @Test
    public void testCustomConfig() {
        BulletStormConfig config = new BulletStormConfig("src/test/resources/test_config.yaml");
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_NAME), "test");
        Assert.assertEquals(config.get(BulletStormConfig.QUERY_MAX_DURATION), 10000L);
        Assert.assertEquals(config.get("fake.setting"), "bar");

        // Defaulted
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_ENABLE), true);
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
        Assert.assertEquals(settings.get(BulletStormConfig.CUSTOM_STORM_SETTING_PREFIX + "storm.foo"), "bar");
        Assert.assertEquals(settings.get(BulletStormConfig.CUSTOM_STORM_SETTING_PREFIX + "bar"), "baz");
    }
}
