/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

public class BulletStormConfigTest {
    @Test
    public void testNoFiles() throws IOException {
        BulletStormConfig config = new BulletStormConfig(null);
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_NAME), "bullet-topology");
        Assert.assertEquals(config.get(BulletStormConfig.SPECIFICATION_MAX_DURATION), 120000L);
        Assert.assertEquals(config.get(BulletStormConfig.AGGREGATION_MAX_SIZE), 512L);
        config = new BulletStormConfig("");
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_NAME), "bullet-topology");
    }

    @Test
    public void testCustomConfig() throws IOException {
        BulletStormConfig config = new BulletStormConfig("src/test/resources/test_config.yaml");
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_NAME), "test");
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_FUNCTION), "foo");
        Assert.assertEquals(config.get(BulletStormConfig.SPECIFICATION_MAX_DURATION), 10000L);
        Assert.assertEquals(config.get("fake.setting"), "bar");

        // Defaulted
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_WORKERS), 92L);
        Assert.assertEquals(config.get(BulletStormConfig.AGGREGATION_MAX_SIZE), 512L);
        Assert.assertEquals(config.get(BulletStormConfig.DRPC_SPOUT_CPU_LOAD), 20.0);
    }

    @Test
    public void testGettingNonStormSettingsOnly() throws IOException {
        BulletStormConfig config = new BulletStormConfig(null);
        Map<String, Object> settings = config.getNonTopologySubmissionSettings();
        BulletStormConfig.TOPOLOGY_SUBMISSION_SETTINGS.stream().forEach(s -> Assert.assertFalse(settings.containsKey(s)));
        Assert.assertTrue(settings.size() > 0);
    }
}
