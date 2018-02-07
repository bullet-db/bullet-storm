/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.storm.BulletStormConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DRPCConfigTest {
    @Test
    public void testCustomConfig() {
        DRPCConfig config = new DRPCConfig("src/test/resources/test_drpc_config.yaml");
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_NAME), "bullet-topology");
        Assert.assertEquals(config.get(BulletStormConfig.QUERY_MAX_DURATION), Long.MAX_VALUE);
        Assert.assertEquals(config.get("fake.setting"), "foo");

        // Defaulted
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_ENABLE), false);
        Assert.assertEquals(config.get(BulletStormConfig.AGGREGATION_MAX_SIZE), BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE);
        Assert.assertEquals(config.get(BulletStormConfig.QUERY_SPOUT_CPU_LOAD), BulletStormConfig.DEFAULT_QUERY_SPOUT_CPU_LOAD);
    }
}
