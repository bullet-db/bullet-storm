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
    private static DRPCConfig makeEmpty() {
        BulletStormConfig config = new BulletStormConfig();
        config.set(BulletStormConfig.WINDOW_DISABLE, true);
        return new DRPCConfig(config);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testWindowingMustBeDisabled() {
        new DRPCConfig((String) null);
    }

    @Test
    public void testDefaultInitialization() {
        DRPCConfig config = makeEmpty();

        Assert.assertEquals(config.get(DRPCConfig.DRPC_MAX_UNCOMMITED_MESSAGES), DRPCConfig.DEFAULT_DRPC_MAX_UNCOMMITED_MESSAGES);
        Assert.assertEquals(config.get(DRPCConfig.TOPOLOGY_NAME), DRPCConfig.DEFAULT_TOPOLOGY_NAME);
        Assert.assertEquals(config.get(DRPCConfig.QUERY_MAX_DURATION), DRPCConfig.DEFAULT_QUERY_MAX_DURATION);
        Assert.assertEquals(config.get(DRPCConfig.AGGREGATION_MAX_SIZE), DRPCConfig.DEFAULT_AGGREGATION_MAX_SIZE);
    }

    @Test
    public void testCustomConfig() {
        DRPCConfig config = new DRPCConfig("src/test/resources/test_drpc_config.yaml");
        Assert.assertEquals(config.get("fake.setting"), "foo");
        Assert.assertEquals(config.get(DRPCConfig.DRPC_HTTP_CONNECT_RETRY_LIMIT), 1);

        // Defaulted
        Assert.assertEquals(config.get(BulletStormConfig.TOPOLOGY_METRICS_ENABLE), false);
        Assert.assertEquals(config.get(BulletStormConfig.AGGREGATION_MAX_SIZE), BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE);
    }

    @Test
    public void testPortString() {
        DRPCConfig config = makeEmpty();

        config.set(DRPCConfig.DRPC_HTTP_PORT, null);
        config.validate();
        Assert.assertEquals(config.get(DRPCConfig.DRPC_HTTP_PORT), DRPCConfig.DEFAULT_DRPC_HTTP_PORT);

        config.set(DRPCConfig.DRPC_HTTP_PORT, "");
        config.validate();
        Assert.assertEquals(config.get(DRPCConfig.DRPC_HTTP_PORT), DRPCConfig.DEFAULT_DRPC_HTTP_PORT);

        config.set(DRPCConfig.DRPC_HTTP_PORT, "foo");
        config.validate();
        Assert.assertEquals(config.get(DRPCConfig.DRPC_HTTP_PORT), DRPCConfig.DEFAULT_DRPC_HTTP_PORT);

        config.set(DRPCConfig.DRPC_HTTP_PORT, "1.35");
        config.validate();
        Assert.assertEquals(config.get(DRPCConfig.DRPC_HTTP_PORT), DRPCConfig.DEFAULT_DRPC_HTTP_PORT);

        config.set(DRPCConfig.DRPC_HTTP_PORT, 1.35);
        config.validate();
        Assert.assertEquals(config.get(DRPCConfig.DRPC_HTTP_PORT), DRPCConfig.DEFAULT_DRPC_HTTP_PORT);

        config.set(DRPCConfig.DRPC_HTTP_PORT, -1);
        config.validate();
        Assert.assertEquals(config.get(DRPCConfig.DRPC_HTTP_PORT), DRPCConfig.DEFAULT_DRPC_HTTP_PORT);

        config.set(DRPCConfig.DRPC_HTTP_PORT, "-1");
        config.validate();
        Assert.assertEquals(config.get(DRPCConfig.DRPC_HTTP_PORT), DRPCConfig.DEFAULT_DRPC_HTTP_PORT);

        config.set(DRPCConfig.DRPC_HTTP_PORT, true);
        config.validate();
        Assert.assertEquals(config.get(DRPCConfig.DRPC_HTTP_PORT), DRPCConfig.DEFAULT_DRPC_HTTP_PORT);

        config.set(DRPCConfig.DRPC_HTTP_PORT, "35");
        config.validate();
        Assert.assertEquals(config.get(DRPCConfig.DRPC_HTTP_PORT), "35");

        config.set(DRPCConfig.DRPC_HTTP_PORT, 350);
        config.validate();
        Assert.assertEquals(config.get(DRPCConfig.DRPC_HTTP_PORT), "350");
    }
}
