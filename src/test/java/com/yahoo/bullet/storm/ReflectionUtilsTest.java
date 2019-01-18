/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.storm.testing.CustomIMetricsConsumer;
import com.yahoo.bullet.storm.testing.CustomIRichBolt;
import com.yahoo.bullet.storm.testing.CustomIRichSpout;
import com.yahoo.bullet.storm.testing.TestBolt;
import com.yahoo.bullet.storm.testing.TestSpout;
import org.apache.storm.Config;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

public class ReflectionUtilsTest {
    @Test(expectedExceptions = ClassNotFoundException.class)
    public void testGettingNonExistentSpout() throws Exception {
        ReflectionUtils utils = new ReflectionUtils();
        utils.getSpout("does.not.exist", null);
    }

    @Test
    public void testGettingSpoutWithDefaultConstructor() throws Exception {
        IRichSpout spout = ReflectionUtils.getSpout(CustomIRichSpout.class.getName(), null);
        Assert.assertTrue(spout instanceof CustomIRichSpout);
    }

    @Test
    public void testGettingSpoutWithArguments() throws Exception {
        IRichSpout spout = ReflectionUtils.getSpout(TestSpout.class.getName(), Collections.singletonList("foo"));
        Assert.assertTrue(spout instanceof TestSpout);
        Assert.assertEquals(((TestSpout) spout).getArgs(), Collections.singletonList("foo"));
    }

    @Test(expectedExceptions = ClassNotFoundException.class)
    public void testGettingNonExistentBolt() throws Exception {
        ReflectionUtils utils = new ReflectionUtils();
        utils.getBolt("does.not.exist", null);
    }

    @Test
    public void testGettingBoltWithDefaultConstructor() throws Exception {
        IRichBolt bolt = ReflectionUtils.getBolt(CustomIRichBolt.class.getName(), null);
        Assert.assertTrue(bolt instanceof CustomIRichBolt);
    }

    @Test
    public void testGettingBoltWithArguments() throws Exception {
        IRichBolt bolt = ReflectionUtils.getBolt(TestBolt.class.getName(), Collections.singletonList("foo"));
        Assert.assertTrue(bolt instanceof TestBolt);
        Assert.assertEquals(((TestBolt) bolt).getArgs(), Collections.singletonList("foo"));
    }

    @Test
    public void testIsIMetricsConsumer() {
        Assert.assertFalse(ReflectionUtils.isIMetricsConsumer(LoggingMetricsConsumer.class.getName()));
        Assert.assertTrue(ReflectionUtils.isIMetricsConsumer(CustomIMetricsConsumer.class.getName()));
    }

    @Test
    public void testRegisteringIMetricsConsumer() {
        Config config = new Config();
        BulletStormConfig bulletStormConfig = new BulletStormConfig();
        Assert.assertNull(bulletStormConfig.get(CustomIMetricsConsumer.CUSTOM_METRICS_REGISTERED));

        ReflectionUtils.registerMetricsConsumer(LoggingMetricsConsumer.class.getName(), config, bulletStormConfig);
        Assert.assertNull(config.get(Config.TOPOLOGY_METRICS_CONSUMER_REGISTER));

        ReflectionUtils.registerMetricsConsumer(CustomIMetricsConsumer.class.getName(), config, bulletStormConfig);
        Assert.assertNotNull(config.get(Config.TOPOLOGY_METRICS_CONSUMER_REGISTER));
        Assert.assertTrue((Boolean) bulletStormConfig.get(CustomIMetricsConsumer.CUSTOM_METRICS_REGISTERED));
    }
}
