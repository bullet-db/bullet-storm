/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.Config;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.storm.drpc.utils.DRPCOutputCollector;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.yahoo.bullet.storm.drpc.MockDRPCSpout.makeReturnInfo;

public class DRPCResultPublisherTest {
    private DRPCResultPublisher publisher;
    private DRPCOutputCollector collector;
    private MockReturnResults injectedMockBolt;

    @BeforeMethod
    public void setup() {
        DRPCConfig config = new DRPCConfig("src/test/resources/test_drpc_config.yaml");

        Map stormConfig = new Config("src/test/resources/test_storm_config.yaml").getAll(Optional.empty());
        config.set(DRPCConfig.STORM_CONFIG, stormConfig);

        publisher = new DRPCResultPublisher(config);
        collector = publisher.getCollector();
        // Override the ReturnResults with our own that uses our collector and fails every two tuples
        injectedMockBolt = new MockReturnResults(collector, 5);
        publisher.setBolt(injectedMockBolt);
    }

    @Test
    public void testSending() throws Exception {
        Assert.assertEquals(injectedMockBolt.getCount(), 0);
        PubSubMessage message = new PubSubMessage("foo", "{}", new Metadata(null, makeReturnInfo("a", "testHost", 80)));
        publisher.send(message);
        Assert.assertEquals(injectedMockBolt.getCount(), 1);
        Assert.assertTrue(collector.isAcked());
        Assert.assertFalse(collector.isFailed());
        // Output is no longer present
        Assert.assertFalse(collector.haveOutput());
        Assert.assertNull(collector.reset());

        message = new PubSubMessage("bar", "{}", new Metadata(null, makeReturnInfo("b", "testHost", 80)));
        publisher.send(message);
        Assert.assertEquals(injectedMockBolt.getCount(), 2);
        Assert.assertTrue(collector.isAcked());
        Assert.assertFalse(collector.isFailed());
        Assert.assertFalse(collector.haveOutput());
        Assert.assertNull(collector.reset());
    }

    @Test(expectedExceptions = PubSubException.class)
    public void testFailing() throws Exception {
        injectedMockBolt.setFailNumber(1);
        Assert.assertEquals(injectedMockBolt.getCount(), 0);
        PubSubMessage message = new PubSubMessage("foo", "{}", new Metadata(null, makeReturnInfo("a", "testHost", 80)));
        publisher.send(message);
    }

    @Test
    public void testCleaningUp() {
        Assert.assertFalse(injectedMockBolt.isCleanedUp());
        publisher.close();
        Assert.assertTrue(injectedMockBolt.isCleanedUp());
    }
}
