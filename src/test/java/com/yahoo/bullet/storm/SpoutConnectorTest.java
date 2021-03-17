/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.storm.testing.CallCountingCredentialsSpout;
import com.yahoo.bullet.storm.testing.CallCountingSpout;
import com.yahoo.bullet.storm.testing.CallCountingSpoutConnector;
import com.yahoo.bullet.storm.testing.CustomTopologyContext;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;

public class SpoutConnectorTest {
    private CallCountingSpoutConnector connector;

    @BeforeMethod
    public void setup() {
        BulletStormConfig config = new BulletStormConfig();
        config.set(BulletStormConfig.DSL_SPOUT_CONNECTOR_CLASS_NAME, CallCountingCredentialsSpout.class.getName());
        connector = new CallCountingSpoutConnector(config);
    }

    @Test
    public void testCreation() {
        Assert.assertNotNull(connector.getProxy());
    }

    @Test
    public void testAsSpout() {
        connector.getComponentConfiguration();
        connector.declareOutputFields(null);
        connector.setCredentials(null);
        connector.open(null, null, null);
        connector.activate();
        connector.nextTuple();
        connector.ack(null);
        connector.fail(null);
        connector.deactivate();
        CallCountingSpout spout = connector.getProxy();
        Assert.assertEquals(spout.getConfigurationCalls(), 1);
        Assert.assertEquals(spout.getDeclareCalls(), 1);
        Assert.assertEquals(spout.getCredentialCalls(), 1);
        Assert.assertEquals(spout.getOpenCalls(), 1);
        Assert.assertEquals(spout.getActivateCalls(), 1);
        Assert.assertEquals(spout.getNextTupleCalls(), 1);
        Assert.assertEquals(spout.getAckCalls(), 1);
        Assert.assertEquals(spout.getFailCalls(), 1);
        Assert.assertEquals(spout.getDeactivateCalls(), 1);
    }

    @Test
    public void testAsNoCredentialsSpout() {
        BulletStormConfig config = new BulletStormConfig();
        config.set(BulletStormConfig.DSL_SPOUT_CONNECTOR_CLASS_NAME, CallCountingSpout.class.getName());
        connector = new CallCountingSpoutConnector(config);

        connector.getComponentConfiguration();
        connector.declareOutputFields(null);
        connector.setCredentials(null);
        connector.open(null, null, null);
        connector.activate();
        connector.nextTuple();
        connector.ack(null);
        connector.fail(null);
        connector.deactivate();
        CallCountingSpout spout = connector.getProxy();
        Assert.assertEquals(spout.getConfigurationCalls(), 1);
        Assert.assertEquals(spout.getDeclareCalls(), 1);
        Assert.assertEquals(spout.getOpenCalls(), 1);
        Assert.assertEquals(spout.getActivateCalls(), 1);
        Assert.assertEquals(spout.getCredentialCalls(), 0);
        Assert.assertEquals(spout.getNextTupleCalls(), 1);
        Assert.assertEquals(spout.getAckCalls(), 1);
        Assert.assertEquals(spout.getFailCalls(), 1);
        Assert.assertEquals(spout.getDeactivateCalls(), 1);
    }

    @Test
    public void testProxyingToSpout() {
        Map<String, Object> config = Collections.emptyMap();
        TopologyContext context = new CustomTopologyContext();
        SpoutOutputCollector collector = new SpoutOutputCollector(null);

        connector.initialize();
        connector.close();
        CallCountingSpout spout = connector.getProxy();
        Assert.assertEquals(spout.getConfigurationCalls(), 0);
        Assert.assertEquals(spout.getDeclareCalls(), 0);
        Assert.assertEquals(spout.getCredentialCalls(), 0);
        Assert.assertEquals(spout.getOpenCalls(), 1);
        Assert.assertEquals(spout.getActivateCalls(), 1);
        Assert.assertEquals(spout.getNextTupleCalls(), 0);
        Assert.assertEquals(spout.getAckCalls(), 0);
        Assert.assertEquals(spout.getFailCalls(), 0);
        Assert.assertEquals(spout.getDeactivateCalls(), 1);

        connector.open(config, context, collector);
        Assert.assertEquals(spout.getOpenCalls(), 2);
        Assert.assertEquals(spout.getActivateCalls(), 1);
        Assert.assertSame(connector.getOpenMap(), config);
        Assert.assertSame(connector.getOpenContext(), context);
        Assert.assertSame(connector.getOpenCollector(), collector);

        config = Collections.emptyMap();
        context = new CustomTopologyContext();
        collector = new SpoutOutputCollector(null);
        connector.setStormConfiguration(config);
        connector.setContext(context);
        connector.setOutputCollector(collector);
        Assert.assertSame(connector.getOpenMap(), config);
        Assert.assertSame(connector.getOpenContext(), context);
        Assert.assertSame(connector.getOpenCollector(), collector);
    }
}
