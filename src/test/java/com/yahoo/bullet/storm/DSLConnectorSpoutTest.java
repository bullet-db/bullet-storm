/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.storm.testing.CallCountingCredentialsSpout;
import com.yahoo.bullet.storm.testing.CallCountingSpout;
import com.yahoo.bullet.storm.testing.CallCountingSpoutConnector;
import com.yahoo.bullet.storm.testing.CustomEmitter;
import com.yahoo.bullet.storm.testing.CustomOutputFieldsDeclarer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DSLConnectorSpoutTest {
    private DSLConnectorSpout spout;
    private CallCountingSpoutConnector connector;

    @BeforeMethod
    public void setup() {
        BulletStormConfig config = new BulletStormConfig("test_dsl_config.yaml");
        config.set(BulletDSLConfig.CONNECTOR_CLASS_NAME, CallCountingSpoutConnector.class.getName());
        config.set(BulletStormConfig.DSL_SPOUT_CONNECTOR_CLASS_NAME, CallCountingCredentialsSpout.class.getName());
        spout = new DSLConnectorSpout(config);
        connector = (CallCountingSpoutConnector) spout.connector;
    }

    @Test
    public void testCreation() {
        CallCountingSpout spout = connector.getProxy();
        Assert.assertEquals(spout.getConfigurationCalls(), 0);
        Assert.assertEquals(spout.getDeclareCalls(), 0);
        Assert.assertEquals(spout.getCredentialCalls(), 0);
        Assert.assertEquals(spout.getOpenCalls(), 0);
        Assert.assertEquals(spout.getActivateCalls(), 0);
        Assert.assertEquals(spout.getNextTupleCalls(), 0);
        Assert.assertEquals(spout.getAckCalls(), 0);
        Assert.assertEquals(spout.getFailCalls(), 0);
        Assert.assertEquals(spout.getDeactivateCalls(), 0);
    }

    @Test
    public void testProxyingOnlyCertainSpoutMethods() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        CustomEmitter emitter = new CustomEmitter();
        spout.open(null, null, new SpoutOutputCollector(emitter));
        spout.getComponentConfiguration();
        spout.declareOutputFields(declarer);
        spout.setCredentials(null);
        spout.activate();
        spout.nextTuple();
        spout.ack(null);
        spout.fail(null);
        spout.deactivate();
        CallCountingSpout spout = connector.getProxy();
        Assert.assertEquals(spout.getConfigurationCalls(), 1);
        Assert.assertEquals(spout.getDeclareCalls(), 0);
        Assert.assertEquals(spout.getCredentialCalls(), 1);
        Assert.assertEquals(spout.getOpenCalls(), 1);
        Assert.assertEquals(spout.getActivateCalls(), 1);
        Assert.assertEquals(spout.getNextTupleCalls(), 0);
        Assert.assertEquals(spout.getAckCalls(), 1);
        Assert.assertEquals(spout.getFailCalls(), 1);
        Assert.assertEquals(spout.getDeactivateCalls(), 1);
        Assert.assertEquals(emitter.getEmitted().size(), 0);
    }
}
