/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import backtype.storm.tuple.Fields;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.storm.testing.ComponentUtils;
import com.yahoo.bullet.storm.testing.CustomEmitter;
import com.yahoo.bullet.storm.testing.CustomOutputFieldsDeclarer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

public class DSLSpoutTest {
    private DSLSpout dslSpout;
    private BulletStormConfig config;
    private CustomEmitter emitter;

    @BeforeMethod
    public void setup() {
        emitter = new CustomEmitter();
        config = new BulletStormConfig("src/test/resources/test_dsl_config.yaml");
        dslSpout = ComponentUtils.open(new DSLSpout(config), emitter);
        dslSpout.activate();
    }

    @Test
    public void testNextTuple() {
        dslSpout.nextTuple();

        // MockConnector reads out a convertible map, an inconvertible map, and a null
        Assert.assertEquals(emitter.getEmitted().size(), 1);

        BulletRecord record = (BulletRecord) emitter.getEmitted().get(0).getTuple().get(TopologyConstants.RECORD_POSITION);

        Assert.assertEquals(record.get("foo"), "bar");

        // connector throws
        dslSpout.nextTuple();

        // nothing new emitted
        Assert.assertEquals(emitter.getEmitted().size(), 1);
    }

    @Test
    public void testNextTupleWithDeserialize() {
        config.set(BulletStormConfig.DSL_DESERIALIZER_ENABLE, true);

        dslSpout = ComponentUtils.open(new DSLSpout(config), emitter);
        dslSpout.activate();
        dslSpout.nextTuple();

        // MockDeserializer changes key "foo" to "bar"
        Assert.assertEquals(emitter.getEmitted().size(), 1);

        BulletRecord record = (BulletRecord) emitter.getEmitted().get(0).getTuple().get(TopologyConstants.RECORD_POSITION);

        Assert.assertNull(record.get("foo"));
        Assert.assertEquals(record.get("bar"), "bar");
    }

    @Test
    public void testNextTupleWithDSLBolt() {
        config.set(BulletStormConfig.DSL_BOLT_ENABLE, true);

        dslSpout = ComponentUtils.open(new DSLSpout(config), emitter);
        dslSpout.activate();
        dslSpout.nextTuple();

        Assert.assertEquals(emitter.getEmitted().size(), 1);

        List<Object> objects = (List<Object>) emitter.getEmitted().get(0).getTuple().get(TopologyConstants.RECORD_POSITION);
        Assert.assertEquals(objects.size(), 2);
    }

    @Test
    public void testDeclareOutputFields() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        dslSpout.declareOutputFields(declarer);

        Fields expectedFields = new Fields(TopologyConstants.RECORD_FIELD, TopologyConstants.RECORD_TIMESTAMP_FIELD);
        Assert.assertTrue(declarer.areFieldsPresent(expectedFields));
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Could not open DSLSpout\\.")
    public void testOpen() {
        // coverage - second MockConnector initialize() will throw
        dslSpout.open(null, null, null);
    }

    @Test
    public void testDeactivate() {
        // coverage - does nothing
        dslSpout.deactivate();
    }

    @Test
    public void testClose() {
        // coverage - second MockConnector close() will throw
        dslSpout.close();
        dslSpout.close();
    }

    @Test
    public void testAck() {
        // coverage - does nothing
        dslSpout.ack(null);
    }

    @Test
    public void testFail() {
        // coverage - does nothing
        dslSpout.fail(null);
    }
}
