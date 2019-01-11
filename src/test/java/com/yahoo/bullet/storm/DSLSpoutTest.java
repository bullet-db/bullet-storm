/*
 *  Copyright 2019, Verizon Media.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.dsl.converter.MapBulletRecordConverter;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.storm.testing.ComponentUtils;
import com.yahoo.bullet.storm.testing.CustomEmitter;
import com.yahoo.bullet.storm.testing.CustomOutputFieldsDeclarer;
import com.yahoo.bullet.storm.testing.MockConnector;
import org.apache.storm.tuple.Fields;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

public class DSLSpoutTest {
    private DSLSpout dslSpout;
    private CustomEmitter emitter;

    @BeforeMethod
    public void setup() {
        emitter = new CustomEmitter();
        dslSpout = ComponentUtils.open(new DSLSpout(new BulletStormConfig("src/test/resources/test_dsl_config.yaml")), emitter);
        dslSpout.activate();
    }

    @Test
    public void testListConstructor() {
        dslSpout = new DSLSpout(Collections.singletonList("src/test/resources/test_dsl_config.yaml"));

        Assert.assertTrue(dslSpout.getConnector() instanceof MockConnector);
        Assert.assertTrue(dslSpout.getConverter() instanceof MapBulletRecordConverter);
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
    public void testDeclareOutputFields() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        dslSpout.declareOutputFields(declarer);

        Fields expectedFields = new Fields(TopologyConstants.RECORD_FIELD, TopologyConstants.RECORD_TIMESTAMP_FIELD);
        Assert.assertTrue(declarer.areFieldsPresent(expectedFields));
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Could not activate DSLSpout\\.")
    public void testActivateThrows() {
        // second MockConnector initialize will throw
        dslSpout.activate();
    }

    @Test
    public void testDeactivate() {
        dslSpout.deactivate();

        // coverage - second MockConnector close() will throw
        dslSpout.deactivate();
    }

    @Test
    public void testClose() {
        dslSpout.close();

        // coverage - second MockConnector close() will throw
        dslSpout.close();
    }

    @Test
    public void testAck() {
        // coverage
        dslSpout.ack(null);
    }

    @Test
    public void testFail() {
        // coverage
        dslSpout.fail(null);
    }
}
