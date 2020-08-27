/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.storm.testing.ComponentUtils;
import com.yahoo.bullet.storm.testing.CustomCollector;
import com.yahoo.bullet.storm.testing.CustomOutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.yahoo.bullet.storm.testing.TupleUtils.makeTuple;

public class DSLBoltTest {
    private CustomCollector collector;
    private DSLBolt dslBolt;
    private BulletStormConfig config;

    @BeforeMethod
    public void setup() {
        collector = new CustomCollector();
        config = new BulletStormConfig("src/test/resources/test_dsl_config.yaml");
        dslBolt = ComponentUtils.prepare(new DSLBolt(config), collector);
    }

    @Test
    public void testExecute() {
        Tuple tuple = makeTuple(Arrays.asList(Collections.singletonMap("foo", "bar"), Collections.singletonMap("foo", 5)));

        dslBolt.execute(tuple);

        Assert.assertEquals(collector.getEmittedCount(), 1);

        BulletRecord record = (BulletRecord) collector.getEmitted().get(0).getTuple().get(TopologyConstants.RECORD_POSITION);

        Assert.assertEquals(record.typedGet("foo").getValue(), "bar");

        Assert.assertEquals(collector.getAckedCount(), 1);
    }

    @Test
    public void testExecuteWithDeserialize() {
        config.set(BulletStormConfig.DSL_DESERIALIZER_ENABLE, true);

        dslBolt = ComponentUtils.prepare(new DSLBolt(config), collector);

        Tuple tuple = makeTuple(Arrays.asList(Collections.singletonMap("foo", "bar"), Collections.singletonMap("foo", 5)));

        dslBolt.execute(tuple);

        // MockDeserializer changes key "foo" to "bar"
        Assert.assertEquals(collector.getEmitted().size(), 1);

        BulletRecord record = (BulletRecord) collector.getEmitted().get(0).getTuple().get(TopologyConstants.RECORD_POSITION);

        Assert.assertNull(record.typedGet("foo").getValue());
        Assert.assertEquals(record.typedGet("bar").getValue(), "bar");

        Assert.assertEquals(collector.getAckedCount(), 1);
    }


    @Test
    public void testOutputFields() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        dslBolt.declareOutputFields(declarer);
        Fields expected = new Fields(TopologyConstants.RECORD_FIELD, TopologyConstants.RECORD_TIMESTAMP_FIELD);
        Assert.assertTrue(declarer.areFieldsPresent(expected));
    }

    @Test
    public void testCleanup() {
        // coverage
        dslBolt.cleanup();
    }
}
