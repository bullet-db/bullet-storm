/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.storm.testing.ComponentUtils;
import com.yahoo.bullet.storm.testing.CustomEmitter;
import com.yahoo.bullet.storm.testing.CustomOutputFieldsDeclarer;
import com.yahoo.bullet.storm.testing.TupleUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TickSpoutTest {
    private CustomEmitter emitter;
    private TickSpout spout;

    private void forceTickByLastTickTime(long lastTime) {
        spout.setLastTickTime(lastTime);
        spout.nextTuple();
    }
    private void forceTick() {
        forceTickByLastTickTime(0L);
    }

    @BeforeMethod
    public void setup() {
        emitter = new CustomEmitter();
        spout = ComponentUtils.open(new TickSpout(new BulletStormConfig()), emitter);
    }

    @Test
    public void testDefaults() {
        Assert.assertEquals(TickSpout.GRACEFUL_SLEEP, BulletStormConfig.TICK_INTERVAL_MINIMUM / 2);
        Assert.assertTrue(spout.getId() != 0);
        Assert.assertTrue(spout.getLastTickTime() <= System.currentTimeMillis());
        Assert.assertEquals(spout.getTick(), 0L);
        Assert.assertEquals(spout.getTickInterval(), BulletStormConfig.DEFAULT_TICK_SPOUT_INTERVAL);
        Assert.assertNotNull(spout.getCollector());
        spout.deactivate();
        spout.activate();
        spout.ack("foo");
        spout.fail("bar");
        spout.close();
        Assert.assertEquals(emitter.getEmitted().size(), 0);
        Assert.assertEquals(spout.getTick(), 0L);
    }

    @Test
    public void testDeclaredOutputFields() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        spout.declareOutputFields(declarer);
        Fields expected = new Fields(TopologyConstants.ID_FIELD, TopologyConstants.TICK_FIELD);
        Assert.assertTrue(declarer.areFieldsPresent(TopologyConstants.TICK_STREAM, false, expected));
    }

    @Test
    public void testEmittingTicks() {
        int id = spout.getId();
        forceTick();
        forceTick();
        forceTick();
        Tuple expectedFirst = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE, id, 1L);
        Tuple expectedSecond = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE, id, 2L);
        Tuple expectedThird = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE, id, 3L);
        Assert.assertTrue(emitter.wasTupleEmittedTo(expectedFirst, TopologyConstants.TICK_STREAM));
        Assert.assertTrue(emitter.wasTupleEmittedTo(expectedSecond, TopologyConstants.TICK_STREAM));
        Assert.assertTrue(emitter.wasTupleEmittedTo(expectedThird, TopologyConstants.TICK_STREAM));
        Assert.assertTrue(emitter.wasNthEmitted(expectedFirst, 1));
        Assert.assertTrue(emitter.wasNthEmitted(expectedSecond, 2));
        Assert.assertTrue(emitter.wasNthEmitted(expectedThird, 3));
    }

    @Test
    public void testEmittingAtTickIntervals() throws InterruptedException {
        long timeNow = System.currentTimeMillis();
        int interval = 100000;
        BulletStormConfig config = new BulletStormConfig();
        config.set(BulletStormConfig.TICK_SPOUT_INTERVAL, interval);
        spout = ComponentUtils.open(new TickSpout(config), emitter);
        spout.nextTuple();
        Assert.assertEquals(spout.getTickInterval(), interval);
        Assert.assertEquals(emitter.getEmitted().size(), 0);

        forceTickByLastTickTime(timeNow - interval);
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE, spout.getId(), 1L);
        Assert.assertTrue(emitter.wasTupleEmittedTo(expected, TopologyConstants.TICK_STREAM));
    }
}
