/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;

public class DRPCOutputCollectorTest {
    private DRPCOutputCollector collector;

    @BeforeMethod
    public void setup() {
        collector = new DRPCOutputCollector();
    }

    @Test
    public void testSpoutEmit() {
        Assert.assertFalse(collector.haveOutput());
        Assert.assertFalse(collector.isAcked());
        Assert.assertFalse(collector.isFailed());

        Assert.assertNull(collector.emit("foo", new Values("bar", 1), "id1"));
        Assert.assertNull(collector.emit("bar", new Values("baz", 2), "id2"));

        Assert.assertTrue(collector.haveOutput());
        Assert.assertFalse(collector.isAcked());
        Assert.assertFalse(collector.isFailed());

        List<List<Object>> tuples = collector.reset();
        Assert.assertNotNull(tuples);
        Assert.assertEquals(tuples.size(), 2);

        List<Object> first = tuples.get(0);
        Assert.assertEquals(first.get(0), asList("bar", 1));
        Assert.assertEquals(first.get(1), "id1");

        List<Object> second = tuples.get(1);
        Assert.assertEquals(second.get(0), asList("baz", 2));
        Assert.assertEquals(second.get(1), "id2");

        Assert.assertFalse(collector.haveOutput());
        Assert.assertFalse(collector.isAcked());
        Assert.assertFalse(collector.isFailed());
    }

    @Test
    public void testFailing() {
        Assert.assertFalse(collector.haveOutput());
        Assert.assertFalse(collector.isAcked());
        Assert.assertFalse(collector.isFailed());

        collector.fail(null);

        Assert.assertFalse(collector.haveOutput());
        Assert.assertFalse(collector.isAcked());
        Assert.assertTrue(collector.isFailed());
    }

    @Test
    public void testAcking() {
        Assert.assertFalse(collector.haveOutput());
        Assert.assertFalse(collector.isAcked());
        Assert.assertFalse(collector.isFailed());

        collector.ack(null);

        Assert.assertFalse(collector.haveOutput());
        Assert.assertTrue(collector.isAcked());
        Assert.assertFalse(collector.isFailed());
    }

    // Unimplemented methods

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testReportError() {
        collector.reportError(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testResetTimeout() {
        collector.resetTimeout(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testBoltEmit() {
        collector.emit(null, (Collection<Tuple>) null, null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testBoltEmitDirect() {
        collector.emitDirect(0, null, (Collection<Tuple>) null, null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testSpoutEmitDirect() {
        collector.emitDirect(0, null, (List<Object>) null, null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetPendingCount() {
        collector.getPendingCount();
    }
}
