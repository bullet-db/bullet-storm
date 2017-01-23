/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.tracing;

import com.google.gson.JsonParseException;
import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.AggregationOperations.AggregationType;
import com.yahoo.bullet.operations.aggregations.Raw;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static com.yahoo.bullet.TestHelpers.getListBytes;
import static com.yahoo.bullet.parsing.RuleUtils.getAggregationRule;
import static com.yahoo.bullet.parsing.RuleUtils.makeAggregationRule;
import static java.util.Collections.emptyMap;

public class AggregationRuleTest {
    @Test(expectedExceptions = JsonParseException.class)
    public void testBadJSON() {
        getAggregationRule("{", emptyMap());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullConfig() {
        getAggregationRule("{}", null);
    }

    @Test
    public void testRuleAsString() {
        AggregationRule rule = getAggregationRule("{}", emptyMap());
        Assert.assertEquals(rule.toString(), "{}", null);
    }

    @Test
    public void testAggregateIsNotNull() {
        AggregationRule rule = getAggregationRule("{}", emptyMap());
        Assert.assertNotNull(rule.getData());
        rule = getAggregationRule("{'aggregation': {}}", emptyMap());
        Assert.assertNotNull(rule.getData());
        rule = getAggregationRule("{'aggregation': null}", emptyMap());
        Assert.assertNotNull(rule.getData());
    }

    @Test
    public void testCreationTime() {
        long startTime = System.currentTimeMillis();
        AggregationRule rule = getAggregationRule("{'aggregation' : {}}", emptyMap());
        long creationTime = rule.getStartTime();
        long endTime = System.currentTimeMillis();
        Assert.assertTrue(creationTime >= startTime && creationTime <= endTime);
    }

    @Test
    public void testAggregationTime() {
        AggregationRule rule = getAggregationRule("{'aggregation' : {}}", emptyMap());
        long creationTime = rule.getStartTime();
        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, Aggregation.DEFAULT_SIZE).forEach((x) -> rule.consume(record));
        Assert.assertEquals(rule.getData().getRecords().size(), (int) Aggregation.DEFAULT_SIZE);
        long lastAggregationTime = rule.getLastAggregationTime();
        Assert.assertTrue(creationTime <= lastAggregationTime);
    }

    @Test
    public void testDefaultLimiting() {
        AggregationRule rule = getAggregationRule("{'aggregation' : {}}", emptyMap());
        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, Aggregation.DEFAULT_SIZE - 1).forEach(x -> Assert.assertFalse(rule.consume(record)));
        Assert.assertTrue(rule.consume(record));
        Assert.assertEquals((Integer) rule.getData().getRecords().size(), Aggregation.DEFAULT_SIZE);
    }

    @Test
    public void testCustomLimiting() {
        AggregationRule rule = getAggregationRule(makeAggregationRule(AggregationType.RAW, 10), emptyMap());
        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, 9).forEach(x -> Assert.assertFalse(rule.consume(record)));
        Assert.assertTrue(rule.consume(record));
        Assert.assertEquals(rule.getData().getRecords().size(), 10);
    }

    @Test
    public void testSizeUpperBound() {
        AggregationRule rule = getAggregationRule(makeAggregationRule(AggregationType.RAW, 1000), emptyMap());
        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, Raw.DEFAULT_MAX_SIZE - 1).forEach(x -> Assert.assertFalse(rule.consume(record)));
        Assert.assertTrue(rule.consume(record));
        Assert.assertEquals((Integer) rule.getData().getRecords().size(), Raw.DEFAULT_MAX_SIZE);
    }

    @Test
    public void testConfiguredUpperBound() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.AGGREGATION_MAX_SIZE, 2000);
        config.put(BulletConfig.RAW_AGGREGATION_MAX_SIZE, 200);

        AggregationRule rule = getAggregationRule(makeAggregationRule(AggregationType.RAW, 1000), config);
        byte[] record = getListBytes(new BulletRecord());
        IntStream.range(0, 199).forEach(x -> Assert.assertFalse(rule.consume(record)));
        Assert.assertTrue(rule.consume(record));
        Assert.assertEquals(rule.getData().getRecords().size(), 200);
    }

}
