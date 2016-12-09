/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.tracing;

import com.yahoo.bullet.operations.AggregationOperations.AggregationType;
import com.yahoo.bullet.operations.FilterOperations.FilterType;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.yahoo.bullet.TestHelpers.getByteArray;
import static com.yahoo.bullet.parsing.RuleUtils.getFilterRule;
import static com.yahoo.bullet.parsing.RuleUtils.makeAggregationRule;
import static com.yahoo.bullet.parsing.RuleUtils.makeProjectionFilterRule;
import static com.yahoo.bullet.parsing.RuleUtils.makeRawFullRule;
import static java.util.Collections.emptyMap;

public class FilterRuleTest {
    @Test
    public void testFilteringProjection() {
        FilterRule rule = getFilterRule(makeProjectionFilterRule("map_field.id", Arrays.asList("1", "23"),
                                                                  FilterType.EQUALS,
                                                                  Pair.of("map_field.id", "mid")),
                                         emptyMap());
        RecordBox boxA = RecordBox.get().addMap("map_field", Pair.of("id", "3"));
        Assert.assertFalse(rule.consume(boxA.getRecord()));
        Assert.assertNull(rule.getData());

        RecordBox boxB = RecordBox.get().addMap("map_field", Pair.of("id", "23"));
        RecordBox expected = RecordBox.get().add("mid", "23");
        Assert.assertTrue(rule.consume(boxB.getRecord()));
        Assert.assertEquals(rule.getData(), getByteArray(expected.getRecord()));
    }

    @Test
    public void testNoAggregationAttempted() {
        FilterRule rule = getFilterRule(makeRawFullRule("map_field.id", Arrays.asList("1", "23"), FilterType.EQUALS,
                        AggregationType.RAW, Aggregation.DEFAULT_MAX_SIZE,
                        Pair.of("map_field.id", "mid")),
                                         emptyMap());

        RecordBox boxA = RecordBox.get().addMap("map_field", Pair.of("id", "23"));
        RecordBox expectedA = RecordBox.get().add("mid", "23");
        Assert.assertTrue(rule.consume(boxA.getRecord()));
        Assert.assertEquals(rule.getData(), getByteArray(expectedA.getRecord()));

        RecordBox boxB = RecordBox.get().addMap("map_field", Pair.of("id", "3"));
        Assert.assertFalse(rule.consume(boxB.getRecord()));
        Assert.assertNull(rule.getData());

        RecordBox boxC = RecordBox.get().addMap("map_field", Pair.of("id", "1"));
        RecordBox expectedC = RecordBox.get().add("mid", "1");
        Assert.assertTrue(rule.consume(boxC.getRecord()));
        Assert.assertEquals(rule.getData(), getByteArray(expectedC.getRecord()));
    }

    @Test
    public void testMaximumEmitted() {
        FilterRule rule = getFilterRule(makeAggregationRule(AggregationType.RAW, 2), emptyMap());
        RecordBox box = RecordBox.get();
        Assert.assertTrue(rule.consume(box.getRecord()));
        Assert.assertEquals(rule.getData(), getByteArray(box.getRecord()));
        Assert.assertTrue(rule.consume(box.getRecord()));
        Assert.assertEquals(rule.getData(), getByteArray(box.getRecord()));
        for (int i = 0; i < 10; ++i) {
            Assert.assertFalse(rule.consume(box.getRecord()));
            Assert.assertNull(rule.getData());
        }
    }

    @Test
    public void testMaximumEmittedWithNonMatchingRecords() {
        FilterRule rule = getFilterRule(makeRawFullRule("mid", Arrays.asList("1", "23"), FilterType.EQUALS,
                        AggregationType.RAW, 2,
                        Pair.of("mid", "mid")),
                                               emptyMap());
        RecordBox boxA = RecordBox.get().add("mid", "23");
        RecordBox expectedA = RecordBox.get().add("mid", "23");

        RecordBox boxB = RecordBox.get().add("mid", "42");

        Assert.assertTrue(rule.consume(boxA.getRecord()));
        Assert.assertEquals(rule.getData(), getByteArray(expectedA.getRecord()));

        Assert.assertFalse(rule.consume(boxB.getRecord()));
        Assert.assertNull(rule.getData());

        Assert.assertFalse(rule.consume(boxB.getRecord()));
        Assert.assertNull(rule.getData());

        Assert.assertTrue(rule.consume(boxA.getRecord()));
        Assert.assertEquals(rule.getData(), getByteArray(expectedA.getRecord()));

        for (int i = 0; i < 10; ++i) {
            Assert.assertFalse(rule.consume(boxA.getRecord()));
            Assert.assertNull(rule.getData());
            Assert.assertFalse(rule.consume(boxB.getRecord()));
            Assert.assertNull(rule.getData());
        }
    }
}
