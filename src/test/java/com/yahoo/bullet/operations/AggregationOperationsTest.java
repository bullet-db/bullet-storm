/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations;

import com.yahoo.bullet.operations.aggregations.CountDistinct;
import com.yahoo.bullet.operations.aggregations.GroupAll;
import com.yahoo.bullet.operations.aggregations.Raw;
import com.yahoo.bullet.parsing.Aggregation;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;

public class AggregationOperationsTest {
    @Test
    public void testUnimplementedStrategies() {
        Aggregation aggregation = new Aggregation();
        aggregation.configure(Collections.emptyMap());

        aggregation.setType(AggregationOperations.AggregationType.GROUP);
        Assert.assertNull(AggregationOperations.getStrategyFor(aggregation));

        aggregation.setType(AggregationOperations.AggregationType.PERCENTILE);
        Assert.assertNull(AggregationOperations.getStrategyFor(aggregation));

        aggregation.setType(AggregationOperations.AggregationType.TOP);
        Assert.assertNull(AggregationOperations.getStrategyFor(aggregation));
    }

    @Test
    public void testGroupOperationTypeIdentifying() {
        AggregationOperations.GroupOperationType count = AggregationOperations.GroupOperationType.COUNT;
        Assert.assertFalse(count.isMe("count"));
        Assert.assertFalse(count.isMe(null));
        Assert.assertFalse(count.isMe(""));
        Assert.assertTrue(count.isMe(AggregationOperations.GroupOperationType.COUNT.getName()));
    }

    @Test
    public void testRawStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.configure(Collections.emptyMap());

        Assert.assertEquals(AggregationOperations.getStrategyFor(aggregation).getClass(), Raw.class);
    }

    @Test
    public void testGroupAllStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(AggregationOperations.AggregationType.GROUP);
        aggregation.setAttributes(singletonMap(Aggregation.OPERATIONS,
                                  asList(singletonMap(Aggregation.OPERATION_TYPE,
                                         AggregationOperations.GroupOperationType.COUNT.getName()))));
        aggregation.configure(Collections.emptyMap());

        Assert.assertEquals(AggregationOperations.getStrategyFor(aggregation).getClass(), GroupAll.class);
    }

    @Test
    public void testCountDistinctStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(AggregationOperations.AggregationType.COUNT_DISTINCT);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(Collections.emptyMap());

        Assert.assertEquals(AggregationOperations.getStrategyFor(aggregation).getClass(), CountDistinct.class);
    }
}
