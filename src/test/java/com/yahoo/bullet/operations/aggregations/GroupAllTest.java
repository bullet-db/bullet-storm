/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.operations.AggregationOperations.AggregationType;
import com.yahoo.bullet.operations.AggregationOperations.GroupOperationType;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.yahoo.bullet.parsing.AggregationUtils.makeAttributes;
import static com.yahoo.bullet.parsing.AggregationUtils.makeGroupOperation;
import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GroupAllTest {
    @SafeVarargs
    public static GroupAll makeGroupAll(Map<String, String>... groupOperations) {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(AggregationType.GROUP);
        // Does not matter
        aggregation.setSize(1);
        aggregation.setAttributes(makeAttributes(groupOperations));
        aggregation.configure(Collections.emptyMap());
        return new GroupAll(aggregation);
    }

    public static GroupAll makeGroupAll(GroupOperation... groupOperations) {
        Aggregation aggregation = mock(Aggregation.class);
        when(aggregation.getGroupOperations()).thenReturn(new HashSet<>(asList(groupOperations)));
        return new GroupAll(aggregation);
    }

    @Test
    public void testNoRecordCount() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.COUNT, null, null));

        Assert.assertNotNull(groupAll.getSerializedAggregation());
        List<BulletRecord> aggregate = groupAll.getAggregation();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add(GroupOperationType.COUNT.getName(), 0L).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testNullRecordCount() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.COUNT, null, "count"));
        groupAll.consume(RecordBox.get().add("foo", "bar").getRecord());
        Assert.assertNotNull(groupAll.getSerializedAggregation());
        List<BulletRecord> aggregate = groupAll.getAggregation();

        // We do not expect to send in null records so the count is incremented.
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("count", 1L).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testCounting() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.COUNT, null, "count"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();

        IntStream.range(0, 10).forEach(i -> groupAll.consume(someRecord));

        Assert.assertNotNull(groupAll.getSerializedAggregation());
        List<BulletRecord> aggregate = groupAll.getAggregation();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add("count", 10).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testCountingMoreThanMaximum() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.COUNT, null, null));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();

        IntStream.range(0, 2 * Aggregation.DEFAULT_MAX_SIZE).forEach(i -> groupAll.consume(someRecord));

        Assert.assertNotNull(groupAll.getSerializedAggregation());
        List<BulletRecord> aggregate = groupAll.getAggregation();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        BulletRecord expected = RecordBox.get().add(GroupOperationType.COUNT.getName(),
                                                    2L * Aggregation.DEFAULT_MAX_SIZE).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testCombiningMetrics() {
        GroupAll groupAll = makeGroupAll(new GroupOperation(GroupOperationType.COUNT, null, "myCount"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();
        IntStream.range(0, 21).forEach(i -> groupAll.consume(someRecord));

        GroupAll another = makeGroupAll(makeGroupOperation(GroupOperationType.COUNT, null, null));
        IntStream.range(0, 21).forEach(i -> another.consume(someRecord));
        byte[] serialized = another.getSerializedAggregation();

        groupAll.combine(serialized);

        Assert.assertNotNull(groupAll.getSerializedAggregation());
        List<BulletRecord> aggregate = groupAll.getAggregation();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        // 21 + 21
        BulletRecord expected = RecordBox.get().add("myCount", 42L).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testCombiningMetricsFail() {
        GroupAll groupAll = makeGroupAll(makeGroupOperation(GroupOperationType.COUNT, null, null));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();
        IntStream.range(0, 10).forEach(i -> groupAll.consume(someRecord));

        // Not a serialized GroupData
        groupAll.combine(String.valueOf(242).getBytes());

        Assert.assertNotNull(groupAll.getSerializedAggregation());
        List<BulletRecord> aggregate = groupAll.getAggregation();
        Assert.assertEquals(aggregate.size(), 1);
        BulletRecord actual = aggregate.get(0);
        // Unchanged count
        BulletRecord expected = RecordBox.get().add(GroupOperationType.COUNT.getName(), 10L).getRecord();
        Assert.assertEquals(actual, expected);
    }
}
