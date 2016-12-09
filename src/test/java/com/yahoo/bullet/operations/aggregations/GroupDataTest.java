/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.operations.AggregationOperations.GroupOperationType;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

public class GroupDataTest {
    private class NotSerializable {
        private int foo;
    }

    private class UnserializableGroupData extends GroupData {
        private NotSerializable notSerializable = new NotSerializable();

        public UnserializableGroupData(Set<GroupOperation> operations) {
            super(operations);
        }
    }

    private GroupData make(byte[] data) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return (GroupData) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] unmake(GroupData data) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream ois = new ObjectOutputStream(bos);
            ois.writeObject(data);
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static GroupData make(GroupOperation... operations) {
        return new GroupData(new HashSet<>(Arrays.asList(operations)));
    }

    @Test
    public void testManualSerializationFailing() {
        GroupData bad = new UnserializableGroupData(Collections.emptySet());
        Assert.assertNull(GroupData.toBytes(bad));
    }

    @Test
    public void testManualSerialization() {
        GroupData data = make(new GroupOperation(GroupOperationType.COUNT, null, "foo"));
        BulletRecord sample = RecordBox.get().getRecord();
        IntStream.range(0, 5).forEach(i -> data.compute(sample));

        byte[] serialized = GroupData.toBytes(data);
        Assert.assertNotNull(serialized);

        GroupData remade = make(serialized);
        BulletRecord expected = RecordBox.get().add("foo", 5L).getRecord();
        Assert.assertEquals(remade.getAsBulletRecord(), expected);
    }

    @Test
    public void testManualDeserializationFailing() {
        Assert.assertNull(GroupData.fromBytes(null));
    }

    @Test
    public void testManualDeserialization() {
        GroupData data = make(new GroupOperation(GroupOperationType.COUNT, null, "foo"));
        BulletRecord sample = RecordBox.get().getRecord();
        IntStream.range(0, 5).forEach(i -> data.compute(sample));

        byte[] serialized = unmake(data);
        GroupData remade = GroupData.fromBytes(serialized);
        BulletRecord expected = RecordBox.get().add("foo", 5L).getRecord();
        Assert.assertEquals(remade.getAsBulletRecord(), expected);
    }

    @Test
    public void testNameExtraction() {
        GroupOperation operation;

        operation = new GroupOperation(GroupOperationType.COUNT, null, null);
        Assert.assertEquals(GroupData.getResultName(operation), GroupOperationType.COUNT.getName());

        operation = new GroupOperation(GroupOperationType.COUNT, "foo", null);
        Assert.assertEquals(GroupData.getResultName(operation), GroupOperationType.COUNT.getName() +
                                                                  GroupData.NAME_SEPARATOR + "foo");

        operation = new GroupOperation(GroupOperationType.COUNT, "foo", "bar");
        Assert.assertEquals(GroupData.getResultName(operation), "bar");

        operation = new GroupOperation(GroupOperationType.AVG, "foo", "bar");
        Assert.assertEquals(GroupData.getResultName(operation), "bar");
    }

    @Test
    public void testNullRecordCount() {
        GroupData data = make(new GroupOperation(GroupOperationType.COUNT, null, "count"));
        data.compute(RecordBox.get().add("foo", "bar").getRecord());

        // We do not expect to send in null records so the count is incremented.
        BulletRecord expected = RecordBox.get().add("count", 1L).getRecord();
        Assert.assertEquals(data.getAsBulletRecord(), expected);
    }

    @Test
    public void testNoRecordCount() {
        GroupData data = make(new GroupOperation(GroupOperationType.COUNT, null, "count"));

        // Count should be 0 if there was no data presented.
        BulletRecord expected = RecordBox.get().add("count", 0L).getRecord();
        Assert.assertEquals(data.getAsBulletRecord(), expected);
    }

    @Test
    public void testSingleCounting() {
        GroupData data = make(new GroupOperation(GroupOperationType.COUNT, null, "foo"));
        BulletRecord expected = RecordBox.get().add("foo", 0L).getRecord();

        Assert.assertEquals(data.getAsBulletRecord(), expected);

        data.compute(RecordBox.get().getRecord());
        expected = RecordBox.get().add("foo", 1L).getRecord();
        Assert.assertEquals(data.getAsBulletRecord(), expected);
    }

    @Test
    public void testMultiCounting() {
        GroupData data = make(new GroupOperation(GroupOperationType.COUNT, null, "count"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();

        IntStream.range(0, 10).forEach(i -> data.compute(someRecord));

        BulletRecord expected = RecordBox.get().add("count", 10L).getRecord();
        Assert.assertEquals(data.getAsBulletRecord(), expected);
    }

    @Test
    public void testCountingMoreThanMaximum() {
        GroupData data = make(new GroupOperation(GroupOperationType.COUNT, null, null));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();

        IntStream.range(0, 2 * Aggregation.DEFAULT_MAX_SIZE).forEach(i -> data.compute(someRecord));

        BulletRecord expected = RecordBox.get().add(GroupOperationType.COUNT.getName(),
                                                    2L * Aggregation.DEFAULT_MAX_SIZE).getRecord();
        Assert.assertEquals(data.getAsBulletRecord(), expected);
    }

    @Test
    public void testMergingRawMetric() {
        GroupData data = make(new GroupOperation(GroupOperationType.COUNT, null, "myCount"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();
        IntStream.range(0, 21).forEach(i -> data.compute(someRecord));

        GroupData another = make(new GroupOperation(GroupOperationType.COUNT, null, "count"));
        IntStream.range(0, 21).forEach(i -> another.compute(someRecord));
        byte[] serialized = GroupData.toBytes(another);

        data.merge(serialized);

        // 21 + 21
        BulletRecord expected = RecordBox.get().add("myCount", 42L).getRecord();
        Assert.assertEquals(data.getAsBulletRecord(), expected);
    }


    @Test
    public void testMergingRawMetricFail() {
        GroupData data = make(new GroupOperation(GroupOperationType.COUNT, null, null));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();
        IntStream.range(0, 10).forEach(i -> data.compute(someRecord));

        // Not a serialized GroupData
        data.merge(String.valueOf(242).getBytes());

        // Unchanged count
        BulletRecord expected = RecordBox.get().add(GroupOperationType.COUNT.getName(), 10L).getRecord();
        Assert.assertEquals(data.getAsBulletRecord(), expected);
    }

    @Test
    public void testMergingTwoUnsupportedOperations() {
        GroupData data = make(new GroupOperation(GroupOperationType.AVG, "foo", "bar"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();
        IntStream.range(0, 10).forEach(i -> data.compute(someRecord));

        GroupData another = make(new GroupOperation(GroupOperationType.AVG, "foo", "bar"));
        IntStream.range(0, 21).forEach(i -> another.compute(someRecord));
        byte[] serialized = GroupData.toBytes(another);

        data.merge(serialized);

        // Empty record
        BulletRecord expected = RecordBox.get().getRecord();
        Assert.assertEquals(data.getAsBulletRecord(), expected);
    }

    @Test
    public void testMergingSupportedAndUnSupportedOperation() {
        GroupData data = make(new GroupOperation(GroupOperationType.COUNT, null, null));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();
        IntStream.range(0, 10).forEach(i -> data.compute(someRecord));

        GroupData another = make(new GroupOperation(GroupOperationType.AVG, "foo", "bar"));
        IntStream.range(0, 21).forEach(i -> another.compute(someRecord));
        byte[] serialized = GroupData.toBytes(another);

        // This should combine since we only merge known GroupOperations from the other GroupData
        data.merge(serialized);

        // AVG should not have influenced other counts.
        BulletRecord expected = RecordBox.get().add(GroupOperationType.COUNT.getName(), 10L).getRecord();
        Assert.assertEquals(data.getAsBulletRecord(), expected);
    }

    @Test
    public void testMergingUnsupportedAndSupportedOperation() {
        GroupData data = make(new GroupOperation(GroupOperationType.AVG, "foo", "bar"));
        BulletRecord someRecord = RecordBox.get().add("foo", 1).getRecord();
        IntStream.range(0, 10).forEach(i -> data.compute(someRecord));

        GroupData another = make(new GroupOperation(GroupOperationType.COUNT, null, null));
        IntStream.range(0, 21).forEach(i -> another.compute(someRecord));
        byte[] serialized = GroupData.toBytes(another);

        // This should not merge the counts into this operation
        data.merge(serialized);

        // AVG does not exist and COUNT will not be merged.
        BulletRecord expected = RecordBox.get().getRecord();
        Assert.assertEquals(data.getAsBulletRecord(), expected);
    }
}
