/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.TestHelpers;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.yahoo.bullet.TestHelpers.getByteArray;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RawTest {
    public static Raw makeRaw(int size) {
        Aggregation aggregation = new Aggregation();
        aggregation.setSize(size);
        return new Raw(aggregation);
    }

    @Test
    public void testCanAcceptData() {
        BulletRecord record = RecordBox.get().add("foo", "bar").getRecord();
        Raw raw = makeRaw(2);

        Assert.assertTrue(raw.isAcceptingData());

        raw.consume(record);
        Assert.assertTrue(raw.isAcceptingData());

        raw.consume(record);
        Assert.assertFalse(raw.isAcceptingData());
    }

    @Test
    public void testMicroBatching() {
        BulletRecord record = RecordBox.get().add("foo", "bar").getRecord();
        Raw raw = makeRaw(2);

        Assert.assertFalse(raw.isMicroBatch());
        raw.consume(record);
        Assert.assertTrue(raw.isMicroBatch());
        Assert.assertNotNull(raw.getSerializedAggregation());
        Assert.assertFalse(raw.isMicroBatch());
        raw.consume(record);
        Assert.assertTrue(raw.isMicroBatch());
    }

    @Test
    public void testNull() {
        Raw raw = makeRaw(1);
        raw.consume(null);
        Assert.assertNull(raw.getSerializedAggregation());
        raw.combine(null);
        Assert.assertNull(raw.getSerializedAggregation());
        Assert.assertEquals(raw.getAggregation().size(), 0);
    }

    @Test
    public void testBadRecord() throws IOException {
        BulletRecord mocked = mock(BulletRecord.class);
        when(mocked.getAsByteArray()).thenThrow(new IOException("Testing"));

        Raw raw = makeRaw(1);
        raw.consume(mocked);
        Assert.assertNull(raw.getSerializedAggregation());
    }

    @Test
    public void testSerializationOnConsumedRecord() {
        Raw raw = makeRaw(2);

        BulletRecord recordA = RecordBox.get().add("foo", "bar").getRecord();
        raw.consume(recordA);

        Assert.assertEquals(raw.getSerializedAggregation(), getByteArray(recordA));

        BulletRecord recordB = RecordBox.get().add("bar", "baz").getRecord();
        raw.consume(recordB);

        Assert.assertEquals(raw.getSerializedAggregation(), getByteArray(recordB));

        Assert.assertFalse(raw.isAcceptingData());

        BulletRecord recordC = RecordBox.get().add("baz", "qux").getRecord();
        // This consumption should not occur
        raw.consume(recordC);
        Assert.assertNull(raw.getSerializedAggregation());
    }

    @Test
    public void testLimitZero() {
        Raw raw = makeRaw(0);

        List<BulletRecord> aggregate = raw.getAggregation();
        Assert.assertFalse(raw.isAcceptingData());
        Assert.assertFalse(raw.isMicroBatch());
        Assert.assertEquals(aggregate.size(), 0);

        BulletRecord record = RecordBox.get().add("foo", "bar").getRecord();
        raw.consume(record);

        Assert.assertFalse(raw.isAcceptingData());
        Assert.assertFalse(raw.isMicroBatch());
        Assert.assertNull(raw.getSerializedAggregation());
        Assert.assertEquals(raw.getAggregation().size(), 0);
    }

    @Test
    public void testLimitLessThanSpecified() {
        Raw raw = makeRaw(10);
        List<BulletRecord> records = IntStream.range(0, 5).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                              .collect(Collectors.toList());

        records.stream().map(TestHelpers::getByteArray).forEach(raw::combine);

        List<BulletRecord> aggregate = raw.getAggregation();
        // We should have 5 records
        Assert.assertEquals(aggregate.size(), 5);
        // We should have all the records
        Assert.assertEquals(aggregate, records);
    }

    @Test
    public void testLimitExact() {
        Raw raw = makeRaw(10);

        List<BulletRecord> records = IntStream.range(0, 10).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                              .collect(Collectors.toList());

        records.stream().map(TestHelpers::getByteArray).forEach(raw::combine);

        List<BulletRecord> aggregate = raw.getAggregation();
        // We should have 10 records
        Assert.assertEquals(aggregate.size(), 10);
        // We should have the all records
        Assert.assertEquals(aggregate, records);
    }


    @Test
    public void testLimitMoreThanMaximum() {
        Raw raw = makeRaw(10);

        List<BulletRecord> records = IntStream.range(0, 20).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                              .collect(Collectors.toList());

        records.stream().map(TestHelpers::getByteArray).forEach(raw::combine);

        List<BulletRecord> aggregate = raw.getAggregation();
        // We should have 10 records
        Assert.assertEquals(aggregate.size(), 10);
        // We should have the first 10 records
        Assert.assertEquals(aggregate, records.subList(0, 10));
    }
}
