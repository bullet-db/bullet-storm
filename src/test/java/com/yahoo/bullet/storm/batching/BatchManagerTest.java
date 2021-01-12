/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.batching;

import com.yahoo.bullet.storm.StormUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BatchManagerTest {
    private static final int BATCH_SIZE = 10000;
    private static final int PARTITION_COUNT = 4;
    private BatchManager<String> batchManager;

    @BeforeMethod
    public void setup() {
        batchManager = new BatchManager<>(BATCH_SIZE, PARTITION_COUNT, true);
        // coverage
        Assert.assertTrue(batchManager.getPartitions().get(0).isBatchCompressEnable());
        Assert.assertEquals(batchManager.getPartitions().get(0).getBatchSize(), BATCH_SIZE);
        Assert.assertNotNull(batchManager.getPartitions().get(0).getChanged());
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Partition count must be greater than 0.*")
    public void testConstructorInvalidPartitionCount() {
        new BatchManager(0, 0, false);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Batch size must be greater than 0.*")
    public void testConstructorInvalidBatchSize() {
        new BatchManager(0, 1, false);
    }

    @Test
    public void testBatchesAndCompressedBatchesMatch() {
        // Each partition starts with one batch
        Assert.assertEquals(batchManager.getBatches().size(), PARTITION_COUNT);
        Assert.assertEquals(batchManager.getCompressedBatches().size(), PARTITION_COUNT);
        Assert.assertEquals(batchManager.size(), 0);

        Map<String, String> map = IntStream.range(0, BATCH_SIZE).boxed().collect(Collectors.toMap(Object::toString, Object::toString));

        batchManager.addAll(map);
        Assert.assertEquals(batchManager.size(), BATCH_SIZE);

        List<Map<String, String>> batches = batchManager.getBatches();
        List<byte[]> compressedBatches = batchManager.getCompressedBatches();

        Assert.assertEquals(batches.size(), compressedBatches.size());

        for (int i = 0; i < batches.size(); i++) {
            Assert.assertEquals(batches.get(i), StormUtils.decompress(compressedBatches.get(i)));
        }
    }

    @Test
    public void testPartitionedBatchesAndCompressedPartitionedBatchesMatch() {
        // Each partition starts with one batch
        Map<Integer, List<Map<String, String>>> partitionedBatches = batchManager.getPartitionedBatches();
        Map<Integer, List<byte[]>> compressedPartitionedBatches = batchManager.getPartitionedCompressedBatches();

        Assert.assertEquals(partitionedBatches.size(), 4);
        Assert.assertTrue(partitionedBatches.values().stream().allMatch(batches -> batches.size() == 1));
        Assert.assertEquals(compressedPartitionedBatches.size(), 4);
        Assert.assertTrue(compressedPartitionedBatches.values().stream().allMatch(batches -> batches.size() == 1));

        Map<String, String> map = IntStream.range(0, BATCH_SIZE).boxed().collect(Collectors.toMap(Object::toString, Object::toString));

        batchManager.addAll(map);
        Assert.assertEquals(batchManager.size(), BATCH_SIZE);

        partitionedBatches = batchManager.getPartitionedBatches();
        compressedPartitionedBatches = batchManager.getPartitionedCompressedBatches();

        Assert.assertEquals(partitionedBatches.keySet(), compressedPartitionedBatches.keySet());

        for (Integer k : partitionedBatches.keySet()) {
            List<Map<String, String>> batches = partitionedBatches.get(k);
            List<byte[]> compressedBatches = compressedPartitionedBatches.get(k);
            for (int i = 0; i < batches.size(); i++) {
                Assert.assertEquals(batches.get(i), StormUtils.decompress(compressedBatches.get(i)));
            }
        }
    }

    @Test
    public void testGetBatchesImmutable() {
        batchManager = new BatchManager<>(10000, 1, true);

        batchManager.add("aaa", "bbb");
        Assert.assertEquals(batchManager.size(), 1);

        List<Map<String, String>> batches = batchManager.getBatches();
        Assert.assertEquals(batches.get(0).size(), 1);

        batchManager.remove("aaa");
        Assert.assertEquals(batchManager.size(), 0);
        Assert.assertEquals(batches.get(0).size(), 1);

        batches.clear();
        Assert.assertEquals(batchManager.getPartitions().get(0).getBatches().size(), 1);
    }

    @Test
    public void testGetBatchesForPartitionImmutable() {
        batchManager = new BatchManager<>(10000, 1, true);

        batchManager.add("aaa", "bbb");
        Assert.assertEquals(batchManager.size(), 1);

        List<Map<String, String>> batches = batchManager.getBatchesForPartition(0);
        Assert.assertEquals(batches.get(0).size(), 1);

        batchManager.remove("aaa");
        Assert.assertEquals(batchManager.size(), 0);
        Assert.assertEquals(batches.get(0).size(), 1);

        batches.clear();
        Assert.assertEquals(batchManager.getPartitions().get(0).getBatches().size(), 1);
    }

    @Test
    public void testGetPartitionedBatchesImmutable() {
        batchManager = new BatchManager<>(10000, 1, true);

        batchManager.add("aaa", "bbb");
        Assert.assertEquals(batchManager.size(), 1);

        List<Map<String, String>> batches = batchManager.getPartitionedBatches().get(0);
        Assert.assertEquals(batches.get(0).size(), 1);

        batchManager.remove("aaa");
        Assert.assertEquals(batchManager.size(), 0);
        Assert.assertEquals(batches.get(0).size(), 1);

        batches.clear();
        Assert.assertEquals(batchManager.getPartitions().get(0).getBatches().size(), 1);
    }

    @Test
    public void testGetCompressedBatchesImmutable() {
        batchManager = new BatchManager<>(10000, 1, true);

        batchManager.add("aaa", "bbb");
        Assert.assertEquals(batchManager.size(), 1);

        List<byte[]> batches = batchManager.getCompressedBatches();

        byte[] batch = batches.get(0);

        batchManager.remove("aaa");
        Assert.assertEquals(batchManager.size(), 0);

        // Triggers compression
        batchManager.getCompressedBatches();
        Assert.assertEquals(batch, batches.get(0));

        batches.clear();
        Assert.assertEquals(batchManager.getPartitions().get(0).getData().size(), 1);
    }

    @Test
    public void testGetCompressedBatchesForPartitionImmutable() {
        batchManager = new BatchManager<>(10000, 1, true);

        batchManager.add("aaa", "bbb");
        Assert.assertEquals(batchManager.size(), 1);

        List<byte[]> batches = batchManager.getCompressedBatchesForPartition(0);

        byte[] batch = batches.get(0);

        batchManager.remove("aaa");
        Assert.assertEquals(batchManager.size(), 0);

        // Triggers compression
        batchManager.getCompressedBatches();
        Assert.assertEquals(batch, batches.get(0));

        batches.clear();
        Assert.assertEquals(batchManager.getPartitions().get(0).getData().size(), 1);
    }

    @Test
    public void testGetCompressedPartitionedBatchesImmutable() {
        batchManager = new BatchManager<>(10000, 1, true);

        batchManager.add("aaa", "bbb");
        Assert.assertEquals(batchManager.size(), 1);

        List<byte[]> batches = batchManager.getPartitionedCompressedBatches().get(0);

        byte[] batch = batches.get(0);

        batchManager.remove("aaa");
        Assert.assertEquals(batchManager.size(), 0);

        // Triggers compression
        batchManager.getPartitionedCompressedBatches();
        Assert.assertEquals(batch, batches.get(0));

        batches.clear();
        Assert.assertEquals(batchManager.getPartitions().get(0).getData().size(), 1);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Throwing runtime exception since batch compression is not enabled\\.")
    public void testGetCompressedBatchesThrowsWhenCompressionIsDisabled() {
        new BatchManager<>(BATCH_SIZE, PARTITION_COUNT, false).getCompressedBatches();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Throwing runtime exception since batch compression is not enabled\\.")
    public void testGetCompressedBatchesForPartitionThrowsWhenCompressionIsDisabled() {
        new BatchManager<>(BATCH_SIZE, PARTITION_COUNT, false).getCompressedBatchesForPartition(0);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Throwing runtime exception since batch compression is not enabled\\.")
    public void testGetCompressedPartitionedBatchesThrowsWhenCompressionIsDisabled() {
        new BatchManager<>(BATCH_SIZE, PARTITION_COUNT, false).getPartitionedCompressedBatches();
    }

    @Test
    public void testAddRemoveContainsClear() {
        Map<String, String> map = IntStream.range(0, BATCH_SIZE).boxed().collect(Collectors.toMap(Object::toString, Object::toString));

        batchManager.addAll(map);
        Assert.assertEquals(batchManager.size(), BATCH_SIZE);

        Assert.assertFalse(batchManager.contains("aaa"));

        batchManager.add("aaa", "bbb");
        Assert.assertTrue(batchManager.contains("aaa"));
        Assert.assertEquals(batchManager.size(), BATCH_SIZE + 1);

        Assert.assertTrue(batchManager.contains("1234"));

        batchManager.remove("1234");
        Assert.assertFalse(batchManager.contains("1234"));
        Assert.assertEquals(batchManager.size(), BATCH_SIZE);

        batchManager.remove("1234");
        Assert.assertEquals(batchManager.size(), BATCH_SIZE);

        Set<String> set = IntStream.range(0, BATCH_SIZE).boxed().map(Objects::toString).collect(Collectors.toSet());

        batchManager.removeAll(set);
        Assert.assertEquals(batchManager.size(), 1);

        batchManager.add("aaa", "bbb");
        Assert.assertEquals(batchManager.size(), 1);

        // All partitions start with one batch
        Assert.assertEquals(batchManager.getPartitions().get(0).getBatchCount(), 1);
        Assert.assertEquals(batchManager.getPartitions().get(1).getBatchCount(), 1);
        Assert.assertEquals(batchManager.getPartitions().get(2).getBatchCount(), 1);
        Assert.assertEquals(batchManager.getPartitions().get(3).getBatchCount(), 1);

        // Pigeonhole principle 4 x 10000 + 1
        map = IntStream.range(0, PARTITION_COUNT * BATCH_SIZE).boxed().collect(Collectors.toMap(Object::toString, Object::toString));

        batchManager.addAll(map);
        Assert.assertEquals(batchManager.size(), PARTITION_COUNT * BATCH_SIZE + 1);
        Assert.assertTrue(batchManager.getPartitions().get(0).getBatchCount() == 2 ||
                          batchManager.getPartitions().get(1).getBatchCount() == 2 ||
                          batchManager.getPartitions().get(2).getBatchCount() == 2 ||
                          batchManager.getPartitions().get(3).getBatchCount() == 2);

        // Clear does not resize
        batchManager.clear();
        Assert.assertEquals(batchManager.size(), 0);
        Assert.assertTrue(batchManager.getPartitions().get(0).getBatchCount() == 2 ||
                          batchManager.getPartitions().get(1).getBatchCount() == 2 ||
                          batchManager.getPartitions().get(2).getBatchCount() == 2 ||
                          batchManager.getPartitions().get(3).getBatchCount() == 2);
    }

    @Test
    public void testPartitionResizing() {
        batchManager = new BatchManager<>(10000, 1, false);

        Assert.assertEquals(batchManager.size(), 0);
        Assert.assertEquals(batchManager.getPartitions().get(0).getBatchCount(), 1);
        Assert.assertEquals(batchManager.getPartitions().get(0).getMaxCapacity(), 10000);
        Assert.assertEquals(batchManager.getPartitions().get(0).getMinCapacity(), 0);

        Map<String, String> map = IntStream.range(0, 10000).boxed().collect(Collectors.toMap(Object::toString, Object::toString));

        batchManager.addAll(map);
        Assert.assertEquals(batchManager.size(), 10000);
        Assert.assertEquals(batchManager.getPartitions().get(0).getBatchCount(), 1);

        batchManager.add("aaa", "bbb");
        Assert.assertEquals(batchManager.size(), 10001);
        Assert.assertEquals(batchManager.getPartitions().get(0).getBatchCount(), 2);
        Assert.assertEquals(batchManager.getPartitions().get(0).getMaxCapacity(), 20000);
        Assert.assertEquals(batchManager.getPartitions().get(0).getMinCapacity(), 5000);

        batchManager.remove("aaa");
        Assert.assertEquals(batchManager.size(), 10000);
        Assert.assertEquals(batchManager.getPartitions().get(0).getBatchCount(), 2);

        Set<String> set = IntStream.range(0, 5000).boxed().map(Objects::toString).collect(Collectors.toSet());

        batchManager.removeAll(set);
        Assert.assertEquals(batchManager.size(), 5000);
        Assert.assertEquals(batchManager.getPartitions().get(0).getBatchCount(), 2);

        batchManager.remove("5000");
        Assert.assertEquals(batchManager.size(), 4999);
        Assert.assertEquals(batchManager.getPartitions().get(0).getBatchCount(), 1);
        Assert.assertEquals(batchManager.getPartitions().get(0).getMaxCapacity(), 10000);
        Assert.assertEquals(batchManager.getPartitions().get(0).getMinCapacity(), 0);
    }
}
