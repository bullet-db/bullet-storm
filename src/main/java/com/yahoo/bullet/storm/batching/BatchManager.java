/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.batching;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.yahoo.bullet.storm.StormUtils.getHashIndex;

/**
 * Objects in the batch manager are partitioned by key and then batched randomly within each partition. The number of
 * partitions and the batch size can be specified.
 *
 * Note that batch size is used to determine the max/min capacity of a partition and when a partition needs to be
 * resized. It is not a guarantee on the max size of a batch.
 *
 * The batch manager supports GZIP compression. Batches are only compressed when partitions are resized or when the
 * compressed batches are retrieved, in which case only changed batches will be re-compressed.
 *
 * Batches can be retrieved both partitioned and non-partitioned. These batches are separate data structures and do not
 * reflect changes to the batch manager.
 */
@Slf4j
public class BatchManager<T> {
    /* Exposed for testing only. */
    @Getter(AccessLevel.PACKAGE)
    private final List<Partition<T>> partitions;
    private final int partitionCount;
    private final boolean batchCompressEnable;

    /**
     * Creates a BatchManager with the given batch size and partition count.
     *
     * @param batchSize The approximate maximum number of elements in a batch.
     * @param partitionCount The number of partitions to use.
     * @param batchCompressEnable Whether or not to serialize and compress batches.
     */
    public BatchManager(int batchSize, int partitionCount, boolean batchCompressEnable) {
        log.info("Creating BatchManager with batch size {}, partition count {}, and batch compression enabled {}.",
                batchSize, partitionCount, batchCompressEnable);

        if (partitionCount < 1) {
            throw new RuntimeException("Partition count must be greater than 0. The parameter given was " + partitionCount);
        }

        partitions = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            partitions.add(new Partition<>(i, batchSize, batchCompressEnable));
        }

        this.partitionCount = partitionCount;
        this.batchCompressEnable = batchCompressEnable;
    }

    /**
     * Get all batches as a list of batches.
     *
     * @return A list containing all batches.
     */
    public List<Map<String, T>> getBatches() {
        return partitions.stream().map(Partition::getImmutableBatches).flatMap(Collection::stream).collect(Collectors.toList());
    }

    /**
     * Get all batches from the given partition.
     *
     * @param index The partition index.
     * @return All the batches for the given partition as a {@link List} of {@link Map}.
     */
    public List<Map<String, T>> getBatchesForPartition(int index) {
        return partitions.get(index).getImmutableBatches();
    }

    /**
     * Get all batches as lists of batches mapped by partition.
     *
     * @return A map containing all batches mapped by partition.
     */
    public Map<Integer, List<Map<String, T>>> getPartitionedBatches() {
        return partitions.stream().collect(Collectors.toMap(Partition::getId, Partition::getImmutableBatches));
    }

    /**
     * Get all batches back as a list of compressed bytes.
     *
     * @return A list containing all batches compressed.
     */
    public List<byte[]> getCompressedBatches() {
        if (!batchCompressEnable) {
            throw new RuntimeException("Throwing runtime exception since batch compression is not enabled.");
        }
        partitions.forEach(Partition::compress);
        return partitions.stream().map(Partition::getImmutableData).flatMap(Collection::stream).collect(Collectors.toList());
    }

    /**
     * Get all batches from the given partition as a list of compressed bytes.
     *
     * @param index The partition index.
     * @return All the batches for the given partition as a {@link List} of byte[].
     */
    public List<byte[]> getCompressedBatchesForPartition(int index) {
        if (!batchCompressEnable) {
            throw new RuntimeException("Throwing runtime exception since batch compression is not enabled.");
        }
        partitions.get(index).compress();
        return partitions.get(index).getImmutableData();
    }

    /**
     * Get all batches back as lists of compressed bytes mapped by partition.
     *
     * @return A map containing all batches compressed mapped by partition.
     */
    public Map<Integer, List<byte[]>> getPartitionedCompressedBatches() {
        if (!batchCompressEnable) {
            throw new RuntimeException("Throwing runtime exception since batch compression is not enabled.");
        }
        partitions.forEach(Partition::compress);
        return partitions.stream().collect(Collectors.toMap(Partition::getId, Partition::getImmutableData));
    }

    /**
     * Add all elements from the given map whose keys do not already exist.
     *
     * @param map The map of elements to add.
     */
    public void addAll(Map<String, T> map) {
        map.forEach((k, v) -> partition(k).add(k, v));
        partitions.forEach(Partition::resize);
    }

    /**
     * Remove all elements whose keys are in the given set.
     *
     * @param keys The set of keys specifying which elements to remove.
     */
    public void removeAll(Set<String> keys) {
        keys.forEach(k -> partition(k).remove(k));
        partitions.forEach(Partition::resize);
    }

    /**
     * Add an element if this key does not already exist.
     *
     * @param key The key of the element to add.
     * @param value The element to add.
     */
    public void add(String key, T value) {
        Partition<T> partition = partition(key);
        partition.add(key, value);
        partition.resize();
    }

    /**
     * Remove the element with the given key if it exists.
     *
     * @param key The key of the element to remove.
     */
    public void remove(String key) {
        Partition<T> partition = partition(key);
        partition.remove(key);
        partition.resize();
    }

    /**
     * Checks whether or not the given key exists.
     *
     * @param key The key to check exists.
     * @return True if the key exists and false otherwise.
     */
    public boolean contains(String key) {
        return partition(key).getKeyMapping().containsKey(key);
    }

    /**
     * Gets the number of elements.
     *
     * @return The number of elements.
     */
    public int size() {
        return partitions.stream().map(Partition::getKeyMapping).mapToInt(Map::size).sum();
    }

    /**
     * Clears all partitions.
     */
    public void clear() {
        partitions.forEach(Partition::clear);
    }

    private Partition<T> partition(String key) {
        return partitions.get(getHashIndex(key, partitionCount));
    }
}
