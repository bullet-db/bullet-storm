/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.batching;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Partitions have a max capacity of batch size times batch count. When the maximum capacity is exceeded, the partition
 * is resized, and the number of batches is doubled. When the partition size drops under the minimum capacity (25% of
 * the maximum capacity), the number of batches is halved. (When the batch count is 1, the minimum capacity is 0)
 */
@Getter(AccessLevel.PACKAGE)
@Slf4j
class Partition<T> {
    private static final int INITIAL_BATCH_COUNT = 1;

    private final Random random = new Random();
    private final int id;
    private final int batchSize;
    private final boolean batchCompressEnable;
    private int batchCount;
    private int maxCapacity;
    private int minCapacity;
    private List<Map<String, T>> batches;
    private Map<String, Integer> keyMapping;
    private List<byte[]> data;
    private boolean[] changed;

    Partition(int id, int batchSize, boolean batchCompressEnable) {
        log.info("Creating partition {} with batch size {}, initial batch count {}, and batch compression enabled {}",
                id, batchSize, batchCompressEnable);

        if (batchSize < 1) {
            throw new RuntimeException("Batch size must be greater than 0. The parameter given was " + batchSize);
        }

        this.id = id;
        this.batchSize = batchSize;
        this.batchCompressEnable = batchCompressEnable;

        batches = new ArrayList<>();
        keyMapping = new HashMap<>();

        resize(INITIAL_BATCH_COUNT);
    }

    void add(String key, T value) {
        if (keyMapping.containsKey(key)) {
            return;
        }
        int index = random.nextInt(batchCount);
        batches.get(index).put(key, value);
        keyMapping.put(key, index);
        changed[index] = true;
    }

    void remove(String key) {
        Integer index = keyMapping.remove(key);
        if (index != null) {
            batches.get(index).remove(key);
            changed[index] = true;
        }
    }

    void resize() {
        if (keyMapping.size() > maxCapacity) {
            upsize();
        } else if (keyMapping.size() < minCapacity) {
            downsize();
        }
    }

    private void upsize() {
        log.info("Upsizing partition {}", id);
        while (keyMapping.size() > maxCapacity) {
            batchCount *= 2;
            maxCapacity = batchCount * batchSize;
            minCapacity = maxCapacity / 4;
        }
        resize(batchCount);
    }

    private void downsize() {
        log.info("Downsizing partition {}", id);
        while (keyMapping.size() < minCapacity) {
            batchCount /= 2;
            maxCapacity = batchCount * batchSize;
            minCapacity = maxCapacity / 4;
        }
        resize(batchCount);
    }

    /**
     * Resizes the partition to the specified number of batches.
     */
    private void resize(int numBatches) {
        batchCount = numBatches;
        maxCapacity = batchCount * batchSize;
        minCapacity = maxCapacity / 4;
        data = new ArrayList<>();
        changed = new boolean[batchCount];

        if (batchCount == 1) {
            minCapacity = 0;
        }

        log.info("Resizing partition {} batch size to {} with new max capacity {} and new min capacity {} (the current number of elements is {})",
                id, numBatches, maxCapacity, minCapacity, keyMapping.size());

        List<Map<String, T>> resized = Stream.generate(HashMap<String, T>::new).limit(batchCount).collect(Collectors.toList());
        for (Map<String, T> batch : batches) {
            for (Map.Entry<String, T> entry : batch.entrySet()) {
                String key = entry.getKey();
                int index = random.nextInt(batchCount);
                resized.get(index).put(key, entry.getValue());
                keyMapping.put(key, index);
            }
        }
        batches = resized;

        if (batchCompressEnable) {
            log.info("Compressing {} batches after resize", numBatches);
            long timestamp = System.currentTimeMillis();
            batches.stream().map(BatchManager::compress).forEach(data::add);
            log.info("Took {} seconds to compress.", (System.currentTimeMillis() - timestamp) / 1000.0);
        } else {
            log.warn("Not compressing batches after resize since compression is not enabled.");
        }
    }

    /**
     * Only compresses batches that have changed.
     */
    void compress() {
        long timestamp = System.currentTimeMillis();
        int count = 0;
        for (int i = 0; i < batchCount; i++) {
            if (changed[i]) {
                data.set(i, BatchManager.compress(batches.get(i)));
                changed[i] = false;
                count++;
            }
        }
        log.info("{} out of {} batches needed compressing. Took {} seconds to compress.", count, batchCount, (System.currentTimeMillis() - timestamp) / 1000.0);
    }

    List<Map<String, T>> getImmutableBatches() {
        return batches.stream().map(HashMap::new).collect(Collectors.toList());
    }

    List<byte[]> getImmutableData() {
        return new ArrayList<>(data);
    }

    /**
     * We clear out the maps and keep the partition size the same because this is meant to be used for activate/deactivate in ReplayableQuerySpout.
     * When repopulating queries, we mostly likely won't have to resize.
     */
    void clear() {
        batches.forEach(Map::clear);
        keyMapping.clear();
        if (batchCompressEnable) {
            for (int i = 0; i < batchCount; i++) {
                data.set(i, null);
                changed[i] = true;
            }
        }
    }
}
