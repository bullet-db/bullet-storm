/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements the LIMIT operation on multiple raw {@link BulletRecord}.
 *
 * A call to {@link #getSerializedAggregation()} will <strong>only returns and removes </strong> the oldest consumed
 * record. It does <strong>not</strong> serialize the entire aggregation. This is because the strategy is currently
 * meant to be used in two contexts or modes:
 *
 * 1) Micro-Batch: For each {@link #consume(BulletRecord)} call, a call to {@link #getSerializedAggregation()} will
 * return the successful serialized consumed record. {@link #isMicroBatch()} will return true till the record is
 * serialized. Further calls to get the serialized record will return null till the next successful
 * consumption. Checks for micro batching will also return false.
 *
 * This {@link Strategy} will consume till the specified aggregation size is reached.
 *
 * To reiterate, {@link #getSerializedAggregation()} <strong>only</strong> returns <strong>once</strong>the serialized
 * byte[] representing the oldest successfully consumed {@link BulletRecord}. In other words, the size of the
 * micro-batch is <strong>one</strong> record. This is done in order to not store too many records when simply
 * consuming.
 *
 * 2) Combining: For each {@link #combine(byte[])} call (where each byte[] should be a single record because that is
 * the result of {@link #getSerializedAggregation()}), a call to {@link #getAggregation()} will return a {@link List}
 * combined so far. Further calls will <strong>return null</strong>. This {@link Strategy} will also only
 * combine till the specified aggregation size is reached. In this mode, the strategy is not doing micro-batching.
 *
 * If you mix and match these calls, the aggregation will work as intended (consume and combine till the aggregation
 * size is reached) but will be inefficient when serializing. Serializing will still only serialize the oldest consumed
 * record and will be a O(n) operation where n is the number of records consumed/combined so far.
 *
 */
@Slf4j
public class Raw implements Strategy {
    private List<BulletRecord> aggregate = new ArrayList<>();

    private Integer size;
    private int consumed = 0;
    private int combined = 0;

    /**
     * Constructor that takes in an {@link Aggregation}. The size of the aggregation is used as a LIMIT
     * operation.
     *
     * @param aggregation The {@link Aggregation} that specifies how and what this will compute.
     */
    public Raw(Aggregation aggregation) {
        size = aggregation.getSize();
    }

    @Override
    public boolean isAcceptingData() {
        return consumed + combined < size;
    }

    @Override
    public boolean isMicroBatch() {
        // Anything more than a single record is a micro-batch
        return aggregate.size() > 0;
    }

    @Override
    public void consume(BulletRecord data) {
        if (!isAcceptingData() || data == null) {
            return;
        }
        consumed++;
        aggregate.add(data);
    }

    /**
     * In the case of a Raw aggregation, serializing means return the serialized version of the last
     * {@link BulletRecord} seen. Once the data has been serialized, further calls to obtain it again
     * will result in nulls. In other words, this method can only return a valid byte[] once per
     * successful consume call.
     *
     * @return the serialized byte[] representing the last {@link BulletRecord} or null if it could not.
     */
    @Override
    public byte[] getSerializedAggregation() {
        if (aggregate.isEmpty()) {
            return null;
        }
        // This call is cheap if we are sticking to the consume -> getSerializedAggregation, since removing a single
        // element from the list does not do a array copy.
        BulletRecord batch = aggregate.remove(0);
        try {
            return batch.getAsByteArray();
        } catch (IOException ioe) {
            log.error("Could not serialize BulletRecord", batch);
        }
        return null;
    }

    /**
     * Since {@link #getSerializedAggregation()} returns a single serialized {@link BulletRecord}, this method consumes
     * a single serialized {@link BulletRecord}. It also stops combining if more than the maximum allowed by the
     * {@link Aggregation} has been reached.
     *
     * @param serializedAggregation A serialized {@link BulletRecord}.
     */
    @Override
    public void combine(byte[] serializedAggregation) {
        if (!isAcceptingData() || serializedAggregation == null) {
            return;
        }
        combined++;
        aggregate.add(new BulletRecord(serializedAggregation));
    }

    /**
     * Gets the combined records so far.
     *
     * @return a List of the combined {@link BulletRecord} so far. The List has a size that is at most the maximum
     * specified by the {@link Aggregation}.
     */
    @Override
    public List<BulletRecord> getAggregation() {
        // Guaranteed to be <= size
        List<BulletRecord> batch = aggregate;
        aggregate = new ArrayList<>();
        return batch;
    }
}
