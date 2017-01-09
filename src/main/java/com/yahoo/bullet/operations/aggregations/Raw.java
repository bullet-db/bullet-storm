/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implements the LIMIT operation on multiple raw {@link BulletRecord}.
 *
 * A call to {@link #getSerializedAggregation()} will return and removes the current collection of records, which
 * is a {@link List} of {@link BulletRecord}.
 *
 * A call to {@link #combine(byte[])} with the result of {@link #getSerializedAggregation()} will combine records from
 * the {@link List} till the aggregation size is reached.
 *
 * This {@link Strategy} will only consume or combine till the specified aggregation size is reached.
 */
@Slf4j
public class Raw implements Strategy {
    public static final Integer DEFAULT_MICRO_BATCH_SIZE = 1;
    private List<BulletRecord> aggregate = new ArrayList<>();

    private Integer size;
    private int consumed = 0;
    private int combined = 0;
    private int microBatchSize = DEFAULT_MICRO_BATCH_SIZE;

    /**
     * Constructor that takes in an {@link Aggregation}. The size of the aggregation is used as a LIMIT
     * operation.
     *
     * @param aggregation The {@link Aggregation} that specifies how and what this will compute.
     */
    public Raw(Aggregation aggregation) {
        size = aggregation.getSize();
        microBatchSize = ((Number) aggregation.getConfiguration().getOrDefault(BulletConfig.RAW_AGGREGATION_MICRO_BATCH_SIZE,
                                                                               DEFAULT_MICRO_BATCH_SIZE)).intValue();
    }

    @Override
    public boolean isAcceptingData() {
        return consumed + combined < size;
    }

    @Override
    public boolean isMicroBatch() {
        // Anything more than a single record is a micro-batch
        return aggregate.size() >= microBatchSize;
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
     * Since {@link #getSerializedAggregation()} returns a {@link List} of {@link BulletRecord}, this method consumes
     * that list. If the deserialized List has a size that takes the aggregated records above the aggregation size, only
     * the first X records in the List will be combined till the size is reached.
     *
     * @param serializedAggregation A serialized {@link List} of {@link BulletRecord}.
     */
    @Override
    public void combine(byte[] serializedAggregation) {
        if (!isAcceptingData() || serializedAggregation == null) {
            return;
        }
        List<BulletRecord> batch = read(serializedAggregation);
        if (batch.isEmpty()) {
            return;
        }
        int batchSize = batch.size();
        int maximumLeft = size - aggregate.size();
        if (batchSize <= maximumLeft) {
            aggregate.addAll(batch);
            combined += batchSize;
        } else {
            aggregate.addAll(batch.subList(0, maximumLeft));
            combined += maximumLeft;
        }
    }

    /**
     * In the case of a Raw aggregation, serializing means return the serialized {@link List} of
     * {@link BulletRecord} seen before the last call to this method. Once the data has been serialized, further calls
     * to obtain it again without calling {@link #consume(BulletRecord)} or {@link #combine(byte[])} will result in
     * nulls. In other words, the Raw strategy micro-batches and finalizes the aggregation so far when this
     * method is called.
     *
     * @return the serialized byte[] representing the {@link List} of {@link BulletRecord} or null if it could not.
     */
    @Override
    public byte[] getSerializedAggregation() {
        if (aggregate.isEmpty()) {
            return null;
        }
        List<BulletRecord> batch = aggregate;
        aggregate = new ArrayList<>();
        return write(batch);
    }

    /**
     * Gets the aggregated records so far since the last call to {@link #getSerializedAggregation()}.
     *
     * @return a {@link Clip} of the combined records so far. The records have a size that is at most the maximum
     * specified by the {@link Aggregation}.
     */
    @Override
    public Clip getAggregation() {
        return Clip.of(aggregate);
    }

    private byte[] write(List<BulletRecord> batch) {
        try (
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos)
        ) {
            oos.writeObject(batch);
            return bos.toByteArray();
        } catch (IOException ioe) {
            log.error("Could not serialize batch {}", batch);
            log.error("Exception was ", ioe);
        }
        return null;
    }

    private List<BulletRecord> read(byte[] batch) {
        try (
            ByteArrayInputStream bis = new ByteArrayInputStream(batch);
            ObjectInputStream ois = new ObjectInputStream(bis)
        ) {
            return (List<BulletRecord>) ois.readObject();
        } catch (IOException | ClassNotFoundException | ClassCastException e) {
            log.error("Could not deserialize batch {}", batch);
            log.error("Exception was ", e);
        }
        return Collections.emptyList();
    }
}
