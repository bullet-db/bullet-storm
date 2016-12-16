/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static java.util.Collections.singletonList;

@Slf4j
public class GroupAll implements Strategy {
    // We only have a single group.
    private GroupData data;

    /**
     * Constructor that takes in an {@link Aggregation}. Requires the aggregation to have generated its group operations.
     *
     * @param aggregation The {@link Aggregation} that specifies how and what this will compute.
     */
    public GroupAll(Aggregation aggregation) {
        // GroupOperations is all we care about - size etc. are meaningless for Group All since it's a single result
        data = new GroupData(aggregation.getGroupOperations());
    }

    @Override
    public void consume(BulletRecord data) {
        this.data.consume(data);
    }

    @Override
    public void combine(byte[] serializedAggregation) {
        data.combine(serializedAggregation);
    }

    @Override
    public byte[] getSerializedAggregation() {
        return GroupData.toBytes(data);
    }

    @Override
    public List<BulletRecord> getAggregation() {
        return singletonList(data.getAsBulletRecord());
    }
}
