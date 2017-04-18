/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.Utilities;
import com.yahoo.bullet.operations.SerializerDeserializer;
import com.yahoo.bullet.operations.aggregations.grouping.GroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;

@Slf4j
public class GroupAll implements Strategy {
    // We only have a single group.
    private GroupData data;

    private Set<GroupOperation> operations;
    /**
     * Constructor that takes in an {@link Aggregation}. Requires the aggregation to have generated its group operations.
     *
     * @param aggregation The {@link Aggregation} that specifies how and what this will compute.
     */
    public GroupAll(Aggregation aggregation) {
        // GroupOperations is all we care about - size etc. are meaningless for Group All since it's a single result
        operations = GroupOperation.getOperations(aggregation.getAttributes());
        data = new GroupData(operations);
    }

    @Override
    public List<Error> initialize() {
        if (Utilities.isEmpty(operations)) {
            return singletonList(GroupOperation.REQUIRES_FIELD_OR_OPERATION_ERROR);
        }
        return GroupOperation.checkOperations(operations);
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
        return SerializerDeserializer.toBytes(data);
    }

    @Override
    public Clip getAggregation() {
        return Clip.of(data.getMetricsAsBulletRecord());
    }
}
