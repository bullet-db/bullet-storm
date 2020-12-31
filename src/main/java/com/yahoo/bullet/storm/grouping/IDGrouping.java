/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.grouping;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.yahoo.bullet.storm.StormUtils.getHashIndex;
import static com.yahoo.bullet.storm.TopologyConstants.ID_POSITION;

/**
 * Custom grouping used to group by the id (first) field.
 */
public class IDGrouping implements CustomStreamGrouping {
    private List<List<Integer>> tasks;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        tasks = targetTasks.stream().map(Collections::singletonList).collect(Collectors.toList());
    }

    @Override
    public List<Integer> chooseTasks(int taskID, List<Object> values) {
        if (values == null || values.isEmpty()) {
            throw new RuntimeException("Tried to emit an empty tuple.");
        }
        Object key = values.get(ID_POSITION);
        int index = getHashIndex(key, tasks.size());
        return tasks.get(index);
    }
}
