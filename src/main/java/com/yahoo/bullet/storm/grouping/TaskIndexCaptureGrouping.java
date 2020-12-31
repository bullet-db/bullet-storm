/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.grouping;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Custom grouping used to capture the mapping from task id to task index. This mapping is the same across all custom groupings
 * from one component to another.
 */
public class TaskIndexCaptureGrouping implements CustomStreamGrouping {
    public static final Map<Integer, Integer> TASK_INDEX_MAP = new HashMap<>();

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        synchronized (TASK_INDEX_MAP) {
            if (TASK_INDEX_MAP.isEmpty()) {
                for (int i = 0; i < targetTasks.size(); i++) {
                    TASK_INDEX_MAP.put(targetTasks.get(i), i);
                }
            }
        }
    }

    @Override
    public List<Integer> chooseTasks(int taskID, List<Object> values) {
        throw new RuntimeException("Tried to emit using TaskIndexCaptureGrouping from task id " + taskID);
    }
}
