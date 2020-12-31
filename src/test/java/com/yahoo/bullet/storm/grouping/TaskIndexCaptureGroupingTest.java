/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.grouping;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

public class TaskIndexCaptureGroupingTest {
    @Test
    public void testPrepareTaskIndexMap() {
        Assert.assertEquals(TaskIndexCaptureGrouping.TASK_INDEX_MAP.size(), 0);

        new TaskIndexCaptureGrouping().prepare(null, null, Arrays.asList(20, 21, 22, 23));

        Assert.assertEquals(TaskIndexCaptureGrouping.TASK_INDEX_MAP.size(), 4);
        Assert.assertEquals(TaskIndexCaptureGrouping.TASK_INDEX_MAP.get(20).intValue(), 0);
        Assert.assertEquals(TaskIndexCaptureGrouping.TASK_INDEX_MAP.get(21).intValue(), 1);
        Assert.assertEquals(TaskIndexCaptureGrouping.TASK_INDEX_MAP.get(22).intValue(), 2);
        Assert.assertEquals(TaskIndexCaptureGrouping.TASK_INDEX_MAP.get(23).intValue(), 3);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Tried to emit using TaskIndexCaptureGrouping.*")
    public void testChooseTasksThrows() {
        new TaskIndexCaptureGrouping().chooseTasks(0, null);
    }
}
