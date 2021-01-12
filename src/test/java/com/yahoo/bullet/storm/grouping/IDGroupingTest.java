/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.grouping;

import com.yahoo.bullet.storm.StormUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IDGroupingTest {
    private static final int HASH_COUNT = 4;

    @Test
    public void testChooseTasks() {
        IDGrouping grouping = new IDGrouping();

        List<Integer> targetTasks = Arrays.asList(20, 21, 22, 23);

        grouping.prepare(null, null, targetTasks);

        int expected = targetTasks.get(StormUtils.getHashIndex("42", HASH_COUNT));

        Assert.assertEquals(grouping.chooseTasks(0, Arrays.asList("42")).get(0).intValue(), expected);
        Assert.assertEquals(grouping.chooseTasks(0, Arrays.asList("42", 123)).get(0).intValue(), expected);
        Assert.assertEquals(grouping.chooseTasks(0, Arrays.asList("42", null, 456)).get(0).intValue(), expected);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Tried to emit an empty tuple\\.")
    public void testChooseTasksThrowsOnEmptyTuple() {
        new IDGrouping().chooseTasks(0, Collections.emptyList());
    }
}
