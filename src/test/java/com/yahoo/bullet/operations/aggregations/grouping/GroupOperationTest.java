/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations.grouping;

import com.yahoo.bullet.operations.AggregationOperations.GroupOperationType;
import org.testng.Assert;
import org.testng.annotations.Test;
public class GroupOperationTest {

    @Test
    public void testHashCode() {
        GroupOperation a = new GroupOperation(GroupOperationType.AVG, "foo", "avg1");
        GroupOperation b = new GroupOperation(GroupOperationType.AVG, "foo", "avg2");
        GroupOperation c = new GroupOperation(GroupOperationType.AVG, "bar", "avg1");
        GroupOperation d = new GroupOperation(GroupOperationType.COUNT, "foo", "count1");
        GroupOperation e = new GroupOperation(GroupOperationType.COUNT, "bar", "count2");

        Assert.assertEquals(a.hashCode(), b.hashCode());
        Assert.assertNotEquals(a.hashCode(), c.hashCode());
        Assert.assertNotEquals(a.hashCode(), d.hashCode());
        Assert.assertNotEquals(c.hashCode(), d.hashCode());
        Assert.assertNotEquals(a.hashCode(), e.hashCode());
        Assert.assertNotEquals(c.hashCode(), e.hashCode());
        Assert.assertNotEquals(d.hashCode(), e.hashCode());
    }

    @Test
    public void testNullFieldsForHashCode() {
        GroupOperation a = new GroupOperation(null, null, "a");
        GroupOperation b = new GroupOperation(null, "foo", "a");
        GroupOperation c = new GroupOperation(null, "bar", "a");
        GroupOperation d = new GroupOperation(null, null, "b");
        GroupOperation e = new GroupOperation(GroupOperationType.AVG, null, "avg");

        Assert.assertNotEquals(a.hashCode(), b.hashCode());
        Assert.assertNotEquals(a.hashCode(), c.hashCode());
        Assert.assertNotEquals(b.hashCode(), c.hashCode());
        Assert.assertEquals(a.hashCode(), d.hashCode());
        Assert.assertNotEquals(a.hashCode(), e.hashCode());
        Assert.assertNotEquals(b.hashCode(), e.hashCode());
        Assert.assertNotEquals(c.hashCode(), e.hashCode());
    }

    @Test
    public void testEquals() {
        GroupOperation a = new GroupOperation(GroupOperationType.AVG, "foo", "avg1");
        GroupOperation b = new GroupOperation(GroupOperationType.AVG, "foo", "avg2");
        GroupOperation c = new GroupOperation(GroupOperationType.AVG, "bar", "avg");
        GroupOperation d = new GroupOperation(GroupOperationType.COUNT, "foo", "count");
        String e = "foo";

        Assert.assertEquals(a, b);
        Assert.assertNotEquals(a, c);
        Assert.assertNotEquals(a, d);
        Assert.assertNotEquals(c, d);

        Assert.assertFalse(a.equals(e));
    }

    @Test
    public void testNullFieldsForEquals() {
        GroupOperation a = new GroupOperation(null, null, "a");
        GroupOperation b = new GroupOperation(null, "foo", "a");
        GroupOperation c = new GroupOperation(null, "bar", "a");
        GroupOperation d = new GroupOperation(null, null, "a");

        Assert.assertNotEquals(a, b);
        Assert.assertNotEquals(a, c);
        Assert.assertNotEquals(b, c);
        Assert.assertEquals(a, d);
    }
}
