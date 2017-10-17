/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class DRPCTupleTest {
    private List<Object> data;
    private DRPCTuple tuple;

    @BeforeMethod
    public void setup() {
        data = new ArrayList<>();
        data.add("foo");
        data.add(1);
        tuple = new DRPCTuple(data);
    }

    @Test
    public void gettingByIndex() {
        Assert.assertEquals(tuple.getValue(0), "foo");
        Assert.assertEquals(tuple.getValue(1), 1);

        Assert.assertEquals(tuple.getString(0), "foo");
        Assert.assertEquals(tuple.getString(1), "1");
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void gettingOutOfRangeValue() {
        tuple.getValue(2);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void gettingOutOfRangeString() {
        tuple.getString(2);
    }

    @Test
    public void testGettingAllData() {
        Assert.assertEquals(tuple.getValues(), data);
    }

    @Test
    public void testingSize() {
        Assert.assertEquals(tuple.size(), 2);
        data.add("bar");
        Assert.assertEquals(tuple.size(), 3);
    }

    // Unimplemented methods

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetSourceGlobalStreamid() {
        tuple.getSourceGlobalStreamid();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetSourceComponent() {
        tuple.getSourceComponent();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetSourceTask() {
        tuple.getSourceTask();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetSourceStreamId() {
        tuple.getSourceStreamId();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetMessageId() {
        tuple.getMessageId();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testContains() {
        tuple.contains(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetFields() {
        tuple.getFields();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testFieldIndex() {
        tuple.fieldIndex(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testSelect() {
        tuple.select(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetInteger() {
        tuple.getInteger(0);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetLong() {
        tuple.getLong(0);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetBoolean() {
        tuple.getBoolean(0);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetShort() {
        tuple.getShort(0);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetByte() {
        tuple.getByte(0);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetDouble() {
        tuple.getDouble(0);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetFloat() {
        tuple.getFloat(0);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetBinary() {
        tuple.getBinary(0);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetValueByField() {
        tuple.getValueByField(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetStringByField() {
        tuple.getStringByField(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetIntegerByField() {
        tuple.getIntegerByField(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetLongByField() {
        tuple.getLongByField(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetBooleanByField() {
        tuple.getBooleanByField(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetShortByField() {
        tuple.getShortByField(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetByteByField() {
        tuple.getByteByField(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetDoubleByField() {
        tuple.getDoubleByField(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetFloatByField() {
        tuple.getFloatByField(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetBinaryByField() {
        tuple.getBinaryByField(null);
    }
}
