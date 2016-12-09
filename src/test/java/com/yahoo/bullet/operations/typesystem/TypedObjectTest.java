/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.typesystem;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

public class TypedObjectTest {
    @Test
    public void testTypedObjectWithUnsupportedType() {
        TypedObject object = new TypedObject(Collections.emptyList());
        Assert.assertEquals(object.getType(), Type.STRING);
        Assert.assertEquals(object.getValue(), Collections.emptyList().toString());
    }

    @Test
    public void testToString() {
        TypedObject object = new TypedObject(null);
        Assert.assertEquals(object.getType(), Type.NULL);
        Assert.assertEquals(object.toString(), Type.NULL_EXPRESSION);
        object = new TypedObject("foo");
        Assert.assertEquals(object.getType(), Type.STRING);
        Assert.assertEquals(object.toString(), "foo");
    }

    @Test
    public void testTypeCasting() {
        TypedObject object = new TypedObject(1L);
        TypedObject castedToObjectType = object.typeCast("1234");
        Assert.assertEquals(castedToObjectType.getType(), Type.LONG);
        Assert.assertEquals(castedToObjectType.getValue(), 1234L);

        object = new TypedObject(1.123);
        castedToObjectType = object.typeCast("1234");
        Assert.assertEquals(castedToObjectType.getType(), Type.DOUBLE);
        Assert.assertEquals(castedToObjectType.getValue(), 1234.0);

        object = new TypedObject(true);
        castedToObjectType = object.typeCast("false");
        Assert.assertEquals(castedToObjectType.getType(), Type.BOOLEAN);
        Assert.assertEquals(castedToObjectType.getValue(), false);

        object = new TypedObject("foo");
        castedToObjectType = object.typeCast("false");
        Assert.assertEquals(castedToObjectType.getType(), Type.STRING);
        Assert.assertEquals(castedToObjectType.getValue(), "false");
    }

    @Test
    public void testFailTypeCasting() {
        TypedObject object = new TypedObject(1L);
        Assert.assertNull(object.typeCast("1234.0"));

        object = new TypedObject(Type.MAP, Collections.emptyMap());
        Assert.assertNull(object.typeCast("{}"));

        object = new TypedObject(Type.LIST, Collections.emptyList());
        Assert.assertNull(object.typeCast("[]"));
    }

    @Test(expectedExceptions = ClassCastException.class)
    public void testUnUnifiedTypeComparison() {
        TypedObject objectA = new TypedObject(1L);
        TypedObject objectB = new TypedObject("1");
        objectA.compareTo(objectB);
    }

    @Test
    public void testBooleanComparison() {
        TypedObject objectA = new TypedObject(true);
        TypedObject objectB = new TypedObject(false);
        Assert.assertTrue(objectA.compareTo(objectB) > 0);
        Assert.assertTrue(objectB.compareTo(objectA) < 0);
        Assert.assertTrue(objectA.compareTo(objectA) == 0);
    }

    @Test
    public void testStringComparison() {
        TypedObject objectA = new TypedObject("foo");
        TypedObject objectB = new TypedObject("bar");
        Assert.assertTrue(objectA.compareTo(objectB) > 0);
        Assert.assertTrue(objectB.compareTo(objectA) < 0);
        Assert.assertTrue(objectA.compareTo(objectA) == 0);
    }

    @Test
    public void testLongComparison() {
        TypedObject objectA = new TypedObject(42L);
        TypedObject objectB = new TypedObject(43L);
        Assert.assertTrue(objectA.compareTo(objectB) < 0);
        Assert.assertTrue(objectB.compareTo(objectA) > 0);
        Assert.assertTrue(objectA.compareTo(objectA) == 0);
    }

    @Test
    public void testDoubleComparison() {
        TypedObject objectA = new TypedObject(42.0);
        TypedObject objectB = new TypedObject(42.1);
        Assert.assertTrue(objectA.compareTo(objectB) < 0);
        Assert.assertTrue(objectB.compareTo(objectA) > 0);
        Assert.assertTrue(objectA.compareTo(objectA) == 0);
    }

    @Test
    public void testNullComparisonToOthers() {
        TypedObject objectA = new TypedObject(null);
        TypedObject objectB = new TypedObject(42.1);
        TypedObject objectC = new TypedObject("foo");
        Assert.assertEquals(objectA.compareTo(objectB), Integer.MIN_VALUE);
        Assert.assertEquals(objectA.compareTo(objectC), Integer.MIN_VALUE);
        Assert.assertTrue(objectA.compareTo(objectA) == 0);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testOtherComparisonToNull() {
        TypedObject nullObject = new TypedObject(null);
        TypedObject object = new TypedObject(42.1);
        object.compareTo(nullObject);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported type cannot .*")
    public void testUnsupportedTypeComparison() {
        TypedObject object = new TypedObject(Type.MAP, Collections.emptyMap());
        object.compareTo(new TypedObject(42L));
    }
}
