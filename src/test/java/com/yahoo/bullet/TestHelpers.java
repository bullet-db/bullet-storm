/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.yahoo.bullet.record.BulletRecord;
import org.testng.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TestHelpers {
    public static <E> void assertContains(Collection<E> collection, E item) {
        Objects.requireNonNull(collection);
        assertContains(collection, item, 1);
    }

    public static <E> void assertContains(Collection<E> collection, E item, long times) {
        Objects.requireNonNull(collection);
        long actual = collection.stream().map(item::equals).filter(x -> x).count();
        Assert.assertEquals(actual, times);
    }

    public static void assertJSONEquals(String actual, String expected) {
        JsonParser parser = new JsonParser();
        JsonElement first = parser.parse(actual);
        JsonElement second = parser.parse(expected);
        Assert.assertEquals(first, second);
    }

    public static byte[] getListBytes(BulletRecord... records) {
        List<BulletRecord> asList = new ArrayList<>();
        Collections.addAll(asList, records);
        return serialize(asList);
    }

    public static byte[] serialize(Object o) {
        try (
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
        ) {
            oos.writeObject(o);
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Object deserialize(byte[] o) {
        try (
            ByteArrayInputStream bis = new ByteArrayInputStream(o);
            ObjectInputStream ois = new ObjectInputStream(bis);
        ) {
            return ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
