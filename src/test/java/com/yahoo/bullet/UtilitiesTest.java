package com.yahoo.bullet;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UtilitiesTest {
    @Test
    public void testCasting() {
        Map<String, Object> map = new HashMap<>();
        map.put("foo", 1L);
        Long actual = Utilities.getCasted(map, "foo", Long.class);
        Assert.assertEquals(actual, (Long) 1L);
    }

    @Test
    public void testFailCasting() {
        Map<String, Object> map = new HashMap<>();
        map.put("foo", "bar");
        Long actual = Utilities.getCasted(map, "foo", Long.class);
        Assert.assertNull(actual);
    }

    @Test(expectedExceptions = ClassCastException.class)
    public void testCastingOnGenerics() {
        Map<String, Object> map = new HashMap<>();
        Map<Integer, Long> anotherMap = new HashMap<>();
        anotherMap.put(1, 2L);

        map.put("foo", anotherMap);

        Map<String, String> incorrect = Utilities.getCasted(map, "foo", Map.class);
        // It is a map but the generics are incorrect
        Assert.assertNotNull(incorrect);
        String value = incorrect.get(1);
    }

    @Test
    public void testEmptyMap() {
        Assert.assertTrue(Utilities.isEmpty((Map) null));
        Assert.assertTrue(Utilities.isEmpty(Collections.emptyMap()));
        Assert.assertFalse(Utilities.isEmpty(Collections.singletonMap("foo", "bar")));
    }

    @Test
    public void testEmptyCollection() {
        Assert.assertTrue(Utilities.isEmpty((List) null));
        Assert.assertTrue(Utilities.isEmpty(Collections.emptyList()));
        Assert.assertFalse(Utilities.isEmpty(Collections.singletonList("foo")));
    }

    @Test
    public void testEmptyString() {
        Assert.assertTrue(Utilities.isEmpty((String) null));
        Assert.assertTrue(Utilities.isEmpty(""));
        Assert.assertFalse(Utilities.isEmpty("foo"));
    }
}