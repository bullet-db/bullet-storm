/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class BulletConfigTest {

    @Test
    public void testNoFiles() throws IOException {
        BulletConfig config = new BulletConfig(null);
        Assert.assertEquals(config.get(BulletConfig.TOPOLOGY_NAME), "bullet-topology");
        config = new BulletConfig("");
        Assert.assertEquals(config.get(BulletConfig.TOPOLOGY_NAME), "bullet-topology");
    }

    @Test
    public void testCustomConfig() throws IOException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        Assert.assertEquals(config.get(BulletConfig.TOPOLOGY_NAME), "test");
        Assert.assertEquals(config.get(BulletConfig.TOPOLOGY_WORKERS), 92L);
        Assert.assertEquals(config.get(BulletConfig.DRPC_SPOUT_CPU_LOAD), 20.0);
        Assert.assertEquals(config.get(BulletConfig.TOPOLOGY_FUNCTION), "foo");
    }

    @Test
    public void testCustomProperties() throws IOException {
        BulletConfig config = new BulletConfig();
        Assert.assertNull(config.get("foo"));
        config.set("foo", "bar");
        Assert.assertEquals(config.get("foo"), "bar");
    }

    @Test
    public void testGettingWithDefault() throws IOException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        Assert.assertEquals(config.getOrDefault(BulletConfig.TOPOLOGY_NAME, "foo"), "test");
        Assert.assertEquals(config.getOrDefault("does.not.exist", "foo"), "foo");
        Assert.assertEquals(config.getOrDefault("fake.setting", "bar"), "bar");
    }

    @Test
    public void testGettingMultipleProperties() throws IOException {
        BulletConfig config = new BulletConfig();
        config.clear();
        config.set("1", 1);
        config.set("pi", 3.14);
        config.set("foo", "bar");
        config.set("true", true);

        Optional<Set<String>> keys = Optional.of(new HashSet<>(Arrays.asList("1", "true")));
        Map<String, Object> mappings = config.getAll(keys);
        Assert.assertEquals(mappings.size(), 2);
        Assert.assertEquals(mappings.get("1"), 1);
        Assert.assertEquals(mappings.get("true"), true);

        mappings = config.getAll(Optional.empty());
        Assert.assertEquals(mappings.size(), 4);
        Assert.assertEquals(mappings.get("1"), 1);
        Assert.assertEquals(mappings.get("pi"), 3.14);
        Assert.assertEquals(mappings.get("foo"), "bar");
        Assert.assertEquals(mappings.get("true"), true);
    }

    @Test
    public void testGettingMaskedProperties() throws IOException {
        BulletConfig config = new BulletConfig();
        config.clear();
        config.set("1", 1);
        config.set("pi", 3.14);
        config.set("foo", "bar");
        config.set("true", true);

        Optional<Set<String>> keys = Optional.of(new HashSet<>(Arrays.asList("1", "true")));
        Map<String, Object> mappings = config.getAllBut(keys);
        Assert.assertEquals(mappings.size(), 2);
        Assert.assertEquals(mappings.get("foo"), "bar");
        Assert.assertEquals(mappings.get("pi"), 3.14);

        mappings = config.getAllBut(Optional.empty());
        Assert.assertEquals(mappings.size(), 4);
        Assert.assertEquals(mappings.get("1"), 1);
        Assert.assertEquals(mappings.get("pi"), 3.14);
        Assert.assertEquals(mappings.get("foo"), "bar");
        Assert.assertEquals(mappings.get("true"), true);
    }

    @Test
    public void testGettingBulletSettingsOnly() throws IOException {
        BulletConfig config = new BulletConfig();
        Map<String, Object> settings = config.getNonTopologySubmissionSettings();
        BulletConfig.TOPOLOGY_SUBMISSION_SETTINGS.stream().forEach(s -> Assert.assertFalse(settings.containsKey(s)));
        Assert.assertTrue(settings.size() > 0);
    }
}
