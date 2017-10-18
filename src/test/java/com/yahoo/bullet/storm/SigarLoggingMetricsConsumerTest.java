/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import org.apache.storm.Config;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class SigarLoggingMetricsConsumerTest {
    @Test
    public void testRegister() {
        Config config = new Config();
        Assert.assertNull(config.get(Config.TOPOLOGY_WORKER_METRICS));
        Assert.assertNull(config.get(Config.TOPOLOGY_METRICS_CONSUMER_REGISTER));

        SigarLoggingMetricsConsumer.register(config, null);

        Assert.assertNotNull(config.get(Config.TOPOLOGY_WORKER_METRICS));
        Assert.assertNotNull(config.get(Config.TOPOLOGY_METRICS_CONSUMER_REGISTER));

        Map<String, String> metrics = (Map<String, String>) config.get(Config.TOPOLOGY_WORKER_METRICS);
        Assert.assertEquals(metrics, SigarLoggingMetricsConsumer.METRICS);

        List<Object> register = (List<Object>) config.get(Config.TOPOLOGY_METRICS_CONSUMER_REGISTER);
        Assert.assertEquals(register.size(), 1);
        Map<Object, Object> actual = (Map<Object, Object>) register.get(0);
        Map<Object, Object> expected = new HashMap<>();
        expected.put("class", LoggingMetricsConsumer.class.getCanonicalName());
        expected.put("parallelism.hint", 1L);
        expected.put("argument", null);
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testMetricsAdditionNotReplacement() {
        Config config = new Config();
        Map<String, String> metrics = new HashMap<>();
        metrics.put("foo", "foo.bar.baz");
        config.put(Config.TOPOLOGY_WORKER_METRICS, metrics);

        SigarLoggingMetricsConsumer.register(config, null);

        Assert.assertNotNull(config.get(Config.TOPOLOGY_WORKER_METRICS));

        Map<String, String> actual = (Map<String, String>) config.get(Config.TOPOLOGY_WORKER_METRICS);
        Assert.assertTrue(actual.keySet().containsAll(SigarLoggingMetricsConsumer.METRICS.keySet()));
        Assert.assertTrue(actual.values().containsAll(SigarLoggingMetricsConsumer.METRICS.values()));
        Assert.assertEquals(actual.get("foo"), "foo.bar.baz");
    }
}
