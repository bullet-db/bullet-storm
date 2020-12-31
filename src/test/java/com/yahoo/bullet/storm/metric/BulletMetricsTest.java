/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.metric;

import com.yahoo.bullet.storm.BulletStormConfig;
import com.yahoo.bullet.storm.testing.CustomTopologyContext;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.ReducedMetric;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BulletMetricsTest {
    @Test
    public void testRegisterMetric() {
        BulletMetrics metrics = new BulletMetrics(new BulletStormConfig());
        CustomTopologyContext context = new CustomTopologyContext();

        AbsoluteCountMetric absoluteCountMetric = metrics.registerAbsoluteCountMetric("A", context);
        CountMetric countMetric = metrics.registerCountMetric("B", context);
        ReducedMetric reducedMetric = metrics.registerAveragingMetric("C", context);
        MapCountMetric mapCountMetric = metrics.registerMapCountMetric("D", context);

        Assert.assertEquals(context.getRegisteredMetricByName("A"), absoluteCountMetric);
        Assert.assertEquals(context.getRegisteredMetricByName("B"), countMetric);
        Assert.assertEquals(context.getRegisteredMetricByName("C"), reducedMetric);
        Assert.assertEquals(context.getRegisteredMetricByName("D"), mapCountMetric);
    }

    @Test
    public void testEnabled() {
        BulletStormConfig config = new BulletStormConfig();
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();

        BulletMetrics metrics = new BulletMetrics(config);
        Assert.assertTrue(metrics.isEnabled());

        AbsoluteCountMetric absoluteCountMetric = Mockito.mock(AbsoluteCountMetric.class);
        metrics.updateCount(absoluteCountMetric, 1L);
        metrics.setCount(absoluteCountMetric, 1L);
        Mockito.verify(absoluteCountMetric).add(1L);
        Mockito.verify(absoluteCountMetric).set(1L);

        CountMetric countMetric = Mockito.mock(CountMetric.class);
        metrics.updateCount(countMetric, 1L);
        Mockito.verify(countMetric).incrBy(1L);

        MapCountMetric mapCountMetric = Mockito.mock(MapCountMetric.class);
        metrics.updateCount(mapCountMetric, "", 1L);
        metrics.setCount(mapCountMetric, "", 1L);
        metrics.clearCount(mapCountMetric);
        Mockito.verify(mapCountMetric).add("", 1L);
        Mockito.verify(mapCountMetric).set("", 1L);
        Mockito.verify(mapCountMetric).clear();
    }

    @Test
    public void testNotEnabled() {
        BulletMetrics metrics = new BulletMetrics(new BulletStormConfig());

        Assert.assertFalse(metrics.isEnabled());

        AbsoluteCountMetric absoluteCountMetric = Mockito.mock(AbsoluteCountMetric.class);
        metrics.updateCount(absoluteCountMetric, 1L);
        metrics.setCount(absoluteCountMetric, 1L);
        Mockito.verifyNoInteractions(absoluteCountMetric);

        CountMetric countMetric = Mockito.mock(CountMetric.class);
        metrics.updateCount(countMetric, 1L);
        Mockito.verifyNoInteractions(countMetric);

        MapCountMetric mapCountMetric = Mockito.mock(MapCountMetric.class);
        metrics.updateCount(mapCountMetric, "", 1L);
        metrics.setCount(mapCountMetric, "", 1L);
        metrics.clearCount(mapCountMetric);
        Mockito.verifyNoInteractions(mapCountMetric);
    }
}
