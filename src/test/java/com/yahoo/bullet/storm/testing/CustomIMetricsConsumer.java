/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import backtype.storm.Config;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import com.yahoo.bullet.storm.BulletStormConfig;

import java.util.Collection;
import java.util.Map;

public class CustomIMetricsConsumer implements IMetricsConsumer {
    public static final String CUSTOM_METRICS_REGISTERED = "bullet.topology.custom.metrics.consumer.was.registered";
    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
    }

    @Override
    public void cleanup() {
    }

    public static void register(Config stormConfig, BulletStormConfig bulletStormConfig) {
        bulletStormConfig.set(CUSTOM_METRICS_REGISTERED, true);
        stormConfig.registerMetricsConsumer(CustomIMetricsConsumer.class);
    }
}
