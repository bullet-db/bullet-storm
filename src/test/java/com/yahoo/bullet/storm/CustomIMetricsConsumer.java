/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;

import java.util.Collection;
import java.util.Map;

public class CustomIMetricsConsumer implements IMetricsConsumer {
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
        stormConfig.registerMetricsConsumer(CustomIMetricsConsumer.class);
    }
}
