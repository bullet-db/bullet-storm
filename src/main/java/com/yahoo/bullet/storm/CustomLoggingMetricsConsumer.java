/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import backtype.storm.Config;
import backtype.storm.metric.LoggingMetricsConsumer;
import com.yahoo.bullet.common.BulletConfig;

public class CustomLoggingMetricsConsumer extends LoggingMetricsConsumer {
    /**
     * Registers the LoggingMetricsConsumer with a parallelism of 1.
     *
     * @param stormConfig  The Storm {@link Config} to add to.
     * @param bulletConfig The Bullet {@link BulletConfig} to get information from.
     */
    public static void register(Config stormConfig, BulletConfig bulletConfig) {
        stormConfig.registerMetricsConsumer(LoggingMetricsConsumer.class);
    }
}
