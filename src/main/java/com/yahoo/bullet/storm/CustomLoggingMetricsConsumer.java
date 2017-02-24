package com.yahoo.bullet.storm;

import backtype.storm.Config;
import backtype.storm.metric.LoggingMetricsConsumer;
import com.yahoo.bullet.BulletConfig;

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
