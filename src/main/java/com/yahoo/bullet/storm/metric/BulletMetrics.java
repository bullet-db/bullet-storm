package com.yahoo.bullet.storm.metric;

import com.yahoo.bullet.storm.BulletStormConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

import static com.yahoo.bullet.storm.BulletStormConfig.DEFAULT_BUILT_IN_METRICS_INTERVAL_KEY;

@Slf4j
@Getter
public class BulletMetrics {
    private boolean enabled;
    private Map<String, Number> metricsIntervalMapping;

    public BulletMetrics(BulletStormConfig config) {
        enabled = config.getAs(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, Boolean.class);
        metricsIntervalMapping = config.getAs(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING, Map.class);
    }

    /**
     * Registers a metric that averages its values with the configured interval for it (if any).
     *
     * @param name The name of the metric to register.
     * @param context The {@link TopologyContext} to register with.
     * @return The registered {@link ReducedMetric} that is averaging.
     */
    public ReducedMetric registerAveragingMetric(String name, TopologyContext context) {
        return registerMetric(new ReducedMetric(new MeanReducer()), name, context);
    }

    /**
     * Registers a metric that counts values monotonically increasing with the configured interval for it (if any).
     *
     * @param name The name of the metric to register.
     * @param context The {@link TopologyContext} to register with.
     * @return The registered {@link AbsoluteCountMetric} that is counting.
     */
    public AbsoluteCountMetric registerAbsoluteCountMetric(String name, TopologyContext context) {
        return registerMetric(new AbsoluteCountMetric(), name, context);
    }

    /**
     * Registers a metric that counts values monotonically increasing and resets after read.
     *
     * @param name The name of the metric to register.
     * @param context The {@link TopologyContext} to register with.
     * @return The registered {@link CountMetric} that is counting.
     */
    public CountMetric registerCountMetric(String name, TopologyContext context) {
        return registerMetric(new CountMetric(), name, context);
    }

    /**
     * Registers a metric that counts a map of values monotonically increasing.
     *
     * @param name The name of the metric to register.
     * @param context The {@link TopologyContext} to register with.
     * @return The registered {@link MapCountMetric} that is counting.
     */
    public MapCountMetric registerMapCountMetric(String name, TopologyContext context) {
        return registerMetric(new MapCountMetric(), name, context);
    }

    /**
     * Adds the given count to the given metric.
     *
     * @param metric The {@link AbsoluteCountMetric} to add the count.
     * @param count The count to add to it.
     */
    public void updateCount(AbsoluteCountMetric metric, long count) {
        if (enabled) {
            metric.add(count);
        }
    }

    /**
     * Sets the given count for the given metric.
     *
     * @param metric The {@link AbsoluteCountMetric} to set the count.
     * @param count The count to set for it.
     */
    public void setCount(AbsoluteCountMetric metric, long count) {
        if (enabled) {
            metric.set(count);
        }
    }

    /**
     * Adds the given count to the given metric.
     *
     * @param metric The {@link CountMetric} to add the count.
     * @param count The count to add to it.
     */
    public void updateCount(CountMetric metric, long count) {
        if (enabled) {
            metric.incrBy(count);
        }
    }

    /**
     * Adds the given count to the given metric.
     *
     * @param metric The {@link MapCountMetric} to add the count.
     * @param key The key to add the count.
     * @param count The count to add.
     */
    public void updateCount(MapCountMetric metric, String key, long count) {
        if (enabled) {
            metric.add(key, count);
        }
    }

    /**
     * Sets the given count for the given metric and key.
     *
     * @param metric The {@link MapCountMetric} to set the count.
     * @param key The key to set the count.
     * @param count The count to set.
     */
    public void setCount(MapCountMetric metric, String key, long count) {
        if (enabled) {
            metric.set(key, count);
        }
    }

    /**
     * Clears the counts of the given metric.
     *
     * @param metric The {@link MapCountMetric} to clear.
     */
    public void clearCount(MapCountMetric metric) {
        if (enabled) {
            metric.clear();
        }
    }

    private <T extends IMetric> T registerMetric(T metric, String name, TopologyContext context) {
        Number interval = metricsIntervalMapping.getOrDefault(name, metricsIntervalMapping.get(DEFAULT_BUILT_IN_METRICS_INTERVAL_KEY));
        log.info("Registered metric: {} with interval {}", name, interval);
        return context.registerMetric(name, metric, interval.intValue());
    }
}
