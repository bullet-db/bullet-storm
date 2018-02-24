/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.querying.Querier;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import static com.yahoo.bullet.storm.BulletStormConfig.DEFAULT_BUILT_IN_METRICS_INTERVAL_KEY;

@Slf4j
public abstract class QueryBolt extends ConfigComponent implements IRichBolt {
    private static final long serialVersionUID = 4567140628827887965L;

    protected transient boolean metricsEnabled;
    protected transient Map<String, Number> metricsIntervalMapping;
    protected transient OutputCollector collector;
    protected transient TupleClassifier classifier;
    protected transient Map<String, Querier> queries;

    /**
     * Creates a QueryBolt with a given {@link BulletStormConfig}.
     *
     * @param config The non-null BulletStormConfig to
     */
    public QueryBolt(BulletStormConfig config) {
        super(config);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        queries = new HashMap<>();
        classifier = new TupleClassifier();
        // Enable built in metrics
        metricsEnabled = config.getAs(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, Boolean.class);
        metricsIntervalMapping = config.getAs(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING, Map.class);
    }

    @Override
    public void cleanup() {
    }

    /**
     * Handles a metadata message for a query.
     *
     * @param tuple The metadata tuple.
     * @return The created {@link Metadata}.
     */
    protected Metadata onMeta(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        Metadata metadata = (Metadata) tuple.getValue(TopologyConstants.METADATA_POSITION);
        Metadata.Signal signal = metadata.getSignal();
        if (signal == Metadata.Signal.KILL || signal == Metadata.Signal.COMPLETE) {
            removeQuery(id);
            log.info("Received {} signal and killed query: {}", signal, id);
        }
        return metadata;
    }

    /**
     * For testing only. Create a {@link Querier} from the given query ID, body and configuration.
     *
     * @param id The ID for the query.
     * @param query The actual query JSON body.
     * @param config The configuration to use for the query.
     * @return A created, uninitialized instance of a querier or a RuntimeException if there were issues.
     */
    protected Querier createQuerier(String id, String query, BulletConfig config) {
        return new Querier(id, query, config);
    }

    /**
     * Setup the query with the given id and body. Override if you need to do additional setup.
     *
     * @param id The String ID of the query.
     * @param query The String body of the query.
     * @param metadata The {@link Metadata} for the query.
     * @param querier The valid, initialized {@link Querier} for this query.
     */
    protected void setupQuery(String id, String query, Metadata metadata, Querier querier) {
        queries.put(id, querier);
        log.info("Initialized query {}", querier.toString());
    }

    /**
     * Remove the query with this given id. Override this if you need to do additional cleanup.
     *
     * @param id The String id of the query.
     */
    protected void removeQuery(String id) {
        queries.remove(id);
    }

    /**
     * Adds the given count to the given metric.
     *
     * @param metric The {@link AbsoluteCountMetric} to add the count.
     * @param count The count to add to it.
     */
    protected void updateCount(AbsoluteCountMetric metric, long count) {
        if (metricsEnabled) {
            metric.add(count);
        }
    }

    /**
     * Registers a metric that averages its values with the configured interval for it (if any).
     *
     * @param name The name of the metric to register.
     * @param context The {@link TopologyContext} to register the metric for.
     * @return The registered {@link ReducedMetric} that is averaging.
     */
    protected ReducedMetric registerAveragingMetric(String name, TopologyContext context) {
        return registerMetric(new ReducedMetric(new MeanReducer()), name, context);
    }

    /**
     * Registers a metric that counts values monotonically increasing with the configured interval for it (if any).
     *
     * @param name The name of the metric to register.
     * @param context The {@link TopologyContext} to register the metric for.
     * @return The registered {@link AbsoluteCountMetric} that is counting.
     */
    protected AbsoluteCountMetric registerAbsoluteCountMetric(String name, TopologyContext context) {
        return registerMetric(new AbsoluteCountMetric(), name, context);
    }

    private <T extends IMetric> T registerMetric(T metric, String name, TopologyContext context) {
        Number interval = metricsIntervalMapping.getOrDefault(name, metricsIntervalMapping.get(DEFAULT_BUILT_IN_METRICS_INTERVAL_KEY));
        log.info("Registered metric: {} with interval {}", name, interval);
        return context.registerMetric(name, metric, interval.intValue());
    }
}
