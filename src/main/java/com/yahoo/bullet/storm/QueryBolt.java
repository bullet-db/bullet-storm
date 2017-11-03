/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.querying.AbstractQuery;
import com.yahoo.bullet.result.Metadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public abstract class QueryBolt<Q extends AbstractQuery> implements IRichBolt {
    public static final Integer DEFAULT_TICK_INTERVAL = 5;

    public static final boolean DEFAULT_BUILT_IN_METRICS_ENABLE = false;
    public static final String DEFAULT_BUILT_IN_METRICS_INTERVAL_KEY = "default";
    public static final int DEFAULT_BUILT_IN_METRICS_INTERVAL_SECS = 60;

    protected boolean metricsEnabled;
    protected Map<String, Number> metricsIntervalMapping;
    protected int tickInterval;
    protected Map configuration;
    protected OutputCollector collector;
    protected Map<String, String> metadataKeys;

    // TODO consider a rotating map with multilevels and reinserts upon rotating instead for scalability
    protected Map<String, Q> queriesMap;

    /**
     * Constructor that accepts the tick interval.
     *
     * @param tickInterval The tick interval in seconds.
     */
    public QueryBolt(Integer tickInterval) {
        this.tickInterval = tickInterval == null ? DEFAULT_TICK_INTERVAL : tickInterval;
    }

    /**
     * Default constructor.
     */
    public QueryBolt() {
        this(DEFAULT_TICK_INTERVAL);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // stormConf is not modifiable. Need to make a copy.
        this.configuration = new HashMap<>(stormConf);
        this.collector = collector;
        queriesMap = new HashMap<>();

        // Get all known Concepts
        metadataKeys = Metadata.getConceptNames(configuration, new HashSet<>(Metadata.KNOWN_CONCEPTS));
        if (!metadataKeys.isEmpty()) {
            log.info("Metadata collection is enabled");
            log.info("Collecting metadata for these concepts:\n{}", metadataKeys);
            // Add all metadataKeys back to configuration for reuse so no need refetch them on every new query
            configuration.put(BulletStormConfig.RESULT_METADATA_METRICS_MAPPING, metadataKeys);
        }

        // Enable built in metrics
        metricsEnabled = (Boolean) configuration.getOrDefault(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE,
                                                              DEFAULT_BUILT_IN_METRICS_ENABLE);
        metricsIntervalMapping = (Map<String, Number>) configuration.getOrDefault(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING,
                                                                                  new HashMap<>());
        metricsIntervalMapping.putIfAbsent(DEFAULT_BUILT_IN_METRICS_INTERVAL_KEY, DEFAULT_BUILT_IN_METRICS_INTERVAL_SECS);
    }

    /**
     * Retires queries that are active past the tick time.
     *
     * @return The map of query ids to queries that were retired.
     */
    protected Map<String, Q> retireQueries() {
        Map<String, Q> retiredQueries = queriesMap.entrySet().stream().filter(e -> e.getValue().isExpired())
                                                                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        queriesMap.keySet().removeAll(retiredQueries.keySet());
        if (retiredQueries.size() > 0) {
            log.info("Retired queries: {}. Active queries: {}.", retiredQueries.size(), queriesMap.size());
        }
        return retiredQueries;
    }

    /**
     * Initializes a query from a query tuple.
     *
     * @param tuple The query tuple with the query to initialize.
     * @return The created query.
     */
    protected Q initializeQuery(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        String queryString = tuple.getString(TopologyConstants.QUERY_POSITION);
        Q query = instantiateQuery(tuple);
        if (query == null) {
            log.error("Failed to initialize query for request {} with query {}", id, queryString);
            return null;
        }
        log.info("Initialized query {} : {}", id, query.toString());
        queriesMap.put(id, query);
        return query;
    }

    /**
     * Gets the default tick configuration to be used.
     *
     * @return A Map configuration containing the default tick configuration.
     */
    public Map<String, Object> getDefaultTickConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickInterval);
        return conf;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return getDefaultTickConfiguration();
    }

    @Override
    public void cleanup() {
    }

    /**
     * Creates and returns the right type of AbstractQuery to use for this Bolt. If query cannot be
     * created, handles the error and returns null.
     *
     * @param queryTuple The query tuple
     * @return The appropriate type of AbstractQuery to use for this Bolt.
     */
    protected abstract Q instantiateQuery(Tuple queryTuple);
}
