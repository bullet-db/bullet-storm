/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.tracing.AbstractRule;
import lombok.extern.slf4j.Slf4j;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public abstract class RuleBolt<R extends AbstractRule> implements IRichBolt {
    public static final Integer DEFAULT_TICK_INTERVAL = 5;

    public static final boolean DEFAULT_BUILT_IN_METRICS_ENABLE = false;
    public static final String DEFAULT_METRICS_INTERVAL_KEY = "default";
    public static final int DEFAULT_BUILT_IN_METRICS_INTERVAL_SECS = 60;

    protected boolean metricsEnabled;
    protected Map<String, Number> metricsIntervalMapping;
    protected int tickInterval;
    protected Map configuration;
    protected OutputCollector collector;
    protected Map<String, String> metadataKeys;

    // TODO consider a rotating map with multilevels and reinserts upon rotating instead for scalability
    protected Map<Long, R> rulesMap;

    /**
     * Constructor that accepts the tick interval.
     * @param tickInterval The tick interval in seconds.
     */
    public RuleBolt(Integer tickInterval) {
        this.tickInterval = tickInterval == null ? DEFAULT_TICK_INTERVAL : tickInterval;
    }

    /**
     * Default constructor.
     */
    public RuleBolt() {
        this(DEFAULT_TICK_INTERVAL);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // stormConf is not modifyable. Need to make a copy.
        this.configuration = new HashMap<>(stormConf);
        this.collector = collector;
        rulesMap = new HashMap<>();

        // Get all known Concepts
        metadataKeys = Metadata.getConceptNames(configuration, new HashSet<>(Metadata.KNOWN_CONCEPTS));
        if (!metadataKeys.isEmpty()) {
            log.info("Metadata collection is enabled");
            log.info("Collecting metadata for these concepts:\n{}", metadataKeys);
            // Add all metadataKeys back to configuration for reuse so no need refetch them on every new rule
            configuration.put(BulletConfig.RESULT_METADATA_METRICS_MAPPING, metadataKeys);
        }

        // Enable built in metrics
        metricsEnabled = (Boolean) configuration.getOrDefault(BulletConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE,
                                                              DEFAULT_BUILT_IN_METRICS_ENABLE);
        metricsIntervalMapping = (Map<String, Number>) configuration.getOrDefault(BulletConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING,
                                                                                  new HashMap<>());
        metricsIntervalMapping.putIfAbsent(DEFAULT_METRICS_INTERVAL_KEY, DEFAULT_BUILT_IN_METRICS_INTERVAL_SECS);
    }

    /**
     * Retires DRPC rules that are active past the tick time.
     * @return The map of DRPC request ids to Rules that were retired.
     */
    protected Map<Long, R> retireRules() {
        Map<Long, R> retiredRules = rulesMap.entrySet().stream().filter(e -> e.getValue().isExpired())
                                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        rulesMap.keySet().removeAll(retiredRules.keySet());
        if (retiredRules.size() > 0) {
            log.info("Retired {} rule(s). There are {} active rule(s).", retiredRules.size(), rulesMap.size());
        }
        return retiredRules;
    }

    /**
     * Initializes a rule from a rule tuple.
     * @param tuple The rule tuple with the rule to initialize.
     * @return The created rule.
     */
    protected R initializeRule(Tuple tuple) {
        Long id = tuple.getLong(TopologyConstants.ID_POSITION);
        String ruleString = tuple.getString(TopologyConstants.RULE_POSITION);
        R rule = getRule(id, ruleString);
        if (rule == null) {
            log.error("Failed to initialize rule for request {} with rule {}", id, ruleString);
            return null;
        }
        log.info("Initialized rule {} : {}", id, rule.toString());
        rulesMap.put(id, rule);
        return rule;
    }

    /**
     * Gets the default tick configuration to be used.
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
     * Finds the right type of AbstractRule to use for this Bolt. If rule cannot be
     * created, handles the error and returns null.
     *
     * @param id The DRPC request id.
     * @param ruleString The String version of the AbstractRule
     * @return The appropriate type of AbstractRule to use for this Bolt.
     */
    protected abstract R getRule(Long id, String ruleString);
}
