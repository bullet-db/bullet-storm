/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.parsing.ParsingException;
import com.yahoo.bullet.querying.FilterQuery;
import com.yahoo.bullet.record.BulletRecord;
import lombok.extern.slf4j.Slf4j;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

@Slf4j
public class FilterBolt extends QueryBolt<FilterQuery> {
    public static final String FILTER_STREAM = Utils.DEFAULT_STREAM_ID;
    public static final String LATENCY_METRIC = TopologyConstants.METRIC_PREFIX + "filter_latency";

    private String recordComponent;
    private transient ReducedMetric averageLatency;

    /**
     * Default constructor.
     */
    public FilterBolt() {
        this(TopologyConstants.RECORD_COMPONENT, QueryBolt.DEFAULT_TICK_INTERVAL);
    }

    /**
     * Constructor that accepts the name of the component that the records are coming from.
     * @param recordComponent The source component name for records.
     */
    public FilterBolt(String recordComponent) {
        this(recordComponent, QueryBolt.DEFAULT_TICK_INTERVAL);
    }

    /**
     * Constructor that accepts the name of the component that the records are coming from and the tick interval.
     * @param recordComponent The source component name for records.
     * @param tickInterval The tick interval in seconds.
     */
    public FilterBolt(String recordComponent, Integer tickInterval) {
        super(tickInterval);
        this.recordComponent = recordComponent;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        if (metricsEnabled) {
            averageLatency = registerAveragingMetric(LATENCY_METRIC, context);
        }
    }

    private TupleType.Type getCustomType(Tuple tuple) {
        return recordComponent.equals(tuple.getSourceComponent()) ? TupleType.Type.RECORD_TUPLE : null;
    }

    @Override
    public void execute(Tuple tuple) {
        // If it isn't any of our default TupleTypes, check if the component is from our custom source
        TupleType.Type type = TupleType.classify(tuple).orElse(getCustomType(tuple));
        switch (type) {
            case TICK_TUPLE:
                emitForQueries(retireQueries());
                break;
            case QUERY_TUPLE:
                initializeQuery(tuple);
                break;
            case RECORD_TUPLE:
                checkQuery(tuple);
                updateLatency(tuple);
                break;
            default:
                // May want to throw an error here instead of not acking
                log.error("Unknown tuple encountered: {}", type);
                return;
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TopologyConstants.ID_FIELD, TopologyConstants.RECORD_FIELD));
    }

    @Override
    protected FilterQuery getQuery(String id, String queryString) {
        // No need to handle any errors here. The JoinBolt reports all errors.
        try {
            return new FilterQuery(queryString, configuration);
        } catch (ParsingException | RuntimeException e) {
            return null;
        }
    }

    private void checkQuery(Tuple tuple) {
        BulletRecord record = (BulletRecord) tuple.getValue(0);
        // For each query that is satisfied, we will emit the data but we will not expire the query.
        queriesMap.entrySet().stream().filter(e -> e.getValue().consume(record)).forEach(this::emitForQuery);
    }

    private void emitForQueries(Map<String, FilterQuery> entries) {
        entries.entrySet().stream().forEach(this::emitForQuery);
    }

    private void emitForQuery(Map.Entry<String, FilterQuery> pair) {
        // The FilterQuery will handle giving us the right data - a byte[] to emit
        byte[] data = pair.getValue().getData();
        if (data != null) {
            collector.emit(new Values(pair.getKey(), data));
        }
    }

    private void updateLatency(Tuple tuple) {
        if (metricsEnabled && tuple.size() > 1) {
            // Could use named fields instead
            Long timestamp = (Long) tuple.getValue(1);
            averageLatency.update(System.currentTimeMillis() - timestamp);
        }
    }

    private ReducedMetric registerAveragingMetric(String name, TopologyContext context) {
        Number interval = metricsIntervalMapping.getOrDefault(name,
                                                              metricsIntervalMapping.get(DEFAULT_BUILT_IN_METRICS_INTERVAL_KEY));
        log.info("Registered {} with interval {}", name, interval);
        return context.registerMetric(name, new ReducedMetric(new MeanReducer()), interval.intValue());
    }
}
