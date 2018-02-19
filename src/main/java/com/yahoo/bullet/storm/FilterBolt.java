/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.querying.Querier;
import com.yahoo.bullet.querying.RateLimitError;
import com.yahoo.bullet.record.BulletRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static com.yahoo.bullet.storm.TopologyConstants.DATA_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.DATA_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.ERROR_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.ERROR_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.ID_FIELD;

@Slf4j
public class FilterBolt extends QueryBolt {
    private static final long serialVersionUID = -4357269268404488793L;

    private String recordComponent;
    private transient ReducedMetric averageLatency;
    private transient AbsoluteCountMetric rateExceededQueries;

    /**
     * Constructor that accepts the name of the component that the records are coming from and the validated config.
     *
     * @param recordComponent The source component name for records.
     * @param config The validated {@link BulletStormConfig} to use.
     */
    public FilterBolt(String recordComponent, BulletStormConfig config) {
        super(config);
        this.recordComponent = recordComponent;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        // Set the record component into the classifier
        classifier.setRecordComponent(recordComponent);
        if (metricsEnabled) {
            averageLatency = registerAveragingMetric(TopologyConstants.LATENCY_METRIC, context);
            rateExceededQueries = registerAbsoluteCountMetric(TopologyConstants.RATE_EXCEEDED_QUERIES_METRIC, context);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        // Check if the tuple is any known type, otherwise make it unknown
        TupleClassifier.Type type = classifier.classify(tuple).orElse(TupleClassifier.Type.UNKNOWN_TUPLE);
        switch (type) {
            case TICK_TUPLE:
                onTick();
                break;
            case METADATA_TUPLE:
                onMeta(tuple);
                break;
            case QUERY_TUPLE:
                onQuery(tuple);
                break;
            case RECORD_TUPLE:
                onRecord(tuple);
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
        // This is the per query data stream
        declarer.declareStream(DATA_STREAM, new Fields(ID_FIELD, DATA_FIELD));
        // This is the where any errors per query are sent
        declarer.declareStream(ERROR_STREAM, new Fields(ID_FIELD, ERROR_FIELD));
    }

    private void onQuery(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        String query = tuple.getString(TopologyConstants.QUERY_POSITION);

        // No need to handle any errors in the Filter Bolt.
        Querier querier = null;
        try {
            querier = createQuerier(id, query, config);
            if (querier.initialize().isPresent()) {
                querier = null;
            }
        } catch (RuntimeException ignored) {
        }
        if (querier == null) {
            log.error("Failed to initialize query for request {} with query {}", id, query);
        } else {
            log.info("Initialized query {}: {}", id, query);
            queries.put(id, querier);
        }
    }

    private void onRecord(Tuple tuple) {
        BulletRecord record = (BulletRecord) tuple.getValue(TopologyConstants.RECORD_POSITION);
        handleCategorizedQueries(new QueryCategorizer().categorize(record, queries));
    }

    private void onTick() {
        handleCategorizedQueries(new QueryCategorizer().categorize(queries, true));
    }

    private void handleCategorizedQueries(QueryCategorizer category) {
        Map<String, Querier> done = category.getDone();
        done.entrySet().forEach(this::emitData);
        queries.keySet().removeAll(done.keySet());

        Map<String, Querier> rateLimited = category.getRateLimited();
        rateLimited.entrySet().forEach(this::emitError);
        queries.keySet().removeAll(rateLimited.keySet());

        Map<String, Querier> closed = category.getClosed();
        closed.entrySet().forEach(this::emitData);
        closed.values().forEach(Querier::reset);

        log.info("Done: {}, Rate limited: {}, Closed: {}, Active: {}",
                 done.size(), rateLimited.size(), closed.size(), queries.size());
    }

    private void emitData(Map.Entry<String, Querier> query) {
        byte[] data = query.getValue().getData();
        if (data != null) {
            collector.emit(DATA_STREAM, new Values(query.getKey(), data));
        }
    }

    private void emitError(Map.Entry<String, Querier> query) {
        RateLimitError error = query.getValue().getRateLimitError();
        if (error != null) {
            collector.emit(ERROR_STREAM, new Values(query.getKey(), error));
            updateCount(rateExceededQueries, 1L);
        }
    }

    private void updateLatency(Tuple tuple) {
        if (metricsEnabled && tuple.size() > 1) {
            // Could use named fields instead
            Long timestamp = (Long) tuple.getValue(TopologyConstants.RECORD_TIMESTAMP_POSITION);
            averageLatency.update(System.currentTimeMillis() - timestamp);
        }
    }
}
