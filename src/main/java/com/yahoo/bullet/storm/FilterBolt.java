/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.PubSubMessageSerDe;
import com.yahoo.bullet.querying.Querier;
import com.yahoo.bullet.querying.QueryCategorizer;
import com.yahoo.bullet.querying.QueryManager;
import com.yahoo.bullet.record.BulletRecord;
import lombok.AccessLevel;
import lombok.Getter;
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
import static com.yahoo.bullet.storm.TopologyConstants.FEEDBACK_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.ID_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.LATENCY_METRIC;
import static com.yahoo.bullet.storm.TopologyConstants.METADATA_FIELD;

@Slf4j
public class FilterBolt extends QueryBolt {
    private static final long serialVersionUID = -4357269268404488793L;

    private String recordComponent;

    // Exposed for testing
    @Getter(AccessLevel.PACKAGE)
    private transient QueryManager manager;
    private transient PubSubMessageSerDe querySerDe;
    private transient ReducedMetric averageLatency;
    private transient int statsTickInterval;
    // Exposed for testing
    @Getter(AccessLevel.PACKAGE)
    private transient int statsTickCount;
    private transient int duplicatedCount;

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
        // Set up the stats report intervals
        statsTickInterval = config.getAs(BulletStormConfig.FILTER_BOLT_STATS_REPORT_TICKS, Integer.class);
        statsTickCount = 0;
        // Set up the manager
        manager = new QueryManager(config);
        if (metrics.isEnabled()) {
            averageLatency = metrics.registerAveragingMetric(LATENCY_METRIC, context);
        }
        querySerDe = PubSubMessageSerDe.from(config);
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
            case BATCH_TUPLE:
                onBatch(tuple);
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
        // This is the per query data stream.
        declarer.declareStream(DATA_STREAM, new Fields(ID_FIELD, DATA_FIELD));
        // This is the where any errors per query are sent
        declarer.declareStream(ERROR_STREAM, new Fields(ID_FIELD, ERROR_FIELD));
        // This is where replay requests are sent
        declarer.declareStream(FEEDBACK_STREAM, new Fields(ID_FIELD, METADATA_FIELD));
    }

    @Override
    protected void initializeQuery(PubSubMessage message) {
        String id = message.getId();
        if (manager.hasQuery(id)) {
            log.debug("Duplicate for request {}", id);
            duplicatedCount++;
            return;
        }
        try {
            message = querySerDe.fromMessage(message);
            Querier querier = createQuerier(Querier.Mode.PARTITION, id, message.getContentAsQuery(), message.getMetadata(), config);
            manager.addQuery(id, querier);
            log.info("Initialized query {} : {}", querier.getRunningQuery().getId(), querier.getRunningQuery().getQueryString());
            log.debug("Initialized query {}", querier);
            return;
        } catch (RuntimeException ignored) {
        }
        // No need to handle any errors in the Filter Bolt.
        log.error("Failed to initialize query for request {}", id);
    }

    @Override
    protected void removeQuery(String id) {
        super.removeQuery(id);
        manager.removeAndGetQuery(id);
    }

    private void onQuery(Tuple tuple) {
        PubSubMessage message = (PubSubMessage) tuple.getValue(TopologyConstants.QUERY_POSITION);
        initializeQuery(message);
    }

    private void onRecord(Tuple tuple) {
        BulletRecord record = (BulletRecord) tuple.getValue(TopologyConstants.RECORD_POSITION);
        handleCategorizedQueries(manager.categorize(record));
    }

    private void onTick() {
        // Categorize queries in partition mode.
        handleCategorizedQueries(manager.categorize());
        handleStats();
        emitReplayRequestIfNecessary();
    }

    private void handleCategorizedQueries(QueryCategorizer category) {
        Map<String, Querier> done = category.getDone();
        done.entrySet().forEach(this::emitData);
        manager.removeQueries(done.keySet());

        Map<String, Querier> rateLimited = category.getRateLimited();
        rateLimited.entrySet().forEach(this::emitError);
        manager.removeQueries(rateLimited.keySet());

        Map<String, Querier> closed = category.getClosed();
        closed.entrySet().forEach(this::emitData);
        closed.values().forEach(Querier::reset);

        log.debug("Done: {}, Rate limited: {}, Closed: {}, Active: {}",
                  done.size(), rateLimited.size(), closed.size(), manager.size());
    }

    private void handleStats() {
        statsTickCount++;
        if (statsTickCount < statsTickInterval) {
            return;
        }
        statsTickCount = 0;
        log.info("Query Manager Statistics:\n{}\n", manager.getStats());
        log.info("Duplicated queries count: {}", duplicatedCount);
    }

    private void emitData(Map.Entry<String, Querier> query) {
        emit(DATA_STREAM, query.getKey(), query.getValue().getData());
    }

    private void emitError(Map.Entry<String, Querier> query) {
        emit(ERROR_STREAM, query.getKey(), query.getValue().getRateLimitError());
    }

    private void emit(String stream, String id, Object value) {
        if (value != null) {
            collector.emit(stream, new Values(id, value));
        }
    }

    private void updateLatency(Tuple tuple) {
        if (metrics.isEnabled() && tuple.size() > 1) {
            // Could use named fields instead
            Long timestamp = (Long) tuple.getValue(TopologyConstants.RECORD_TIMESTAMP_POSITION);
            averageLatency.update(System.currentTimeMillis() - timestamp);
        }
    }
}
