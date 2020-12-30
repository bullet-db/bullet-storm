/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.querying.Querier;
import com.yahoo.bullet.querying.RunningQuery;
import com.yahoo.bullet.storm.metric.BulletMetrics;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static com.yahoo.bullet.storm.BulletStormConfig.REPLAY_ENABLE;
import static com.yahoo.bullet.storm.BulletStormConfig.REPLAY_REQUEST_INTERVAL;
import static com.yahoo.bullet.storm.StormUtils.HYPHEN;
import static com.yahoo.bullet.storm.StormUtils.isKillSignal;
import static com.yahoo.bullet.storm.StormUtils.isReplaySignal;
import static com.yahoo.bullet.storm.TopologyConstants.FEEDBACK_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_BATCH_POSITION;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_INDEX_POSITION;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_TIMESTAMP_POSITION;

@Slf4j
public abstract class QueryBolt extends ConfigComponent implements IRichBolt {
    private static final long serialVersionUID = 4567140628827887965L;

    protected transient BulletMetrics metrics;
    protected transient OutputCollector collector;
    protected transient TupleClassifier classifier;
    protected transient String componentTaskID;
    protected transient long startTimestamp;
    protected transient boolean replayCompleted;
    protected transient boolean replayEnabled;
    protected transient long replayRequestInterval;
    protected transient long lastReplayRequest;
    protected transient int batchCount;
    protected transient int replayedQueriesCount;

    /**
     * Creates a QueryBolt with a given {@link BulletStormConfig}.
     *
     * @param config The non-null BulletStormConfig to use.
     */
    public QueryBolt(BulletStormConfig config) {
        super(config);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        classifier = new TupleClassifier();
        componentTaskID = context.getThisComponentId() + HYPHEN + context.getThisTaskId();
        // Enable built-in metrics
        metrics = new BulletMetrics(config);
        startTimestamp = System.currentTimeMillis();
        replayEnabled = config.getAs(REPLAY_ENABLE, Boolean.class);
        replayRequestInterval = config.getAs(REPLAY_REQUEST_INTERVAL, Number.class).longValue();
        if (replayEnabled) {
            emitReplayRequest();
        }
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
        if (metadata == null) {
            return null;
        }
        Metadata.Signal signal = metadata.getSignal();
        if (isKillSignal(signal)) {
            removeQuery(id);
            log.info("Received {} signal and killed query: {}", signal, id);
        } else if (isReplaySignal(signal)) {
            handleForcedReplay();
        }
        return metadata;
    }

    private void handleForcedReplay() {
        if (!replayEnabled) {
            log.warn("Received forced replay signal but replay is not enabled");
            return;
        }
        log.info("Received forced replay signal.");
        startTimestamp = System.currentTimeMillis();
        replayCompleted = false;
        batchCount = 0;
        replayedQueriesCount = 0;
        emitReplayRequest();
    }

    /**
     * Handles a batch message for query replay.
     *
     * @param tuple The batch tuple.
     */
    @SuppressWarnings("unchecked")
    protected void onBatch(Tuple tuple) {
        if (replayCompleted) {
            log.warn("Batch arrived after replay was completed. Ignoring...");
            return;
        }
        long timestamp = tuple.getLong(REPLAY_TIMESTAMP_POSITION);
        int index = tuple.getInteger(REPLAY_INDEX_POSITION);
        Map<String, PubSubMessage> batch = (Map<String, PubSubMessage>) tuple.getValue(REPLAY_BATCH_POSITION);
        if (timestamp != startTimestamp) {
            log.warn("Batch timestamp {} does not match bolt start timestamp {}. Ignoring...", timestamp, startTimestamp);
            return;
        }
        log.info("Received batch with index {}", index);
        if (batch == null) {
            log.info("Total batches: {}. Total queries replayed: {}", batchCount, replayedQueriesCount);
            replayCompleted = true;

            // Process delayed query kills here






            return;
        }
        for (Map.Entry<String, PubSubMessage> entry : batch.entrySet()) {
            PubSubMessage message = entry.getValue();
            if (message != null) {
                initializeQuery(message);
            }
        }
        batchCount++;
        replayedQueriesCount += batch.size();
        lastReplayRequest = System.currentTimeMillis();
        log.info("Initialized {} queries.", batch.size());
    }

    /**
     * Initialize the query contained in the given {@link PubSubMessage}.
     *
     * @param message The message that contains the query to initialize.
     */
    protected abstract void initializeQuery(PubSubMessage message);

    /**
     * Exposed for testing only. Create a {@link Querier} from the given query ID, body and configuration.
     *
     * @param mode The {@link Querier.Mode} to use to create the instance.
     * @param id The ID for the query.
     * @param query The actual query object.
     * @param metadata The metadata that came with the query object.
     * @param config The configuration to use for the query.
     * @return A created, uninitialized instance of a querier or a RuntimeException if there were issues.
     */
    protected Querier createQuerier(Querier.Mode mode, String id, Query query, Metadata metadata, BulletConfig config) {
        return new Querier(mode, new RunningQuery(id, query, metadata), config);
    }

    /**
     * Remove the query with this given id. Override this if you need to do additional cleanup.
     *
     * @param id The String id of the query.
     */
    protected abstract void removeQuery(String id);

    protected void emitReplayRequestIfNecessary() {
        if (replayEnabled && !replayCompleted && System.currentTimeMillis() >= lastReplayRequest + replayRequestInterval) {
            emitReplayRequest();
        }
    }

    private void emitReplayRequest() {
        log.info("Emitting replay request from {} with start time {}", componentTaskID, startTimestamp);
        collector.emit(FEEDBACK_STREAM, new Values(componentTaskID, new Metadata(Metadata.Signal.ACKNOWLEDGE, startTimestamp)));
        lastReplayRequest = System.currentTimeMillis();
    }
}
