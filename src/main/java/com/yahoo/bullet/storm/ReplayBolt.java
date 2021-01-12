/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.storage.StorageManager;
import com.yahoo.bullet.storm.batching.BatchManager;
import com.yahoo.bullet.storm.grouping.TaskIndexCaptureGrouping;
import com.yahoo.bullet.storm.metric.AbsoluteCountMetric;
import com.yahoo.bullet.storm.metric.BulletMetrics;
import com.yahoo.bullet.storm.metric.MapCountMetric;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.yahoo.bullet.storm.BulletStormConfig.DEFAULT_REPLAY_BATCH_COMPRESS_ENABLE;
import static com.yahoo.bullet.storm.BulletStormConfig.DEFAULT_REPLAY_BATCH_SIZE;
import static com.yahoo.bullet.storm.BulletStormConfig.REPLAY_BATCH_COMPRESS_ENABLE;
import static com.yahoo.bullet.storm.BulletStormConfig.REPLAY_BATCH_SIZE;
import static com.yahoo.bullet.storm.StormUtils.HYPHEN;
import static com.yahoo.bullet.storm.StormUtils.isKillSignal;
import static com.yahoo.bullet.storm.StormUtils.isReplaySignal;
import static com.yahoo.bullet.storm.TopologyConstants.CAPTURE_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.ID_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.ID_POSITION;
import static com.yahoo.bullet.storm.TopologyConstants.QUERY_POSITION;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_BATCH_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_INDEX_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_TIMESTAMP_FIELD;

@Slf4j
@Getter(AccessLevel.PACKAGE)
public class ReplayBolt extends ConfigComponent implements IRichBolt {
    private static final long serialVersionUID = 7678526821834215930L;

    enum ReplayState {
        START_REPLAY,
        PAST_REPLAY,
        FINISHED_REPLAY,
        ACK,
        NON_ACK
    }

    @Getter(AccessLevel.PACKAGE)
    static class Replay {
        private final int taskID;
        private final long timestamp;
        private List batches;
        private Set<Integer> anchors = new HashSet<>();
        private int index;

        Replay(int taskID, long timestamp, List batches) {
            this.taskID = taskID;
            this.timestamp = timestamp;
            this.batches = batches;
        }
    }

    private transient BulletMetrics metrics;
    private transient AbsoluteCountMetric batchedQueriesCount;
    private transient MapCountMetric activeReplaysCount;
    private transient MapCountMetric createdReplaysCount;
    private transient OutputCollector collector;
    private transient TupleClassifier classifier;

    private transient StorageManager<PubSubMessage> storageManager;
    private transient BatchManager<PubSubMessage> batchManager;
    private transient Map<String, Replay> replays;
    private transient boolean batchCompressEnable;

    /**
     * Creates a ReplayBolt with a given {@link BulletStormConfig}.
     *
     * @param config The non-null BulletStormConfig to use.
     */
    public ReplayBolt(BulletStormConfig config) {
        super(config);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        collector = outputCollector;
        classifier = new TupleClassifier();

        // Enable built-in metrics
        metrics = new BulletMetrics(config);
        if (metrics.isEnabled()) {
            batchedQueriesCount = metrics.registerAbsoluteCountMetric(TopologyConstants.BATCHED_QUERIES_METRIC, context);
            activeReplaysCount = metrics.registerMapCountMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC, context);
            createdReplaysCount = metrics.registerMapCountMetric(TopologyConstants.CREATED_REPLAYS_METRIC, context);
        }

        // Initialize batch manager
        int batchSize = config.getOrDefaultAs(REPLAY_BATCH_SIZE, DEFAULT_REPLAY_BATCH_SIZE, Integer.class);
        Number partitionCount = config.getRequiredConfigAs(BulletStormConfig.JOIN_BOLT_PARALLELISM, Number.class);
        batchCompressEnable = config.getOrDefaultAs(REPLAY_BATCH_COMPRESS_ENABLE, DEFAULT_REPLAY_BATCH_COMPRESS_ENABLE, Boolean.class);

        batchManager = new BatchManager<>(batchSize, partitionCount.intValue(), batchCompressEnable);
        replays = new HashMap<>();

        // Initialize storage manager
        try {
            storageManager = StorageManager.from(config);
        } catch (Exception e) {
            throw new RuntimeException("Could not create StorageManager.", e);
        }

        // Populate queries
        long before = System.currentTimeMillis();
        Map<String, PubSubMessage> queries = getStoredQueries();
        log.info("Took {} ms to get {} queries.", System.currentTimeMillis() - before, queries.size());

        before = System.currentTimeMillis();
        batchManager.addAll(queries);
        log.info("Took {} ms to batch {} queries.", System.currentTimeMillis() - before, batchManager.size());

        metrics.setCount(batchedQueriesCount, batchManager.size());
    }

    private Map<String, PubSubMessage> getStoredQueries() {
        try {
            return storageManager.getAll().get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get queries from storage.", e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        // Check if the tuple is any known type, otherwise make it unknown
        TupleClassifier.Type type = classifier.classify(tuple).orElse(TupleClassifier.Type.UNKNOWN_TUPLE);
        switch (type) {
            case QUERY_TUPLE:
                onQuery(tuple);
                break;
            case METADATA_TUPLE:
                onMeta(tuple);
                break;
            case REPLAY_TUPLE:
                onReplay(tuple);
                return;
            default:
                // May want to throw an error here instead of not acking
                log.error("Unknown tuple encountered: {} from {}-{}", type, tuple.getSourceComponent(), tuple.getSourceStreamId());
                return;
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(REPLAY_STREAM, new Fields(ID_FIELD, REPLAY_TIMESTAMP_FIELD, REPLAY_INDEX_FIELD, REPLAY_BATCH_FIELD));
        declarer.declareStream(CAPTURE_STREAM, new Fields());
    }

    @Override
    public void cleanup() {
    }

    private void onQuery(Tuple tuple) {
        String id = tuple.getString(ID_POSITION);
        byte[] queryData = (byte[]) tuple.getValue(QUERY_POSITION);
        // Batch manager does not add duplicate keys
        batchManager.add(id, new PubSubMessage(id, queryData));
        metrics.setCount(batchedQueriesCount, batchManager.size());
    }

    private void onMeta(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        Metadata metadata = (Metadata) tuple.getValue(TopologyConstants.METADATA_POSITION);
        if (metadata == null) {
            return;
        }
        Metadata.Signal signal = metadata.getSignal();
        if (isKillSignal(signal)) {
            batchManager.remove(id);
            metrics.setCount(batchedQueriesCount, batchManager.size());
            log.info("Received {} signal and killed query: {}", signal, id);
        } else if (isReplaySignal(signal)) {
            handleReplaySignal();
        }
    }

    private void handleReplaySignal() {
        log.info("Received {} signal.", Metadata.Signal.REPLAY);
        long before = System.currentTimeMillis();
        metrics.clearCount(activeReplaysCount);
        replays.clear();
        batchManager.clear();
        batchManager.addAll(getStoredQueries());
        metrics.setCount(batchedQueriesCount, batchManager.size());
        log.info("Took {} ms to get and batch {} queries.", System.currentTimeMillis() - before, batchManager.size());
    }

    /**
     * Possible scenarios when we receive a replay tuple:
     * 1) No replay exists - start a new replay state
     * 2) Timestamp is new - replace the old replay state with a new one
     * 3) Timestamp is old - ignore
     * 4) Timestamp matches but replay is done - ignore
     * 5) Timestamp matches and replay is in progress - proceed with replay
     *
     * A replay state for some downstream bolt tracks the batches to send using an index. When an ack is received, the
     * index is incremented and then the next batch is sent. When a non-ack is received, the current batch is (re-)sent.
     *
     * Note, the replay state also keeps separate indices for each spout that may be trying to replay to the downstream
     * bolt. This way, the acks and indices will not be mixed up.
     */
    private void onReplay(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        long timestamp = tuple.getLong(TopologyConstants.REPLAY_TIMESTAMP_POSITION);
        boolean acked = tuple.getBoolean(TopologyConstants.REPLAY_ACK_POSITION);
        Replay replay = replays.get(id);
        switch (classifyReplay(timestamp, acked, tuple, replay)) {
            case START_REPLAY:
                startReplay(id, timestamp, tuple);
                break;
            case PAST_REPLAY:
                log.warn("Received replay tuple for past replay.");
                collector.fail(tuple);
                break;
            case FINISHED_REPLAY:
                log.warn("Received replay tuple for finished replay.");
                collector.fail(tuple);
                break;
            case ACK:
                handleAck(id, tuple, replay);
                break;
            case NON_ACK:
                emitBatch(id, tuple, replay);
                break;
        }
    }

    private ReplayState classifyReplay(long timestamp, boolean acked, Tuple tuple, Replay replay) {
        if (replay == null || timestamp > replay.timestamp) {
            return ReplayState.START_REPLAY;
        } else if (timestamp < replay.timestamp) {
            return ReplayState.PAST_REPLAY;
        } else if (replay.batches == null) {
            return ReplayState.FINISHED_REPLAY;
        } else if (acked && replay.anchors.contains(tuple.getSourceTask())) {
            return ReplayState.ACK;
        } else {
            return ReplayState.NON_ACK;
        }
    }

    private void startReplay(String id, long timestamp, Tuple tuple) {
        log.info("Starting replay to {}", id);
        Replay replay;
        int taskID = Integer.valueOf(id.split(HYPHEN)[1]);
        Integer partitionIndex = TaskIndexCaptureGrouping.TASK_INDEX_MAP.get(taskID);
        if (partitionIndex != null) {
            replay = new Replay(taskID, timestamp, batchCompressEnable ? batchManager.getCompressedBatchesForPartition(partitionIndex) :
                                                                         batchManager.getBatchesForPartition(partitionIndex));
        } else {
            replay = new Replay(taskID, timestamp, batchCompressEnable ? batchManager.getCompressedBatches() :
                                                                         batchManager.getBatches());
        }
        replays.put(id, replay);
        metrics.setCount(activeReplaysCount, id, 1L);
        metrics.updateCount(createdReplaysCount, id, 1L);
        emitBatch(id, tuple, replay);
    }

    private void handleAck(String id, Tuple tuple, Replay replay) {
        // End replay after receiving an ack for the last batch; leave replay in map so if replay tuples come in after replay completes, replay won't restart.
        if (replay.index >= replay.batches.size()) {
            log.info("Ending replay to {}", id);
            replay.batches = null;
            replay.anchors = null;
            metrics.setCount(activeReplaysCount, id, 0L);
            collector.fail(tuple);
            return;
        }
        replay.anchors.clear();
        replay.index++;
        emitBatch(id, tuple, replay);
    }

    private void emitBatch(String id, Tuple anchor, Replay replay) {
        int index = replay.index;
        int size = replay.batches.size();
        if (index < size) {
            log.info("Emitting replay batch with index {} to {}", index, id);
            collector.emitDirect(replay.taskID, REPLAY_STREAM, anchor, new Values(id, replay.timestamp, index, replay.batches.get(index)));
        } else {
            log.info("Emitting replay batch NULL (index {}) to {}", index, id);
            collector.emitDirect(replay.taskID, REPLAY_STREAM, anchor, new Values(id, replay.timestamp, index, null));
        }
        replay.anchors.add(anchor.getSourceTask());
        collector.ack(anchor);
    }
}
