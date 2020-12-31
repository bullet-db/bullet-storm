/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.storm.metric.AbsoluteCountMetric;
import com.yahoo.bullet.storm.metric.BulletMetrics;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

import static com.yahoo.bullet.storm.StormUtils.isReplaySignal;
import static com.yahoo.bullet.storm.TopologyConstants.ID_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.METADATA_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.METADATA_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.QUERY_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.QUERY_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_ACK_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_TIMESTAMP_FIELD;

@Slf4j
public class QuerySpout extends ConfigComponent implements IRichSpout {
    private static final long serialVersionUID = 504190523090872490L;

    // Exposed for testing only.
    @Getter(AccessLevel.PACKAGE)
    @AllArgsConstructor
    static class Replay {
        private String id;
        private long timestamp;
        private boolean stopped;
    }

    private transient BulletMetrics metrics;
    private transient AbsoluteCountMetric activeReplaysCount;

    /** Exposed for testing only. */
    @Getter(AccessLevel.PACKAGE)
    private transient Subscriber subscriber;
    private transient SpoutOutputCollector collector;

    /** Exposed for testing only. */
    @Getter(AccessLevel.PACKAGE)
    private transient Map<String, Replay> replays;

    /**
     * Creates a QuerySpout with a given {@link BulletStormConfig}.
     *
     * @param config The non-null BulletStormConfig to use. It should contain the settings to initialize a PubSub.
     */
    public QuerySpout(BulletStormConfig config) {
        super(config);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // Add the Storm Config and the context as is, in case any PubSubs need it.
        config.set(BulletStormConfig.STORM_CONFIG, conf);
        config.set(BulletStormConfig.STORM_CONTEXT, context);
        this.collector = collector;

        // Enable built-in metrics
        metrics = new BulletMetrics(config);
        if (metrics.isEnabled()) {
            activeReplaysCount = metrics.registerAbsoluteCountMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC, context);
        }
    }

    @Override
    public void activate() {
        try {
            PubSub pubSub = PubSub.from(config);
            subscriber = pubSub.getSubscriber();
            log.info("Setup PubSub: {} with Subscriber: {}", pubSub, subscriber);
        } catch (PubSubException e) {
            throw new RuntimeException("Cannot create PubSub instance or a Subscriber for it.", e);
        }
        replays = new HashMap<>();
        log.info("QuerySpout activated");
    }

    @Override
    public void deactivate() {
        try {
            subscriber.close();
        } catch (Exception e) {
            log.error("Could not close Subscriber.", e);
        }
        replays.clear();
        metrics.setCount(activeReplaysCount, 0L);
    }

    @Override
    public void nextTuple() {
        PubSubMessage message = null;
        try {
            message = subscriber.receive();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        if (message == null) {
            Utils.sleep(1);
            return;
        }

        String id = message.getId();
        Metadata metadata = message.getMetadata();

        if (metadata == null) {
            log.warn("Received message {} without metadata.", id);
            subscriber.commit(id);
            return;
        }

        if (isReplaySignal(metadata.getSignal())) {
            subscriber.commit(id);
            if (metadata.hasContent()) {
                handleReplayRequest(id, (Long) metadata.getContent());
            } else {
                // Note, this tuple is not anchored since reloading the queries in the replay bolt could cause the tuple to time out.
                collector.emit(METADATA_STREAM, new Values(id, metadata));
                log.info("Received forced replay signal.");
            }
        } else if (message.hasContent()) {
            collector.emit(QUERY_STREAM, new Values(id, message.getContent(), metadata), id);
        } else {
            collector.emit(METADATA_STREAM, new Values(id, metadata), id);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(QUERY_STREAM, new Fields(ID_FIELD, QUERY_FIELD, METADATA_FIELD));
        declarer.declareStream(METADATA_STREAM, new Fields(ID_FIELD, METADATA_FIELD));
        declarer.declareStream(REPLAY_STREAM, new Fields(ID_FIELD, REPLAY_TIMESTAMP_FIELD, REPLAY_ACK_FIELD));
    }

    @Override
    public void ack(Object id) {
        Replay replay = replays.get(id);
        if (replay != null) {
            log.info("Received replay loop ack for {}", id);
            emitReplay(replay, true);
            return;
        }
        subscriber.commit((String) id);
    }

    @Override
    public void fail(Object id) {
        Replay replay = replays.get(id);
        if (replay != null) {
            log.info("Received replay loop fail for {}", id);
            replay.stopped = true;
            metrics.updateCount(activeReplaysCount, -1L);
            return;
        }
        subscriber.fail((String) id);
    }

    @Override
    public void close() {
    }

    /*
    Possible scenarios when we receive a replay request
    1) No replay exists - start a new replay
    2) Request timestamp is outdated - ignore
    3) Request timestamp is the same and replay has not stopped - ignore
    4a) Request timestamp is the same and replay has stopped - replay failed somehow; emit to restart
    4b) Request timestamp is new - downstream bolt restarted; replace the timestamp and emit to restart only if replay has stopped
    */
    private void handleReplayRequest(String id, Long timestamp) {
        log.info("Received replay request with id {}", id);
        Replay replay = replays.get(id);
        if (replay == null) {
            log.info("Starting message loop for {}", id);
            replay = new Replay(id, timestamp, false);
            replays.put(id, replay);
            emitReplay(replay, false);
            metrics.updateCount(activeReplaysCount, 1L);
        } else if (timestamp < replay.timestamp) {
            log.info("Ignoring outdated replay request for {}.", id);
        } else if (timestamp == replay.timestamp && !replay.stopped) {
            log.info("Ignoring replay request for {} since replay loop is already in progress.", id);
        } else {
            log.info("Restarting replay loop for {} (bolt restarted: {}, replay stopped: {})", id, timestamp > replay.timestamp, replay.stopped);
            replay.timestamp = timestamp;
            if (replay.stopped) {
                replay.stopped = false;
                emitReplay(replay, false);
                metrics.updateCount(activeReplaysCount, 1L);
            }
        }
    }

    private void emitReplay(Replay replay, boolean acked) {
        collector.emit(REPLAY_STREAM, new Values(replay.id, replay.timestamp, acked), replay.id);
    }
}
