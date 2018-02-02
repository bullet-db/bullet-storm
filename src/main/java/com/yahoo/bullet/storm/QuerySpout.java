/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Subscriber;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

import static com.yahoo.bullet.storm.TopologyConstants.ID_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.METADATA_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.METADATA_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.QUERY_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.QUERY_STREAM;

@Slf4j
public class QuerySpout extends ConfigComponent implements IRichSpout {
    private static final long serialVersionUID = 504190523090872490L;

    private transient Subscriber subscriber;
    private transient SpoutOutputCollector collector;

    /** Exposed for testing only. */
    @Getter(AccessLevel.PACKAGE)
    private transient PubSub pubSub;

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
    }

    @Override
    public void activate() {
        try {
            pubSub = PubSub.from(config);
            subscriber = pubSub.getSubscriber();
            log.info("Setup PubSub: {} with Subscriber: {}", pubSub, subscriber);
        } catch (PubSubException e) {
            throw new RuntimeException("Cannot create PubSub instance or a Subscriber for it.", e);
        }
    }

    @Override
    public void deactivate() {
        subscriber.close();
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
        String content = message.getContent();
        // If no content, it's a metadata only message. Send it on the METADATA_STREAM.
        if (content == null) {
            collector.emit(METADATA_STREAM, new Values(message.getId(), message.getMetadata()), message.getId());
        } else {
            collector.emit(QUERY_STREAM, new Values(message.getId(), message.getContent(), message.getMetadata()), message.getId());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(QUERY_STREAM, new Fields(ID_FIELD, QUERY_FIELD, METADATA_FIELD));
        declarer.declareStream(METADATA_STREAM, new Fields(ID_FIELD, METADATA_FIELD));
    }

    @Override
    public void ack(Object id) {
        subscriber.commit((String) id);
    }

    @Override
    public void fail(Object id) {
        subscriber.fail((String) id);
    }

    @Override
    public void close() {
    }
}
