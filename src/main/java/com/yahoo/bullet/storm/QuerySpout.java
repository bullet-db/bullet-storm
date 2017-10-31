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
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

@Slf4j
public class QuerySpout extends BaseRichSpout {
    public static final String QUERY_STREAM = Utils.DEFAULT_STREAM_ID;
    public static final String ID_FIELD = "id";
    public static final String QUERY_FIELD = "query";
    public static final String METADATA_FIELD = "metadata";

    private BulletStormConfig config;
    private Subscriber subscriber;
    private SpoutOutputCollector collector;

    /** Exposed for testing only. */
    @Getter(AccessLevel.PACKAGE)
    private PubSub pubSub;

    /**
     * Creates a QuerySpout with a passed in {@link BulletStormConfig}.
     *
     * @param config The BulletStormConfig to create the PubSub from.
     */
    public QuerySpout(BulletStormConfig config) {
        this.config = config;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // Add the Storm Config and the context as is, in case any PubSubs need it.
        config.set(BulletStormConfig.STORM_CONFIG, conf);
        config.set(BulletStormConfig.STORM_CONTEXT, context);

        this.collector = collector;
        try {
            pubSub = PubSub.from(config);
            subscriber = pubSub.getSubscriber();
            log.info("Setup PubSub: {} with Subscriber: {}", pubSub, subscriber);
        } catch (PubSubException e) {
            throw new RuntimeException("Cannot create PubSub instance or a Subscriber for it.", e);
        }
    }

    @Override
    public void nextTuple() {
        PubSubMessage message = null;
        try {
            message = subscriber.receive();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        if (message != null) {
            collector.emit(QUERY_STREAM, new Values(message.getId(), message.getContent(), message.getMetadata()), message.getId());
        } else {
            Utils.sleep(1);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(QUERY_STREAM, new Fields(ID_FIELD, QUERY_FIELD, METADATA_FIELD));
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
        subscriber.close();
    }
}
