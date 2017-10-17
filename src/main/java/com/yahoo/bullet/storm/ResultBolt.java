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
import com.yahoo.bullet.pubsub.Publisher;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

@Slf4j
public class ResultBolt extends BaseRichBolt {
    private OutputCollector collector;
    private BulletStormConfig config;

    /** Exposed for testing only. */
    @Getter(AccessLevel.PACKAGE)
    private Publisher publisher;

    /**
     * Creates a ResultBolt and passes in a {@link BulletStormConfig}.
     *
     * @param config The BulletStormConfig to create PubSub from.
     */
    public ResultBolt(BulletStormConfig config) {
        this.config = config;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        // Add the Storm Config and the context as is, in case any PubSubs need it.
        config.set(BulletStormConfig.STORM_CONFIG, conf);
        config.set(BulletStormConfig.STORM_CONTEXT, context);

        try {
            PubSub pubSub = PubSub.from(config);
            this.publisher = pubSub.getPublisher();
        } catch (PubSubException e) {
            throw new RuntimeException("Cannot create PubSub instance or a Publisher for it.", e);
        }
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        PubSubMessage message = new PubSubMessage(tuple.getString(0), tuple.getString(1), (Metadata) tuple.getValue(2));
        try {
            publisher.send(message);
        } catch (PubSubException e) {
            log.error(e.getMessage());
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void cleanup() {
        publisher.close();
    }
}
