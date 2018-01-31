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
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

@Slf4j
public class ResultBolt extends ConfigComponent implements IRichBolt {
    private static final long serialVersionUID = -1927930701345251113L;

    private transient OutputCollector collector;

    /** Exposed for testing only. */
    @Getter(AccessLevel.PACKAGE)
    private transient Publisher publisher;

    /**
     * Creates a ResultBolt with a non-null {@link BulletStormConfig}.
     *
     * @param config The non-null BulletStormConfig to use. It should contain the settings to initialize a PubSub.
     */
    public ResultBolt(BulletStormConfig config) {
        super(config);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        // Add the Storm Config and the context as is, in case any PubSubs need it.
        config.set(BulletStormConfig.STORM_CONFIG, conf);
        config.set(BulletStormConfig.STORM_CONTEXT, context);

        this.collector = collector;
        try {
            PubSub pubSub = PubSub.from(config);
            publisher = pubSub.getPublisher();
            log.info("Setup PubSub: {} with Publisher: {}", pubSub, publisher);
        } catch (PubSubException e) {
            throw new RuntimeException("Cannot create PubSub instance or a Publisher for it.", e);
        }
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
