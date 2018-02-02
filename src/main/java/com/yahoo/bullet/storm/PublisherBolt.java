/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

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
public abstract class PublisherBolt extends ConfigComponent implements IRichBolt {
    private static final long serialVersionUID = -3198734196446239476L;

    private transient OutputCollector collector;

    // Exposed for testing only
    @Getter(AccessLevel.PACKAGE)
    protected transient Publisher publisher;

    /**
     * Creates an instance of this class with the given non-null config.
     *
     * @param config The non-null {@link BulletStormConfig} which is the config for this component.
     */
    public PublisherBolt(BulletStormConfig config) {
        super(config);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        // Add the Storm Config and the context as is, in case any PubSubs need it.
        config.set(BulletStormConfig.STORM_CONFIG, conf);
        config.set(BulletStormConfig.STORM_CONTEXT, context);

        this.collector = collector;
        try {
            this.publisher = createPublisher();
        } catch (PubSubException e) {
            throw new RuntimeException("Cannot create PubSub instance or a Publisher for it.", e);
        }
    }

    /**
     * Creates an instance of {@link Publisher} for use.
     *
     * @return A created publisher instance.
     * @throws PubSubException if there were issues creating one.
     */
    protected abstract Publisher createPublisher() throws PubSubException;

    /**
     * Publishes a message through the {@link Publisher} instance and acks the tuple even if the publishing failed.
     *
     * @param message The {@link PubSubMessage} to publish.
     * @param tuple The {@link Tuple} to ack.
     */
    protected void publish(PubSubMessage message, Tuple tuple) {
        try {
            publisher.send(message);
        } catch (PubSubException e) {
            log.error(e.getMessage());
        }
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        publisher.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
