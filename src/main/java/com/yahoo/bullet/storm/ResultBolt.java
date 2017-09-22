/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.storm.drpc.BulletDRPCConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.Objects;

@Slf4j @Getter
public class ResultBolt extends BaseRichBolt {
    private PubSub pubSub;
    private Publisher publisher;
    private OutputCollector collector;
    private BulletConfig config;

    /**
     * Creates a ResultBolt and passes in a {@link BulletConfig}.
     *
     * @param config BulletConfig to create PubSub from.
     */
    public ResultBolt(BulletConfig config) {
        this.config = config;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        // Merge supplied configs with the cluster defaults.
        conf.forEach((key, value) -> config.set(key.toString(), value));
        config.set(BulletDRPCConfig.DRPC_INSTANCE_INDEX, Objects.isNull(context.getThisComponentId()) ? -1 : context.getThisComponentId());
        try {
            this.pubSub = PubSub.from(config);
        } catch (PubSubException e) {
            throw new RuntimeException("Cannot create PubSub.", e);
        }
        this.collector = collector;
        this.publisher = pubSub.getPublisher();
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
