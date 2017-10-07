/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.storm.drpc.utils.DRPCOutputCollector;
import com.yahoo.bullet.storm.drpc.utils.DRPCTuple;
import lombok.AccessLevel;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * This class wraps a {@link ReturnResults} bolt and uses it to send messages to Storm DRPC. It needs all the Storm
 * config to be able to connect to and write to the DRPC servers using Thrift.
 */
@Slf4j
public class DRPCResultPublisher implements Publisher {
    @Setter(AccessLevel.PACKAGE)
    private ReturnResults bolt;
    private DRPCOutputCollector collector;

     /**
     * Creates and initializes a Publisher that writes to the DRPC servers. Intended to be used inside a Storm
     * bolt in a Storm topology.
     *
     * @param config Needs the Storm configuration {@link Map} in {@link com.yahoo.bullet.storm.BulletStormConfig#STORM_CONFIG}.
     */
    public DRPCResultPublisher(BulletConfig config) {
        // Get the Storm Config that has all the relevant cluster settings and properties
        Map stormConfig = config.getRequiredConfigAs(DRPCConfig.STORM_CONFIG, Map.class);

        collector = new DRPCOutputCollector();
        // Wrap the collector in a OutputCollector (it just delegates to the underlying DRPCOutputCollector)
        OutputCollector boltOutputCollector = new OutputCollector(collector);

        bolt = new ReturnResults();
        // No need for a TopologyContext
        bolt.prepare(stormConfig, null, boltOutputCollector);
    }

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        Metadata metadata = message.getMetadata();

        // Remove the content
        String content = metadata.getContent().toString();
        log.debug("Removing metadata {} for result {}@{}: {}", content, message.getId(), message.getSequence(), message.getContent());
        metadata.setContent(null);

        String serializedMessage = message.asJSON();
        Tuple tuple = new DRPCTuple(new Values(serializedMessage, content));

        // This sends the message through DRPC and not to the collector but it acks or fails accordingly.
        bolt.execute(tuple);
        if (!collector.isAcked()) {
            throw new PubSubException("Message not acked. Unable to send message through DRPC:\n " + serializedMessage);
        }
        // Otherwise, we're good to proceed
        collector.reset();
    }

    @Override
    public void close() {
        bolt.cleanup();
    }
}
