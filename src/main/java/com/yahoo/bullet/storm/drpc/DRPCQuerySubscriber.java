/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.BufferingSubscriber;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.storm.drpc.utils.DRPCOutputCollector;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class wraps a {@link DRPCSpout} and uses it to read messages from Storm DRPC. It needs all the Storm config to
 * be able to connect to and read from the DRPC servers using Thrift.
 *
 * It buffers read queries in memory upto a specified limit (and stops till further commits are received) and can
 * re-emit failed queries. However, it is not resilient if the Subscriber is closed or reinitialized elsewhere.
 */
@Slf4j
public class DRPCQuerySubscriber extends BufferingSubscriber {
    /** Exposed for testing only. */
    @Setter(AccessLevel.PACKAGE)
    private DRPCSpout spout;

    /** Exposed for testing only. */
    @Getter(AccessLevel.PACKAGE)
    private DRPCOutputCollector collector;

    // PubSubMessage id + sequence to DRPCMessageIds. For failing requests if the subscriber is closed.
    private Map<Pair<String, Integer>, Object> emittedIDs;

    /**
     * Creates and initializes a Subscriber that reads from the DRPC servers. Intended to be used inside a Storm
     * spout in a Storm topology.
     *
     * @param config The config containing the String function in {@link DRPCConfig#DRPC_FUNCTION}, the Storm configuration
     *               {@link Map} as {@link com.yahoo.bullet.storm.BulletStormConfig#STORM_CONFIG} and the Storm
     *               {@link TopologyContext} as {@link com.yahoo.bullet.storm.BulletStormConfig#STORM_CONTEXT}.
     * @param maxUnCommittedQueries The maximum number of queries that can be read without committing them.
     */
    public DRPCQuerySubscriber(BulletConfig config, int maxUnCommittedQueries) {
        super(maxUnCommittedQueries);

        collector = new DRPCOutputCollector();
        emittedIDs = new HashMap<>();

        // Get the Storm Config that has all the relevant cluster settings and properties
        Map stormConfig = config.getRequiredConfigAs(DRPCConfig.STORM_CONFIG, Map.class);

        // Get the TopologyContext
        TopologyContext context = config.getRequiredConfigAs(DRPCConfig.STORM_CONTEXT, TopologyContext.class);

        // Wrap the collector in a SpoutOutputCollector (it just delegates to the underlying DRPCOutputCollector)
        SpoutOutputCollector spoutOutputCollector = new SpoutOutputCollector(collector);

        // Get the DRPC function we should subscribe to
        String function = config.getRequiredConfigAs(DRPCConfig.DRPC_FUNCTION, String.class);

        spout = new DRPCSpout(function);
        spout.open(stormConfig, context, spoutOutputCollector);
    }

    @Override
    public List<PubSubMessage> getMessages() throws PubSubException {
        // Try and read from DRPC. The DRPCSpout does a sleep for 1 ms if there are no tuples, so we don't have to do it.
        spout.nextTuple();

        if (!collector.haveOutput()) {
            return null;
        }

        // The DRPCSpout only should have emitted one tuple
        List<List<Object>> tuples = collector.reset();

        log.debug("Have a message through DRPC {}", tuples);
        List<Object> tupleAndID = tuples.get(0);

        // The first object is the actual DRPCSpout tuple and the second is the DRPC messageID.
        List<Object> tuple = (List<Object>) tupleAndID.get(0);
        Object drpcID = tupleAndID.get(1);

        // The first object in the tuple is our PubSubMessage as JSON
        String pubSubMessageJSON = (String) tuple.get(0);
        // The second object in the tuple is the serialized returnInfo added by the DRPCSpout
        String returnInfo = (String) tuple.get(1);

        log.debug("Read message\n{}\nfrom DRPC with return information {}", pubSubMessageJSON, returnInfo);

        PubSubMessage pubSubMessage = PubSubMessage.fromJSON(pubSubMessageJSON);
        // Add returnInfo as metadata. Cannot add it to pubSubMessage
        String id = pubSubMessage.getId();
        String content = pubSubMessage.getContent();
        int sequence = pubSubMessage.getSequence();
        PubSubMessage message = new PubSubMessage(id, content, new Metadata(null, returnInfo), sequence);

        emittedIDs.put(ImmutablePair.of(id, sequence), drpcID);
        return Collections.singletonList(message);
    }

    @Override
    public void close() {
        log.warn("Failing all pending requests: {}", emittedIDs);
        emittedIDs.values().forEach(spout::fail);
        log.info("Closing spout...");
        spout.close();
    }
}
