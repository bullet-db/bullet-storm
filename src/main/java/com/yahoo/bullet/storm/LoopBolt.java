/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import backtype.storm.tuple.Tuple;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class LoopBolt extends PublisherBolt {
    private static final long serialVersionUID = 1291597467671042429L;

    /**
     * Creates an instance of this class with the given non-null config.
     *
     * @param config The non-null validated {@link BulletStormConfig} which is the config for this component.
     */
    public LoopBolt(BulletStormConfig config) {
        super(config);
    }

    @Override
    protected Publisher createPublisher() throws PubSubException {
        PubSub pubSub = PubSub.from(config);

        // Map is always not null and is validated to be a proper BulletStormConfig
        Map<String, Object> overrides = (Map<String, Object>) config.getAs(BulletStormConfig.LOOP_BOLT_PUBSUB_OVERRIDES, Map.class);
        log.info("Loaded pubsub overrides: {}", overrides);
        BulletStormConfig modified = new BulletStormConfig(config);
        overrides.forEach(modified::set);
        pubSub.switchContext(PubSub.Context.QUERY_SUBMISSION, modified);
        log.info("Switched the PubSub into query submission mode");

        Publisher publisher = pubSub.getPublisher();
        log.info("Setup PubSub: {} with Publisher: {}", pubSub, publisher);
        return publisher;
    }

    @Override
    public void execute(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        Metadata metadata = (Metadata) tuple.getValue(TopologyConstants.METADATA_POSITION);
        log.info("Looping back metadata with signal {} for {}", metadata.getSignal(), id);
        publish(new PubSubMessage(id, null, metadata), tuple);
    }
}
