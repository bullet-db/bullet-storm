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
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.tuple.Tuple;

@Slf4j
public class ResultBolt extends PublisherBolt {
    private static final long serialVersionUID = -1927930701345251113L;

    /**
     * Creates a ResultBolt with a non-null {@link BulletStormConfig}.
     *
     * @param config The non-null BulletStormConfig to use. It should contain the settings to initialize a PubSub.
     */
    public ResultBolt(BulletStormConfig config) {
        super(config);
    }

    @Override
    protected Publisher createPublisher() throws PubSubException {
        try (PubSub pubSub = PubSub.from(config)) {
            Publisher publisher = pubSub.getPublisher();
            log.info("Setup PubSub: {} with Publisher: {}", pubSub, publisher);
            return publisher;
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        String result = tuple.getString(TopologyConstants.RESULT_POSITION);
        Metadata metadata = (Metadata) tuple.getValue(TopologyConstants.RESULT_METADATA_POSITION);
        publish(new PubSubMessage(id, result, metadata), tuple);
    }
}
