/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.storm.drpc.utils.QueuedThreadPoolDRPCClient;

public class DRPCRequestPublisher implements Publisher {
    private QueuedThreadPoolDRPCClient client;

    /**
     * Create a DRPCRequestPublisher from a {@link QueuedThreadPoolDRPCClient}.
     *
     * @param client The QueuedThreadPoolDRPCClient to submit requests to.
     */
    public DRPCRequestPublisher(QueuedThreadPoolDRPCClient client) {
        this.client = client;
    }

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        try {
            client.submitQuery(message.getId(), message.getContent());
        } catch (IllegalStateException e) {
            throw new PubSubException("Could not submit query", e);
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
