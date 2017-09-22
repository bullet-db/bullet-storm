/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.storm.drpc.utils.QueuedThreadPoolDRPCClient;
import org.apache.commons.lang3.tuple.Pair;

public class DRPCResponseSubscriber implements Subscriber {
    private QueuedThreadPoolDRPCClient client;

    /**
     * Create a DRPCRequestPublisher from a {@link QueuedThreadPoolDRPCClient}.
     *
     * @param client The QueuedThreadPoolDRPCClient to read responses from.
     */
    DRPCResponseSubscriber(QueuedThreadPoolDRPCClient client) {
        this.client = client;
    }

    @Override
    public PubSubMessage receive() {
        Pair<String, String> response = client.getResponse();
        return response == null ? null : new PubSubMessage(response.getKey(), response.getValue());
    }


    @Override
    public void close() {
        client.close();
    }

    // Since DRPC PubSub completely resides on a local machine on the QUERY_SUBMISSION side, commit and fail are no-op.
    @Override
    public void commit(String id, int sequence) {
    }

    @Override
    public void fail(String id, int sequence) {
    }
}
