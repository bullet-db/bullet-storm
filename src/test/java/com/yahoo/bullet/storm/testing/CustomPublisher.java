/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class CustomPublisher implements Publisher {
    private List<PubSubMessage> sent = new ArrayList<>();
    private boolean closed = false;
    private boolean thrown = false;
    private PubSub.Context context;

    public CustomPublisher(PubSub.Context context) {
        this.context = context;
    }

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        if (closed) {
            throw new PubSubException("");
        }
        sent.add(message);
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            thrown = true;
            throw new Exception();
        }
        closed = true;
    }
}
