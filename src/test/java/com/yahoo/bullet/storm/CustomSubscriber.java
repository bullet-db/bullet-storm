/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Subscriber;
import lombok.Getter;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

@Getter
public class CustomSubscriber implements Subscriber {
    private Queue<PubSubMessage> queue = new LinkedList<>();
    private List<PubSubMessage> received = new ArrayList<>();
    private List<String> committed = new ArrayList<>();
    private List<String> failed = new ArrayList<>();
    private boolean closed = false;

    public void addMessages(PubSubMessage... pubSubMessages) {
        for (PubSubMessage pubSubMessage : pubSubMessages) {
            queue.add(pubSubMessage);
        }
    }

    @Override
    public PubSubMessage receive() throws PubSubException {
        if (queue.isEmpty()) {
            throw new PubSubException("");
        }
        PubSubMessage message = queue.remove();
        received.add(message);
        return message;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void commit(String id, int sequence) {
        committed.add(id);
    }

    @Override
    public void fail(String id, int sequence) {
        failed.add(id);
    }
}
