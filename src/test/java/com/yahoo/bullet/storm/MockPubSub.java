/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;

import java.util.List;

public class MockPubSub extends PubSub {
    Publisher publisher = new CustomPublisher();
    Subscriber subscriber = new CustomSubscriber();

    public MockPubSub(BulletConfig config) {
        super(config);
    }

    @Override
    public Publisher getPublisher() {
        return publisher;
    }

    @Override
    public Subscriber getSubscriber() {
        return subscriber;
    }

    @Override
    public List<Publisher> getPublishers(int n) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Subscriber> getSubscribers(int n) {
        throw new UnsupportedOperationException();
    }
}
