/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;

import java.util.List;

public class MockPubSub extends PubSub {
    private Publisher publisher;
    private Subscriber subscriber;

    public MockPubSub(BulletConfig config) throws PubSubException {
        super(config);
        PubSub.Context context = PubSub.Context.valueOf(config.get(BulletConfig.PUBSUB_CONTEXT_NAME).toString());
        this.publisher = new CustomPublisher(context);
        this.subscriber = new CustomSubscriber(context);
    }

    @Override
    public void switchContext(Context context, BulletConfig config) throws PubSubException {
        super.switchContext(context, config);
        this.publisher = new CustomPublisher(context);
        this.subscriber = new CustomSubscriber(context);
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
