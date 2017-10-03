package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Subscriber;

public class DRPCQuerySubscriber implements Subscriber {
    @Override
    public PubSubMessage receive() throws PubSubException {
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public void commit(String id, int sequence) {

    }

    @Override
    public void fail(String id, int sequence) {

    }
}
