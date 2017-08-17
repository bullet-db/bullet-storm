package com.yahoo.bullet.storm;

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

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        if (message.getMetadata() == null) {
            throw new PubSubException("");
        }
        sent.add(message);
    }

    @Override
    public void close() {
        closed = true;
    }
}
