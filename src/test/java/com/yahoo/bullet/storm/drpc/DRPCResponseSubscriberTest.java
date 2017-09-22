/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.storm.drpc.utils.QueuedThreadPoolDRPCClient;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.mockito.Mockito;
import org.testng.Assert;

import java.util.UUID;

public class DRPCResponseSubscriberTest {
    @Test
    public void testReceive() {
        QueuedThreadPoolDRPCClient queuedThreadPoolDRPCClient = Mockito.mock(QueuedThreadPoolDRPCClient.class);
        String randomID = UUID.randomUUID().toString();
        String randomContent = UUID.randomUUID().toString();
        Mockito.when(queuedThreadPoolDRPCClient.getResponse()).thenReturn(Pair.of(randomID, randomContent));
        DRPCResponseSubscriber drpcResponseSubscriber = new DRPCResponseSubscriber(queuedThreadPoolDRPCClient);
        PubSubMessage message = drpcResponseSubscriber.receive();
        Assert.assertEquals(message.getId(), randomID);
        Assert.assertEquals(message.getContent(), randomContent);
    }

    @Test
    public void testClose() {
        QueuedThreadPoolDRPCClient queuedThreadPoolDRPCClient = Mockito.mock(QueuedThreadPoolDRPCClient.class);
        DRPCResponseSubscriber drpcResponseSubscriber = new DRPCResponseSubscriber(queuedThreadPoolDRPCClient);
        drpcResponseSubscriber.close();
        Mockito.verify(queuedThreadPoolDRPCClient).close();
    }

    @Test
    public void testReceiveWhenClosed() {
        QueuedThreadPoolDRPCClient queuedThreadPoolDRPCClient = Mockito.mock(QueuedThreadPoolDRPCClient.class);
        DRPCResponseSubscriber drpcResponseSubscriber = new DRPCResponseSubscriber(queuedThreadPoolDRPCClient);
        drpcResponseSubscriber.close();
        Mockito.verify(queuedThreadPoolDRPCClient).close();
        Assert.assertNull(drpcResponseSubscriber.receive());
    }
}
