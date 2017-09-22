/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.storm.drpc.utils.QueuedThreadPoolDRPCClient;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.mockito.Matchers.anyString;

public class DRPCRequestPublisherTest {
    @Test
    public void testSend() throws PubSubException {
        QueuedThreadPoolDRPCClient queuedThreadPoolDRPCClient = Mockito.mock(QueuedThreadPoolDRPCClient.class);
        DRPCRequestPublisher drpcRequestPublisher = new DRPCRequestPublisher(queuedThreadPoolDRPCClient);
        String randomID = UUID.randomUUID().toString();
        String randomQuery = UUID.randomUUID().toString();
        drpcRequestPublisher.send(new PubSubMessage(randomID, randomQuery));
        Mockito.verify(queuedThreadPoolDRPCClient).submitQuery(randomID, randomQuery);
    }

    @Test
    public void testClose() {
        QueuedThreadPoolDRPCClient queuedThreadPoolDRPCClient = Mockito.mock(QueuedThreadPoolDRPCClient.class);
        DRPCRequestPublisher drpcRequestPublisher = new DRPCRequestPublisher(queuedThreadPoolDRPCClient);
        drpcRequestPublisher.close();
        Mockito.verify(queuedThreadPoolDRPCClient).close();
    }

    @Test(expectedExceptions = PubSubException.class)
    public void testSendWhenClosed() throws PubSubException {
        QueuedThreadPoolDRPCClient queuedThreadPoolDRPCClient = Mockito.mock(QueuedThreadPoolDRPCClient.class);
        Mockito.doThrow(new IllegalStateException()).when(queuedThreadPoolDRPCClient).submitQuery(anyString(), anyString());
        DRPCRequestPublisher drpcRequestPublisher = new DRPCRequestPublisher(queuedThreadPoolDRPCClient);
        drpcRequestPublisher.send(new PubSubMessage("", ""));
    }
}
