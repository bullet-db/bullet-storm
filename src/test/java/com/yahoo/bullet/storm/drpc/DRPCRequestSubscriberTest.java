/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.google.gson.Gson;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.storm.drpc.utils.DRPCRequestHandler;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.UUID;

import static org.mockito.Matchers.anyLong;

public class DRPCRequestSubscriberTest {
    private DRPCRequestHandler drpcRequestHandler;
    private int fetchIntervalMs;
    private static Gson gson = new Gson();

    @BeforeMethod
    public void setup() {
        drpcRequestHandler = Mockito.mock(DRPCRequestHandler.class);
        fetchIntervalMs = 1000;
    }

    @AfterMethod
    public void teardown() {
        drpcRequestHandler.close();
    }

    @Test
    public void testReceiveWhenEmpty() {
        DRPCRequestSubscriber drpcRequestSubscriber = new DRPCRequestSubscriber(drpcRequestHandler, fetchIntervalMs);
        Assert.assertNull(drpcRequestSubscriber.receive());
    }

    @Test
    public void testReceive() {
        String randomID = UUID.randomUUID().toString();
        String randomContent = UUID.randomUUID().toString();
        Mockito.when(drpcRequestHandler.getNextRequest(anyLong())).thenReturn(new PubSubMessage(randomID, randomContent));
        DRPCRequestSubscriber drpcRequestSubscriber = new DRPCRequestSubscriber(drpcRequestHandler, fetchIntervalMs);
        PubSubMessage message = drpcRequestSubscriber.receive();
        Assert.assertEquals(message.getId(), randomID);
        Assert.assertEquals(message.getContent(), randomContent);
    }

    @Test
    public void testClose() {
        DRPCRequestSubscriber drpcRequestSubscriber = new DRPCRequestSubscriber(drpcRequestHandler, fetchIntervalMs);
        drpcRequestSubscriber.close();
        Assert.assertTrue(drpcRequestSubscriber.getExecutorService().isShutdown());
        Mockito.verify(drpcRequestHandler).close();
    }

    @Test
    public void testFail() {
        DRPCRequestSubscriber drpcRequestSubscriber = new DRPCRequestSubscriber(drpcRequestHandler, fetchIntervalMs);
        String id = gson.toJson(Collections.singletonMap("id", UUID.randomUUID().toString()));
        Object idObject = gson.fromJson(id, Object.class);
        drpcRequestSubscriber.fail(id);
        Mockito.verify(drpcRequestHandler).fail(idObject);
    }

    @Test
    public void testCommit() {
        DRPCRequestSubscriber drpcRequestSubscriber = new DRPCRequestSubscriber(drpcRequestHandler, fetchIntervalMs);
        String id = gson.toJson(Collections.singletonMap("id", UUID.randomUUID().toString()));
        Object idObject = gson.fromJson(id, Object.class);
        drpcRequestSubscriber.commit(id);
        Mockito.verify(drpcRequestHandler).ack(idObject);
    }
}
