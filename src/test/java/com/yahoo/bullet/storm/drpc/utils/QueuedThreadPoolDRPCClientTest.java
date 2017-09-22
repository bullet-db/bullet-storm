/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import org.apache.commons.lang3.tuple.Pair;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.UUID;

public class QueuedThreadPoolDRPCClientTest {
    QueuedThreadPoolDRPCClient queuedThreadPoolDRPCClient;
    DRPCClient drpcClient;

    @BeforeMethod
    public void setup() {
        drpcClient = Mockito.mock(DRPCClient.class);
        queuedThreadPoolDRPCClient = new QueuedThreadPoolDRPCClient(drpcClient, 1, 1, 1);
    }

    @AfterMethod
    public void teardown() {
        queuedThreadPoolDRPCClient.close();
    }

    @Test
    public void testClose() {
        queuedThreadPoolDRPCClient.close();
        Assert.assertTrue(queuedThreadPoolDRPCClient.getIsClosed().get());
        Assert.assertTrue(queuedThreadPoolDRPCClient.getExecutorService().isShutdown());
    }

    @Test
    public void testSubmitQuery() throws InterruptedException {
        String randomID = UUID.randomUUID().toString();
        String randomQuery = UUID.randomUUID().toString();
        queuedThreadPoolDRPCClient.submitQuery(randomID, randomQuery);
        Thread.sleep(100);
        Mockito.verify(drpcClient).exeuteQuery(randomQuery);
        Assert.assertEquals(queuedThreadPoolDRPCClient.getResponseQueue().size(), 1);
        Pair response = queuedThreadPoolDRPCClient.getResponseQueue().poll();
        Assert.assertEquals(response.getLeft(), randomID);
        Assert.assertNull(response.getRight());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testSubmitQueryWhenClosed() throws IllegalStateException {
        queuedThreadPoolDRPCClient.close();
        queuedThreadPoolDRPCClient.submitQuery(" ", " ");
    }

    @Test
    public void testGetResponse() throws InterruptedException {
        String randomID = UUID.randomUUID().toString();
        String randomQuery = UUID.randomUUID().toString();
        queuedThreadPoolDRPCClient.submitQuery(randomID, randomQuery);
        Thread.sleep(100);
        Mockito.verify(drpcClient).exeuteQuery(randomQuery);
        Pair response = queuedThreadPoolDRPCClient.getResponse();
        Assert.assertEquals(response.getLeft(), randomID);
        Assert.assertNull(response.getRight());
        Assert.assertNull(queuedThreadPoolDRPCClient.getResponse());
    }

    @Test
    public void testGetResponseWhenEmpty() {
        Assert.assertNull(queuedThreadPoolDRPCClient.getResponse());
    }

    @Test
    public void testGetResponseWhenClosed() {
        queuedThreadPoolDRPCClient.close();
        Assert.assertNull(queuedThreadPoolDRPCClient.getResponse());
    }
}
