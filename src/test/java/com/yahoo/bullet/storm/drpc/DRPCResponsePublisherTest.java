/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.google.gson.Gson;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.storm.drpc.utils.DRPCResponseHandler;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DRPCResponsePublisherTest {
    private DRPCResponseHandler drpcResponseHandler;
    private DRPCResponsePublisher drpcResponsePublisher;
    private Map randomMessageId;
    private String randomResponse;
    private Map randomReturnInfo;
    private static Gson gson = new Gson();

    @BeforeMethod
    public void setup() {
        drpcResponseHandler = Mockito.mock(DRPCResponseHandler.class);
        drpcResponsePublisher = new DRPCResponsePublisher(drpcResponseHandler);
        randomMessageId = Collections.singletonMap("ID", UUID.randomUUID().toString());
        randomResponse = UUID.randomUUID().toString();
        randomReturnInfo = new HashMap();
        randomReturnInfo.put("host", "host");
        randomReturnInfo.put("port", 0);
        randomReturnInfo.put("id", UUID.randomUUID().toString());
    }

    @Test
    public void testSendWithoutReturnInfo() {
        drpcResponsePublisher.send(new PubSubMessage(gson.toJson(randomMessageId), randomResponse));
        Mockito.verify(drpcResponseHandler).fail(randomMessageId);
    }

    @Test
    public void testSendWithMalformedReturnInfo() {
        PubSubMessage message = new PubSubMessage(gson.toJson(randomMessageId), randomResponse,
                new Metadata(null, gson.toJson(randomReturnInfo) + "??"));
        drpcResponsePublisher.send(message);
        Mockito.verify(drpcResponseHandler).fail(randomMessageId);
    }

    @Test
    public void testSendWithMalformedId() {
        PubSubMessage message = new PubSubMessage("foo", randomResponse,
                new Metadata(null, gson.toJson(randomReturnInfo)));
        drpcResponsePublisher.send(message);
        Mockito.verifyZeroInteractions(drpcResponseHandler);
    }

    @Test
    public void testSuccessfulSend() {
        String serializedReturnInfo = gson.toJson(randomReturnInfo);
        Map deserializedReturnInfo = gson.fromJson(serializedReturnInfo, Map.class);
        PubSubMessage message = new PubSubMessage(gson.toJson(randomMessageId), randomResponse,
                new Metadata(null, serializedReturnInfo));
        drpcResponsePublisher.send(message);
        Mockito.verify(drpcResponseHandler).result(randomMessageId, randomResponse, deserializedReturnInfo);
    }

    @Test
    public void testClose() {
        drpcResponsePublisher.close();
        Mockito.verify(drpcResponseHandler).close();
    }
}
