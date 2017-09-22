/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.google.gson.Gson;
import org.apache.storm.drpc.DRPCInvocationsClient;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.thrift.TException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

public class DRPCRequestHandlerTest {
    DRPCRequestHandler drpcRequestHandler;
    String randomID = UUID.randomUUID().toString();
    String randomArgs = UUID.randomUUID().toString();
    Gson gson = new Gson();

    @BeforeMethod
    public void setup() throws IOException {
        drpcRequestHandler = new DRPCRequestHandler(new BulletConfig("test_drpc_config.yaml"));
    }

    @AfterMethod
    public void teardown() {
        drpcRequestHandler.close();
    }

    @Test
    public void testFetchRequest() throws InterruptedException, TException {
        DRPCInvocationsClient client = Mockito.mock(DRPCInvocationsClient.class);
        Mockito.when(client.isConnected()).thenReturn(true);
        Mockito.when(client.fetchRequest(anyString())).thenReturn(new DRPCRequest(randomArgs, randomID));
        drpcRequestHandler.getClients().clear();
        drpcRequestHandler.getClients().addAll(Arrays.asList(Mockito.mock(DRPCInvocationsClient.class), client));
        Thread.sleep(100);
        drpcRequestHandler.fetchRequest();
        PubSubMessage message = drpcRequestHandler.getRequests().poll();
        Assert.assertNotNull(message);
        String receivedID = (gson.fromJson(message.getId(), Map.class)).get("id").toString();
        Assert.assertEquals(receivedID, randomID);
        Assert.assertEquals(message.getContent(), randomArgs);
    }

    @Test
    public void testFetchRequestWhenNotAuthorized() throws TException {
        DRPCInvocationsClient client = Mockito.mock(DRPCInvocationsClient.class);
        Mockito.when(client.isConnected()).thenReturn(true);
        Mockito.doThrow(new AuthorizationException()).when(client).fetchRequest(anyString());
        drpcRequestHandler.getClients().clear();
        drpcRequestHandler.getClients().add(client);

        drpcRequestHandler.fetchRequest();
        Assert.assertEquals(drpcRequestHandler.getRequests().size(), 0);
        Mockito.verify(client).reconnectClient();
    }

    @Test
    public void testFetchRequestWhenUnableToConnect() throws TException {
        DRPCInvocationsClient client = Mockito.mock(DRPCInvocationsClient.class);
        Mockito.when(client.isConnected()).thenReturn(true);
        Mockito.doThrow(new TException()).when(client).fetchRequest(anyString());
        drpcRequestHandler.getClients().clear();
        drpcRequestHandler.getClients().add(client);

        drpcRequestHandler.fetchRequest();
        Assert.assertEquals(drpcRequestHandler.getRequests().size(), 0);
        Mockito.verify(client).reconnectClient();
    }

    @Test
    public void testFetchRequestWhenClosed() throws TException {
        DRPCInvocationsClient client = Mockito.mock(DRPCInvocationsClient.class);
        Mockito.when(client.isConnected()).thenReturn(true);
        Mockito.doThrow(new AuthorizationException()).when(client).fetchRequest(anyString());
        drpcRequestHandler.getClients().clear();
        drpcRequestHandler.getClients().add(client);
        drpcRequestHandler.close();

        drpcRequestHandler.fetchRequest();
        Assert.assertEquals(drpcRequestHandler.getRequests().size(), 0);
        Mockito.verify(client).close();
        Mockito.verify(client, Mockito.never()).fetchRequest(any());
    }

    @Test
    public void testFetchRequestWhenFull() throws TException {
        DRPCInvocationsClient client = Mockito.mock(DRPCInvocationsClient.class);
        Mockito.when(client.isConnected()).thenReturn(true);
        Mockito.doThrow(new AuthorizationException()).when(client).fetchRequest(anyString());
        drpcRequestHandler.getClients().clear();
        drpcRequestHandler.getClients().add(client);
        drpcRequestHandler.getRequests().addAll(Collections.nCopies(2, new PubSubMessage("", "")));
        drpcRequestHandler.fetchRequest();
        Mockito.verify(client, Mockito.never()).fetchRequest(any());
        drpcRequestHandler.getRequests().clear();
    }
}
