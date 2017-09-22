/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import com.yahoo.bullet.BulletConfig;
import org.apache.storm.drpc.DRPCInvocationsClient;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.thrift.TException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;

public class DRPCResponseHandlerTest {
    DRPCRequestHandler drpcRequestHandler;
    BulletConfig config;
    Map returnInfo;
    String randomMessageID;
    DRPCResponseHandler drpcResponseHandler;

    @BeforeMethod
    public void setup() throws IOException {
        drpcRequestHandler = Mockito.mock(DRPCRequestHandler.class);
        config = new BulletConfig("test_drpc_config.yaml");
        returnInfo = new HashMap();
        returnInfo.put("host", "localhost");
        returnInfo.put("port", 1);
        returnInfo.put("id", UUID.randomUUID().toString());
        randomMessageID = UUID.randomUUID().toString();
        drpcResponseHandler = new DRPCResponseHandler(drpcRequestHandler, config);
    }

    @AfterMethod
    public void teardown() {
        drpcRequestHandler.close();
        drpcResponseHandler.close();
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testGetInvocationsClientWhenAbsent() throws IOException {
        drpcResponseHandler.getInvocationsClient(returnInfo);
    }

    @Test
    public void testGetInvocationClientWhenPresent() {
        DRPCInvocationsClient client = Mockito.mock(DRPCInvocationsClient.class);
        Mockito.when(client.getHost()).thenReturn("localhost");
        Mockito.when(client.getPort()).thenReturn(1);
        drpcResponseHandler.getClients().put(Arrays.asList("localhost", 1), client);
        Assert.assertEquals(drpcResponseHandler.getInvocationsClient(returnInfo), client);
    }

    @Test
    public void testSuccessfulResult() throws TException {
        String randomResult = UUID.randomUUID().toString();
        DRPCInvocationsClient client = Mockito.mock(DRPCInvocationsClient.class);
        Mockito.when(client.getHost()).thenReturn("localhost");
        Mockito.when(client.getPort()).thenReturn(1);
        drpcResponseHandler.getClients().put(Arrays.asList("localhost", 1), client);
        drpcResponseHandler.result(randomMessageID, randomResult, returnInfo);
        Mockito.verify(client).result(returnInfo.get("id").toString(), randomResult);
    }

    @Test
    public void testResultWhenNotAuthorized() throws TException {
        String randomResult = UUID.randomUUID().toString();
        DRPCInvocationsClient client = Mockito.mock(DRPCInvocationsClient.class);
        Mockito.when(client.getHost()).thenReturn("localhost");
        Mockito.when(client.getPort()).thenReturn(1);
        Mockito.doThrow(new AuthorizationException()).when(client).result(anyString(), anyString());
        drpcResponseHandler.getClients().put(Arrays.asList("localhost", 1), client);
        try {
            drpcResponseHandler.result(randomMessageID, randomResult, returnInfo);
        } catch (RuntimeException e) {
            Assert.assertEquals(e.getCause().getClass(), AuthorizationException.class);
        }
        Mockito.verify(client).result(returnInfo.get("id").toString(), randomResult);
        Mockito.verify(drpcRequestHandler).fail(randomMessageID);
    }

    @Test
    public void testResultWhenTransportError() throws TException {
        String randomResult = UUID.randomUUID().toString();
        DRPCInvocationsClient client = Mockito.mock(DRPCInvocationsClient.class);
        Mockito.when(client.getHost()).thenReturn("localhost");
        Mockito.when(client.getPort()).thenReturn(1);
        Mockito.doThrow(new TException()).when(client).result(anyString(), anyString());
        drpcResponseHandler.getClients().put(Arrays.asList("localhost", 1), client);
        drpcResponseHandler.result(randomMessageID, randomResult, returnInfo);
        Mockito.verify(client, times(5)).result(returnInfo.get("id").toString(), randomResult);
        Mockito.verify(drpcRequestHandler).fail(randomMessageID);
    }

    @Test
    public void testClose() {
        Map<List, DRPCInvocationsClient> clients = drpcResponseHandler.getClients();
        for (int i = 0; i < 10; i++) {
            clients.put(Arrays.asList("localhost", i), Mockito.mock(DRPCInvocationsClient.class));
        }
        drpcResponseHandler.close();
        clients.forEach((x, y) -> Mockito.verify(y).close());
    }

    @Test
    public void testReconnectClient() throws TException {
        DRPCInvocationsClient client = Mockito.mock(DRPCInvocationsClient.class);
        drpcResponseHandler.reconnectClient(client);
        Mockito.verify(client).reconnectClient();
    }

    @Test
    public void testReconnectClientFailure() throws TException {
        DRPCInvocationsClient client = Mockito.mock(DRPCInvocationsClient.class);
        doThrow(new TException()).when(client).reconnectClient();
        drpcResponseHandler.reconnectClient(client);
        Mockito.verify(client).reconnectClient();
    }
}
