/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.storm.BulletStormConfig;
import com.yahoo.bullet.storm.drpc.BulletDRPCConfig;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DRPCClientTest {
    private final String testConfig = "test_drpc_config.yaml";

    private Stubber add(Stubber ongoingStub, Object object) {
        Answer next = invocation -> {
            if (object instanceof Exception) {
                throw (Throwable) object;
            } else {
                Object[] args = invocation.getArguments();
                // The second argument is the url. Add that to the object
                return new DRPCResponse(args[1] + " : " + object);
            }
        };
        return ongoingStub == null ? doAnswer(next) : ongoingStub.doAnswer(next);
    }

    /**
     * A function for testing that will return a mocked Client that will return the provided Response object.
     *
     * @param response The response (usually mocked) Response that the returned Client will return
     * @return A Client which will return the provided Response
     */
    private Client makeClient(Response response) {
        Invocation.Builder builder = mock(Invocation.Builder.class);
        when(builder.post(any(Entity.class))).thenReturn(response);

        WebTarget target = mock(WebTarget.class);
        when(target.request(MediaType.TEXT_PLAIN)).thenReturn(builder);

        Client client = mock(Client.class);
        when(client.target(anyString())).thenReturn(target);

        return client;
    }

    private Response makeResponse(String entity, Response.Status status) {
        Response response = mock(Response.class);
        when(response.getStatusInfo()).thenReturn(status);
        when(response.readEntity(String.class)).thenReturn(entity);
        return response;
    }

    private DRPCClient makeResponses(DRPCClient drpcClient, Object... responses) {
        DRPCClient spied = Mockito.spy(drpcClient);
        Stubber stub = null;
        for (Object response : responses) {
            stub = add(stub, response);
        }
        stub.doThrow(new ProcessingException("")).when(spied).makeRequest(any(Client.class), anyString(), anyString());
        return spied;
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testBadInitialization() throws IOException {
        BulletConfig config = new BulletConfig(testConfig);
        config.set(BulletDRPCConfig.DRPC_SERVERS, null);
        new DRPCClient(config);
    }

    @Test
    public void testRequestToRandomURL() throws IOException {
        Assert.assertNotNull(new DRPCClient(new BulletStormConfig(testConfig)).makeRequest(ClientBuilder.newClient(), "http://www.yahoo.com", "foo"));
    }

    @Test
    public void testMakeRequestSuccess() throws IOException {
        DRPCClient drpcClient = new DRPCClient(new BulletConfig(testConfig));
        Response response = makeResponse("good", Response.Status.OK);
        Client client = makeClient(response);
        DRPCResponse drpcResponse = drpcClient.makeRequest(client, "ignored", "ignored");
        Assert.assertFalse(drpcResponse.hasError());
        Assert.assertEquals(drpcResponse.getContent(), "good");
    }

    @Test
    public void testConfiguredBehavior() throws IOException {
        BulletConfig config = new BulletConfig(testConfig);
        DRPCClient drpcClient = new DRPCClient(config);
        Assert.assertEquals(drpcClient.getUrls(), new RandomPool(singletonList("http://host:3774/drpc/query")));
        Assert.assertEquals(drpcClient.getConnectTimeout(), 10);
        Assert.assertEquals(drpcClient.getRetryLimit(), 5);
    }

    @Test
    public void testConfiguredInvocation() throws IOException {
        BulletConfig config = new BulletConfig(testConfig);
        config.set(BulletDRPCConfig.DRPC_CONNECT_RETRY_LIMIT, 1);
        config.set(BulletDRPCConfig.DRPC_CONNECT_TIMEOUT, 1);
        DRPCClient drpcClient = new DRPCClient(config);
        String response = drpcClient.exeuteQuery("ignored");

        DRPCError cause = DRPCError.RETRY_LIMIT_EXCEEDED;
        String errorMessage = Clip.of(com.yahoo.bullet.result.Metadata.of(Error.makeError(cause.getError(), cause.getResolution()))).asJSON();
        Assert.assertEquals(response, errorMessage);
    }

    @Test
    public void testRetry() throws IOException {
        DRPCClient drpcClient = new DRPCClient(new BulletConfig(testConfig));
        DRPCClient service = makeResponses(drpcClient, new ProcessingException("1"), "2");
        String response = service.exeuteQuery("ignored");
        Assert.assertEquals(response, "http://host:3774/drpc/query : 2");
    }

    @Test
    public void testFullFailure() throws IOException {
        DRPCClient drpcClient = new DRPCClient(new BulletConfig(testConfig));
        DRPCClient service = makeResponses(drpcClient, new ProcessingException("1"), new ProcessingException("2"),
                                            new ProcessingException("3"), new ProcessingException("4"));
        String response = service.exeuteQuery("ignored");
        DRPCError cause = DRPCError.RETRY_LIMIT_EXCEEDED;
        String errorMessage = Clip.of(com.yahoo.bullet.result.Metadata.of(Error.makeError(cause.getError(), cause.getResolution()))).asJSON();
        Assert.assertEquals(response, errorMessage);
    }
}
