/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static com.yahoo.bullet.storm.drpc.utils.DRPCError.CANNOT_REACH_DRPC;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class DRPCQueryResultPubscriberTest {
    private DRPCQueryResultPubscriber pubscriber;

    private Response getResponse(int status, String statusText, String body) {
        Response mock = mock(Response.class);
        doReturn(status).when(mock).getStatusCode();
        doReturn(statusText).when(mock).getStatusText();
        doReturn(body).when(mock).getResponseBody();
        return mock;
    }

    private Response getNotOkResponse(int status) {
        return getResponse(status, "Error", null);
    }

    private Response getOkResponse(String data) {
        return getResponse(DRPCQueryResultPubscriber.OK_200, "Ok", data);
    }

    private CompletableFuture<Response> getOkFuture(Response response) {
        CompletableFuture<Response> finished = CompletableFuture.completedFuture(response);
        CompletableFuture<Response> mock = mock(CompletableFuture.class);
        // This is the weird bit. We mock the call to exceptionally to return the finished response so that chaining
        // a thenAcceptAsync on it will call the consumer of it with the finished response. This is why it looks
        // weird: mocking the exceptionally to take the "good" path.
        doReturn(finished).when(mock).exceptionally(any());
        // So if we do get to thenAccept on our mock, we should throw an exception because we shouldn't get there.
        doThrow(new RuntimeException("Good futures don't throw")).when(mock).thenAcceptAsync(any());
        return mock;
    }

    private AsyncHttpClient mockClientWith(CompletableFuture<Response> future) {
        ListenableFuture<Response> mockListenable = (ListenableFuture<Response>) mock(ListenableFuture.class);
        doReturn(future).when(mockListenable).toCompletableFuture();

        BoundRequestBuilder mockBuilder = mock(BoundRequestBuilder.class);
        doReturn(mockListenable).when(mockBuilder).execute();
        // Return itself
        doReturn(mockBuilder).when(mockBuilder).setBody(anyString());

        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        doReturn(mockBuilder).when(mockClient).preparePost(anyString());
        return mockClient;
    }

    private PubSubMessage fetch() {
        try {
            PubSubMessage message;
            do {
                message = pubscriber.receive();
                Thread.sleep(1);
            } while (message == null);
            return message;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<PubSubMessage> fetchAsync() {
        return CompletableFuture.supplyAsync(this::fetch);
    }

    @BeforeMethod
    public void setup() {
        BulletConfig config = new DRPCConfig("src/test/resources/test_drpc_config.yaml");
        config.set(DRPCConfig.DRPC_SERVERS, Collections.singletonList("foo.bar.bullet.drpc.com"));

        pubscriber = new DRPCQueryResultPubscriber(config);
    }

    @Test(timeOut = 5000L)
    public void testReadingOkResponse() throws Exception {
        PubSubMessage expected = new PubSubMessage("foo", "response");

        CompletableFuture<Response> response = getOkFuture(getOkResponse(expected.asJSON()));
        AsyncHttpClient mockClient = mockClientWith(response);
        pubscriber.setClient(mockClient);

        pubscriber.send(new PubSubMessage("foo", "bar"));
        // This is async (but practically still very fast since we mocked the response), so need a timeout.
        PubSubMessage actual = fetchAsync().get();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getId(), expected.getId());
        Assert.assertEquals(actual.getContent(), expected.getContent());
    }

    @Test(timeOut = 5000L)
    public void testReadingNotOkResponse() throws Exception {
        CompletableFuture<Response> response = getOkFuture(getNotOkResponse(500));
        AsyncHttpClient mockClient = mockClientWith(response);
        pubscriber.setClient(mockClient);

        pubscriber.send(new PubSubMessage("foo", "bar"));
        // This is async (but practically still very fast since we mocked the response), so need a timeout.
        PubSubMessage actual = fetchAsync().get();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getId(), "foo");
        Assert.assertEquals(actual.getContentAsString(), CANNOT_REACH_DRPC.asJSONClip());
    }

    @Test(timeOut = 5000L)
    public void testReadingNullResponse() throws Exception {
        CompletableFuture<Response> response = getOkFuture(null);
        AsyncHttpClient mockClient = mockClientWith(response);
        pubscriber.setClient(mockClient);

        pubscriber.send(new PubSubMessage("foo", "bar"));
        // This is async (but practically still very fast since we mocked the response), so need a timeout.
        PubSubMessage actual = fetchAsync().get();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getId(), "foo");
        Assert.assertEquals(actual.getContentAsString(), CANNOT_REACH_DRPC.asJSONClip());
    }

    @Test
    public void testClosing() throws Exception {
        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        pubscriber.setClient(mockClient);
        pubscriber.close();
        verify(mockClient, times(1)).close();
    }

    @Test
    public void testClosingWithException() throws Exception {
        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        doThrow(new IOException()).when(mockClient).close();

        pubscriber.setClient(mockClient);
        pubscriber.close();
        verify(mockClient, times(1)).close();
    }

    @Test
    public void testCommiting() {
        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        pubscriber.commit("foo");
        verifyZeroInteractions(mockClient);
    }

    @Test
    public void testFailing() {
        AsyncHttpClient mockClient = mock(AsyncHttpClient.class);
        pubscriber.fail("foo");
        verifyZeroInteractions(mockClient);
    }

    @Test(timeOut = 5000L)
    public void testException() throws Exception {
        // This will hit a non-existent url and fail, testing our exceptions. Our connect and retry is low so even if
        // block the full amount, it's still fast.
        pubscriber.send(new PubSubMessage("foo", "bar"));
        PubSubMessage actual = fetchAsync().get();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getId(), "foo");
        Assert.assertEquals(actual.getContentAsString(), CANNOT_REACH_DRPC.asJSONClip());
    }
}
