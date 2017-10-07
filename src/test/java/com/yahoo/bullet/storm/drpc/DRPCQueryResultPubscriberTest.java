package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class DRPCQueryResultPubscriberTest {
    private DRPCQueryResultPubscriber spied;
    private BulletConfig config;

    private Response getMock(int status, String statusText, String body) {
        Response mock = mock(Response.class);
        doReturn(status).when(mock).getStatusCode();
        doReturn(statusText).when(mock).getStatusText();
        doReturn(body).when(mock).getResponseBody();
        return mock;
    }

    private Response getNotOkResponse(int status) {
        return getMock(status, "Error", null);
    }

    private Response getOkResponse(String data) {
        return getMock(DRPCQueryResultPubscriber.OK_200, "Ok", data);
    }

    private CompletableFuture<Response> getFuture(Response response) {
        CompletableFuture<Response> finished = CompletableFuture.completedFuture(response);
        doReturn(finished).when(finished).exceptionally(any());
        doReturn(finished).when(finished).thenAcceptAsync(any());
        return finished;
    }

    private AsyncHttpClient getMock(CompletableFuture<Response> future) {
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


    @BeforeMethod
    public void setup() {
        AsyncHttpClient mockClient = getMock(null);
        config = new DRPCConfig("src/test/resources/test_drpc_config.yaml");
        spied = spy(new DRPCQueryResultPubscriber(config));
        doReturn(mockClient).when(spied).getClient(any());
    }

    @Test
    public void test() throws Exception {
        DRPCQueryResultPubscriber pubscriber = new DRPCQueryResultPubscriber(config);
        pubscriber.send(new PubSubMessage("foo", "{}"));
    }
}