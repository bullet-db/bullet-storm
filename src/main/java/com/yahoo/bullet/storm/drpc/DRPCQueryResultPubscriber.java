/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.RandomPool;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.storm.drpc.utils.DRPCError;
import lombok.AccessLevel;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * This class is both a query {@link Publisher} and a result {@link Subscriber}. It uses HTTP to do a request- response
 * call where the request is the Bullet query and the response is the result from the backend.
 */
@Slf4j
public class DRPCQueryResultPubscriber implements Publisher, Subscriber {
    private RandomPool<String> urls;
    private Queue<PubSubMessage> responses;
    @Setter(AccessLevel.PACKAGE)
    private AsyncHttpClient client;

    public static final int NO_TIMEOUT = -1;
    public static final int OK_200 = 200;
    // PROTOCOL://URL:PORT/DRPC_PATH/DRPC_FUNCTION
    private static final String URL_TEMPLATE = "%1$s://%2$s:%3$s/%4$s/%5$s";

    /**
     * Create an instance from a given {@link BulletConfig}. Requires {@link DRPCConfig#DRPC_HTTP_CONNECT_TIMEOUT},
     * {@link DRPCConfig#DRPC_HTTP_CONNECT_RETRY_LIMIT}, {@link DRPCConfig#DRPC_SERVERS}, {@link DRPCConfig#DRPC_HTTP_PORT},
     * and {@link DRPCConfig#DRPC_HTTP_PATH} to be set. To be used in PubSub.Context#QUERY_SUBMISSION mode.
     *
     * @param config A non-null config that contains the required settings.
     */
    public DRPCQueryResultPubscriber(BulletConfig config) {
        Objects.requireNonNull(config);

        Number connectTimeout = config.getRequiredConfigAs(DRPCConfig.DRPC_HTTP_CONNECT_TIMEOUT, Number.class);
        Number retryLimit = config.getRequiredConfigAs(DRPCConfig.DRPC_HTTP_CONNECT_RETRY_LIMIT, Number.class);
        List<String> servers = (List<String>) config.getRequiredConfigAs(DRPCConfig.DRPC_SERVERS, List.class);
        String protocol = config.getRequiredConfigAs(DRPCConfig.DRPC_HTTP_PROTOCOL, String.class);
        String port = config.getRequiredConfigAs(DRPCConfig.DRPC_HTTP_PORT, String.class);
        String path = config.getRequiredConfigAs(DRPCConfig.DRPC_HTTP_PATH, String.class);
        String function = config.getRequiredConfigAs(DRPCConfig.DRPC_FUNCTION, String.class);

        List<String> endpoints = servers.stream()
                                        .map(url -> String.format(URL_TEMPLATE, protocol, url, port, path, function))
                                        .collect(Collectors.toList());
        this.urls = new RandomPool<>(endpoints);
        AsyncHttpClientConfig clientConfig = new DefaultAsyncHttpClientConfig.Builder()
                                                    .setConnectTimeout(connectTimeout.intValue())
                                                    .setMaxRequestRetry(retryLimit.intValue())
                                                    .setReadTimeout(NO_TIMEOUT)
                                                    .setRequestTimeout(NO_TIMEOUT)
                                                    .build();
        // This is thread safe
        client = new DefaultAsyncHttpClient(clientConfig);
        responses = new ConcurrentLinkedQueue<>();
    }

    @Override
    public PubSubMessage send(PubSubMessage message) {
        String url = urls.get();
        String id = message.getId();
        String json = message.asJSON();
        log.info("Posting to {} for id {}", url, id);
        log.debug("Posting to {} with body {}", url, json);
        client.preparePost(url).setBody(json).execute().toCompletableFuture()
              .exceptionally(this::handleException)
              .thenAcceptAsync(createResponseConsumer(id));
        return message;
    }

    @Override
    public PubSubMessage receive() {
        return responses.poll();
    }

    @Override
    public void commit(String id) {
        // Do nothing
    }

    @Override
    public void fail(String id) {
        // Do nothing
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException ioe) {
            log.error("Error while closing AsyncHTTPClient", ioe);
        }
    }

    private Consumer<Response> createResponseConsumer(String id) {
        // Create a closure with id
        return response -> handleResponse(id, response);
    }

    private Response handleException(Throwable throwable) {
        // Just for logging
        log.error("Received error while posting query", throwable);
        return null;
    }

    private void handleResponse(String id, Response response) {
        if (response == null || response.getStatusCode() != OK_200) {
            log.error("Handling error for id {} with response {}", id, response);
            responses.offer(new PubSubMessage(id, DRPCError.CANNOT_REACH_DRPC.asJSONClip(), (Metadata) null));
            return;
        }
        log.info("Received for id {}: {} {}", response.getStatusCode(), id, response.getStatusText());
        String body = response.getResponseBody();
        PubSubMessage message = PubSubMessage.fromJSON(body);
        log.debug("Received for id {}:\n{}", message.getId(), message.getContent());
        responses.offer(message);
    }
}
