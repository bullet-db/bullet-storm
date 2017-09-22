/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j @Getter
public class QueuedThreadPoolDRPCClient {
    private BlockingQueue<Pair<String, String>> responseQueue;
    private DRPCClient client;
    private ExecutorService executorService;
    private AtomicBoolean isClosed = new AtomicBoolean();

    /**
     * Create a thread pool executor with given request adn response queue and thread pool sizes.
     *
     * @param client
     * @param requestQueueSize
     * @param responseQueueSize
     * @param threadPoolSize
     */
    public QueuedThreadPoolDRPCClient(DRPCClient client, int requestQueueSize, int responseQueueSize, int threadPoolSize) {
        BlockingQueue<Runnable> requestQueue = new ArrayBlockingQueue<>(requestQueueSize);
        responseQueue = new ArrayBlockingQueue<>(responseQueueSize);
        executorService = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, Long.MAX_VALUE, TimeUnit.MILLISECONDS, requestQueue);
        this.client = client;
        isClosed.set(false);
    }

    /**
     * Schedule a query for submission to Bullet.
     *
     * @param id The query ID associated with the query.
     * @param query The query to be executed by Bullet.
     */
    public void submitQuery(String id, String query) throws IllegalStateException {
        if (isClosed.get()) {
            throw new IllegalStateException("Thread pool closed");
        }
        // Add a task to the executor service that: makes a query and adds the result to the response queue.
        executorService.submit(() -> responseQueue.add(Pair.of(id, client.exeuteQuery(query))));
        log.info("Adding query for execution, ID: " + id + ", query: " + query);
    }

    /**
     *
     * @return
     */
    public Pair<String, String> getResponse() {
        if (!isClosed.get()) {
            return responseQueue.poll();
        }
        return null;
    }

    /**
     * Close the DRPCClient and shutdown the executor service it owns.
     */
    public void close() {
        isClosed.set(true);
        executorService.shutdownNow();
    }
}
