/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.storm.drpc.utils.DRPCClient;
import com.yahoo.bullet.storm.drpc.utils.DRPCRequestHandler;
import com.yahoo.bullet.storm.drpc.utils.DRPCResponseHandler;
import com.yahoo.bullet.storm.drpc.utils.QueuedThreadPoolDRPCClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.utils.Utils;

import java.util.Collections;
import java.util.List;

@Slf4j
public class DRPCPubSub extends PubSub {
    private BulletConfig config;
    private QueuedThreadPoolDRPCClient client;
    private DRPCRequestHandler drpcRequestHandler;
    private DRPCResponseHandler drpcResponseHandler;
    private int fetchIntervalMs;
    /**
     * Create a DRPCPubSub using a {@link BulletDRPCConfig}.
     *
     * @param config The BulletDRPCConfig containing settings to create new DRPCPubSub.
     */
    public DRPCPubSub(BulletConfig config) {
        super(config);
        this.config = config;
        log.info("DRPC PubSub started with config: " + config.toString());
        DRPCClient drpcClient = new DRPCClient(config);
        int requestQueueSize = Utils.getInt(config.get(BulletDRPCConfig.DRPC_REQUEST_QUEUE_SIZE));
        int responseQueueSize = Utils.getInt(config.get(BulletDRPCConfig.DRPC_RESPONSE_QUEUE_SIZE));
        int threadPoolSize = Utils.getInt(config.get(BulletDRPCConfig.DRPC_THREAD_POOL_SIZE));
        client = new QueuedThreadPoolDRPCClient(drpcClient, requestQueueSize, responseQueueSize, threadPoolSize);
        fetchIntervalMs = Utils.getInt(config.get(BulletDRPCConfig.DRPC_REQUEST_FETCH_INTERVAL_MS));

        drpcRequestHandler = new DRPCRequestHandler(config);
        drpcResponseHandler = new DRPCResponseHandler(drpcRequestHandler, config);
    }

    @Override
    public Subscriber getSubscriber() {
        return context == Context.QUERY_SUBMISSION ? new DRPCResponseSubscriber(client) :
                new DRPCRequestSubscriber(drpcRequestHandler, fetchIntervalMs);
    }

    @Override
    public Publisher getPublisher() {
        return context == Context.QUERY_SUBMISSION ? new DRPCRequestPublisher(client) :
                new DRPCResponsePublisher(drpcResponseHandler);
    }

    @Override
    public List<Subscriber> getSubscribers(int n) {
        return Collections.nCopies(n, getSubscriber());
    }

    @Override
    public List<Publisher> getPublishers(int n) {
        return Collections.nCopies(n, getPublisher());
    }
}
