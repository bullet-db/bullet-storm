/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.storm.drpc.utils.DRPCRequestHandler;
import lombok.Getter;
import org.apache.storm.shade.org.json.simple.JSONValue;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Getter
public class DRPCRequestSubscriber implements Subscriber {
    private DRPCRequestHandler drpcRequestHandler;
    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    /**
     * Create a DRPCRequestSubscriber from a {@link DRPCRequestHandler}.
     *
     * @param drpcRequestHandler The DRPCRequestHandler used to fetch requests from the DRPC servers.
     */
    public DRPCRequestSubscriber(DRPCRequestHandler drpcRequestHandler, int fetchIntervalMs) {
        this.drpcRequestHandler = drpcRequestHandler;
        // Fetch new requests from the DRPC server periodically.
        executorService.scheduleAtFixedRate(drpcRequestHandler::fetchRequest, 0, fetchIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public PubSubMessage receive() {
        return drpcRequestHandler.getNextRequest(0);
    }

    @Override
    public void close() {
        drpcRequestHandler.close();
        executorService.shutdownNow();
    }

    @Override
    public void fail(String id, int index) {
        Object messageID = JSONValue.parse(id);
        drpcRequestHandler.fail(messageID);
    }

    /**
     * Convenience method to fail drpc requests where index is not relevant.
     *
     * @param id The serialized DRPCMessageID.
     */
    public void fail(String id) {
        fail(id, 0);
    }

    @Override
    public void commit(String id, int index) {
        Object messageID = JSONValue.parse(id);
        drpcRequestHandler.ack(messageID);
    }

    /**
     * Convenience method to fail DRPC requests where index is not relevant.
     *
     * @param id The serialized DRPCMessageID.
     */
    public void commit(String id) {
        commit(id, 0);
    }
}
