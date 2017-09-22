/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.google.gson.Gson;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.storm.drpc.utils.DRPCResponseHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;

@Slf4j
public class DRPCResponsePublisher implements Publisher {
    private DRPCResponseHandler drpcResponseHandler;
    private static final Gson GSON = new Gson();

    /**
     * Create a DRPCResponsePublisher from a {@link DRPCResponseHandler}.
     *
     * @param drpcResponseHandler The {@link DRPCResponseHandler} used to write responses to DRPC servers.
     */
    public DRPCResponsePublisher(DRPCResponseHandler drpcResponseHandler) {
        this.drpcResponseHandler = drpcResponseHandler;
    }

    @Override
    public void send(PubSubMessage message) {
        String result = message.getContent();
        Object messageID = null;
        Map retMap = null;
        try {
            messageID = GSON.fromJson(message.getId(), Map.class);
            Metadata metadata = Objects.requireNonNull(message.getMetadata());
            String returnInfo = (String) Objects.requireNonNull(metadata.getContent());
            retMap = GSON.fromJson(returnInfo, Map.class);
        } catch (Exception e) {
            log.error("Invalid return information", e);
            if (messageID != null) {
                drpcResponseHandler.fail(messageID);
            }
            return;
        }
        drpcResponseHandler.result(messageID, result, retMap);
    }

    @Override
    public void close() {
        drpcResponseHandler.close();
    }
}
