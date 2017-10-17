/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.result.JSONFormatter;
import com.yahoo.bullet.storm.drpc.utils.DRPCOutputCollector;
import lombok.Getter;
import org.apache.storm.drpc.DRPCSpout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static java.util.Arrays.asList;

@Getter
public class MockDRPCSpout extends DRPCSpout {
    private boolean closed = false;
    private Queue<List<Object>> tuples = new LinkedList<>();
    private Queue<Object> messageIDs = new LinkedList<>();
    private DRPCOutputCollector collector;
    private List<Object> failed = new ArrayList<>();

    public MockDRPCSpout(String function, DRPCOutputCollector collector) {
        super(function);
        this.collector = collector;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void fail(Object id) {
        failed.add(id);
    }

    @Override
    public void nextTuple() {
        Object id = messageIDs.poll();
        List<Object> tuple = tuples.poll();
        if (id != null) {
            collector.emit(null, tuple, id);
        }
    }

    public void addMessageParts(String id, String... contents) {
        int size = messageIDs.size();
        int index = size;
        for (String content : contents) {
            List<Object> tuple = makeTuple(makeMessage(id, content, index - size),
                                           makeReturnInfo("fake" + id, "testHost", index));
            tuples.offer(tuple);
            Object messageID = makeMessageID(id, index);
            messageIDs.offer(messageID);
            index++;
        }
    }

    public static List<Object> makeTuple(String pubSubMessage, String returnInfo) {
        return asList(pubSubMessage, returnInfo);
    }

    public static String makeReturnInfo(String drpcID, String host, int port) {
        return JSONFormatter.asJSON(zipToJSON(asList("id", "host", "port"), asList(drpcID, host, port)));
    }

    public static String makeMessage(String id, String content, int sequence) {
        return new PubSubMessage(id, content, sequence).asJSON();
    }
    public static Object makeMessageID(String id, int index) {
        return zipToJSON(asList("id", "index"), asList(id, index));
    }

    public static String zipToJSON(List<String> keys, List<Object> values) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keys.size(); ++i) {
            map.put(keys.get(i), values.get(i));
        }
        return JSONFormatter.asJSON(map);
    }
}
