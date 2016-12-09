/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.drpc;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

public class CustomOutputFieldsDeclarer implements OutputFieldsDeclarer {
    private Map<String, Fields> captured = new HashMap<>();
    private Map<String, Boolean> directMap = new HashMap<>();

    private void addAll(String stream, Fields fields, boolean direct) {
        captured.put(stream, fields);
        directMap.put(stream, direct);
    }

    public boolean areFieldsPresent(Fields fields) {
        return captured.values().stream().anyMatch(f -> f.toList().equals(fields.toList()));
    }

    public boolean areFieldsPresent(String stream, Fields fields) {
        return captured.containsKey(stream) && captured.get(stream).toList().equals(fields.toList());
    }

    public boolean areFieldsPresent(String stream, boolean direct, Fields fields) {
        return areFieldsPresent(stream, fields) && directMap.get(stream) == direct;
    }

    @Override
    public void declare(Fields fields) {
        addAll(Utils.DEFAULT_STREAM_ID, fields, false);
    }

    @Override
    public void declare(boolean direct, Fields fields) {
        addAll(Utils.DEFAULT_STREAM_ID, fields, direct);
    }

    @Override
    public void declareStream(String streamId, Fields fields) {
        addAll(streamId, fields, false);
    }

    @Override
    public void declareStream(String streamId, boolean direct, Fields fields) {
        addAll(streamId, fields, false);
    }
}
