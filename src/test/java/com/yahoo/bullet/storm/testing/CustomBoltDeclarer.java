/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.SharedMemory;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class CustomBoltDeclarer implements BoltDeclarer {
    private Number offHeap;
    private Number onHeap;
    private Number cpuLoad;

    private String id;
    private IRichBolt bolt;
    private Number parallelism;

    private Map<Pair<String, String>, List<Fields>> fieldsGroupings = new HashMap<>();
    private Map<Pair<String, String>, CustomStreamGrouping> customGroupings = new HashMap<>();
    private List<Pair<String, String>> allGroupings = new ArrayList<>();
    private List<Pair<String, String>> shuffleGroupings = new ArrayList<>();
    private List<Pair<String, String>> directGroupings = new ArrayList<>();


    public CustomBoltDeclarer(String id, IRichBolt bolt, Number parallelism) {
        this.id = id;
        this.bolt = bolt;
        this.parallelism = parallelism;
    }

    @Override
    public BoltDeclarer setMemoryLoad(Number onHeap, Number offHeap) {
        this.onHeap = onHeap;
        this.offHeap = offHeap;
        return this;
    }

    @Override
    public BoltDeclarer setCPULoad(Number amount) {
        this.cpuLoad = amount;
        return this;
    }

    @Override
    public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
        Pair<String, String> key = ImmutablePair.of(componentId, streamId);
        List<Fields> existing = fieldsGroupings.get(key);
        if (existing == null) {
            existing = new ArrayList<>();
        }
        existing.add(fields);
        fieldsGroupings.put(key, existing);
        return this;
    }

    @Override
    public BoltDeclarer customGrouping(String componentId, String streamId, CustomStreamGrouping grouping) {
        customGroupings.put(ImmutablePair.of(componentId, streamId), grouping);
        return this;
    }

    @Override
    public BoltDeclarer allGrouping(String componentId, String streamId) {
        allGroupings.add(ImmutablePair.of(componentId, streamId));
        return this;
    }

    @Override
    public BoltDeclarer shuffleGrouping(String componentId) {
        shuffleGroupings.add(ImmutablePair.of(componentId, Utils.DEFAULT_STREAM_ID));
        return this;
    }

    @Override
    public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
        shuffleGroupings.add(ImmutablePair.of(componentId, streamId));
        return this;
    }

    @Override
    public BoltDeclarer directGrouping(String componentId, String streamId) {
        directGroupings.add(ImmutablePair.of(componentId, streamId));
        return this;
    }

    // Unimplemented

    @Override
    public BoltDeclarer addConfigurations(Map<String, Object> conf) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer addConfiguration(String config, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer setDebug(boolean debug) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer setMaxTaskParallelism(Number val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer setMaxSpoutPending(Number val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer setNumTasks(Number val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer globalGrouping(String componentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer globalGrouping(String componentId, String streamId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer localOrShuffleGrouping(String componentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer localOrShuffleGrouping(String componentId, String streamId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer noneGrouping(String componentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer noneGrouping(String componentId, String streamId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer allGrouping(String componentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer directGrouping(String componentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer partialKeyGrouping(String componentId, Fields fields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer partialKeyGrouping(String componentId, String streamId, Fields fields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer customGrouping(String componentId, CustomStreamGrouping grouping) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer grouping(GlobalStreamId id, Grouping grouping) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer setMemoryLoad(Number onHeap) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer addSharedMemory(SharedMemory request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer addResources(Map<String, Double> resources) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoltDeclarer addResource(String resourceName, Number resourceValue) {
        throw new UnsupportedOperationException();
    }
}
