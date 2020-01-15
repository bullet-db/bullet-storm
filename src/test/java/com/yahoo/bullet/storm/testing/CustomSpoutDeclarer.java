/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import lombok.Getter;
import org.apache.storm.generated.SharedMemory;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.SpoutDeclarer;

import java.util.Map;

@Getter
public class CustomSpoutDeclarer implements SpoutDeclarer {
    private Number cpuLoad;
    private Number offHeap;
    private Number onHeap;
    private String id;
    private IRichSpout spout;
    private Number parallelism;

    public CustomSpoutDeclarer(String id, IRichSpout spout, Number parallelism) {
        this.id = id;
        this.spout = spout;
        this.parallelism = parallelism;
    }

    @Override
    public SpoutDeclarer setMemoryLoad(Number onHeap, Number offHeap) {
        this.onHeap = onHeap;
        this.offHeap = offHeap;
        return this;
    }

    @Override
    public SpoutDeclarer setCPULoad(Number amount) {
        this.cpuLoad = amount;
        return this;
    }


    // Unimplemented
    @Override
    public SpoutDeclarer addConfigurations(Map<String, Object> conf) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpoutDeclarer addConfiguration(String config, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpoutDeclarer setDebug(boolean debug) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpoutDeclarer setMaxTaskParallelism(Number val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpoutDeclarer setMaxSpoutPending(Number val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpoutDeclarer setNumTasks(Number val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpoutDeclarer setMemoryLoad(Number onHeap) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpoutDeclarer addSharedMemory(SharedMemory request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpoutDeclarer addResources(Map<String, Double> resources) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpoutDeclarer addResource(String resourceName, Number resourceValue) {
        throw new UnsupportedOperationException();
    }
}
