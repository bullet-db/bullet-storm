/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import lombok.Getter;

import java.util.Map;

@Getter
public class CustomSpoutDeclarer implements SpoutDeclarer {
    private String id;
    private IRichSpout spout;
    private Number parallelism;

    public CustomSpoutDeclarer(String id, IRichSpout spout, Number parallelism) {
        this.id = id;
        this.spout = spout;
        this.parallelism = parallelism;
    }

    // Unimplemented

    @Override
    public SpoutDeclarer addConfigurations(Map conf) {
        return null;
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
}
