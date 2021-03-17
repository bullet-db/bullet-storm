/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import com.yahoo.bullet.common.BulletConfig;
import lombok.Getter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

import java.util.Map;

@Getter
public class CallCountingSpout implements IRichSpout {
    private static final long serialVersionUID = 4354648865963509383L;

    private int credentialCalls = 0;
    private int openCalls = 0;
    private int closeCalls = 0;
    private int activateCalls = 0;
    private int deactivateCalls = 0;
    private int nextTupleCalls = 0;
    private int ackCalls = 0;
    private int failCalls = 0;
    private int declareCalls = 0;
    private int configurationCalls = 0;

    public CallCountingSpout (BulletConfig config) {
    }

    public void setCredentials(Map<String, String> map) {
        credentialCalls++;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        openCalls++;
    }

    @Override
    public void close() {
        closeCalls++;
    }

    @Override
    public void activate() {
        activateCalls++;
    }

    @Override
    public void deactivate() {
        deactivateCalls++;
    }

    @Override
    public void nextTuple() {
        nextTupleCalls++;
    }

    @Override
    public void ack(Object o) {
        ackCalls++;
    }

    @Override
    public void fail(Object o) {
        failCalls++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        declareCalls++;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        configurationCalls++;
        return null;
    }
}
