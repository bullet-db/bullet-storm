/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.drpc;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class ComponentUtils {
    public static <T extends IRichBolt> T prepare(Map config, T bolt, CustomCollector collector) {
        TopologyContext mocked = mock(TopologyContext.class);
        bolt.prepare(config, mocked, new OutputCollector(collector));
        return bolt;
    }

    public static <T extends IRichBolt> T prepare(T bolt, CustomCollector collector) {
        return prepare(new HashMap<>(), bolt, collector);
    }

    public static <T extends IRichSpout> T open(Map config, T spout, CustomEmitter emitter) {
        TopologyContext mocked = mock(TopologyContext.class);
        spout.open(new HashMap<>(), mocked, new SpoutOutputCollector(emitter));
        return spout;
    }

    public static <T extends IRichSpout> T open(T spout, CustomEmitter emitter) {
        return open(new HashMap<>(), spout, emitter);
    }

}
