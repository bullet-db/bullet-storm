/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class ComponentUtils {
    public static <T extends IRichBolt> T prepare(Map config, T bolt, TopologyContext context, IOutputCollector collector) {
        bolt.prepare(config, context, new OutputCollector(collector));
        return bolt;
    }

    public static <T extends IRichSpout> T open(Map config, T spout, TopologyContext context, ISpoutOutputCollector emitter) {
        spout.open(config, context, new SpoutOutputCollector(emitter));
        return spout;
    }

    public static <T extends IRichBolt> T prepare(Map config, T bolt, CustomCollector collector) {
        return prepare(config, bolt, mock(TopologyContext.class), collector);
    }

    public static <T extends IRichBolt> T prepare(T bolt, CustomCollector collector) {
        return prepare(new HashMap<>(), bolt, mock(TopologyContext.class), collector);
    }

    public static <T extends IRichSpout> T open(Map config, T spout, CustomEmitter emitter) {
        return open(config, spout, mock(TopologyContext.class), emitter);
    }

    public static <T extends IRichSpout> T open(T spout, CustomEmitter emitter) {
        return open(new HashMap<>(), spout, emitter);
    }

}
