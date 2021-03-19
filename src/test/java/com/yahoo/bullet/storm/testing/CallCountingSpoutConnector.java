/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.storm.SpoutConnector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CallCountingSpoutConnector extends SpoutConnector<CallCountingSpout> {
    private static final long serialVersionUID = 5538368781947223090L;

    public CallCountingSpoutConnector(BulletConfig bulletConfig) {
        super(bulletConfig);
    }

    @Override
    public List<Object> read() {
        return Collections.emptyList();
    }

    public CallCountingSpout getProxy() {
        return this.spout;
    }

    public Map<String, Object> getOpenMap() {
        return this.stormConfiguration;
    }

    public TopologyContext getOpenContext() {
        return this.context;
    }

    public SpoutOutputCollector getOpenCollector() {
        return this.outputCollector;
    }
}
