/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import org.apache.storm.ICredentialsListener;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

/**
 * This class exists so that users can use the {@link SpoutConnector} as the DSLSpout itself. Users are expected to do
 * this if they wish to use an existing spout to read their data but still use other DSL components like a deserializer
 * or a converter. This class will proxy all calls for the spout interface including credentials (optionally) to the
 * {@link SpoutConnector} instance. It will not call the {@link SpoutConnector#initialize()} or
 * {@link SpoutConnector#close()} methods. It will use the spout interfaces except for {@code nextTuple()} and the
 * {@link SpoutConnector#read()}.
 */
public class DSLConnectorSpout extends DSLSpout<SpoutConnector> implements ICredentialsListener {
    private static final long serialVersionUID = -6238096209128244464L;

    /**
     * Creates a DSLConnectorSpout with a given {@link BulletStormConfig}.
     *
     * @param bulletStormConfig The non-null BulletStormConfig to use. It should contain the settings to initialize
     *                          a {@link SpoutConnector} and a BulletRecordConverter.
     */
    public DSLConnectorSpout(BulletStormConfig bulletStormConfig) {
        super(bulletStormConfig);
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        connector.open(conf, context, collector);
    }

    @Override
    public void activate() {
        super.activate();
        connector.activate();
    }

    @Override
    public void deactivate() {
        super.deactivate();
        connector.deactivate();
    }

    @Override
    public void ack(Object id) {
        connector.ack(id);
    }

    @Override
    public void fail(Object id) {
        connector.fail(id);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return connector.getComponentConfiguration();
    }

    @Override
    public void setCredentials(Map<String, String> credentials) {
        connector.setCredentials(credentials);
    }
}
