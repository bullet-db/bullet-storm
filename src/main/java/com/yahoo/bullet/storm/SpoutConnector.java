/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.dsl.connector.BulletConnector;
import lombok.Setter;
import org.apache.storm.ICredentialsListener;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

import java.util.Map;

/**
 * This is a {@link BulletConnector} meant to compose an instance of a {@link IRichSpout} that can also optionally
 * implement the {@link ICredentialsListener} interface. The spout being composed must have a constructor that accepts a
 * {@link BulletConfig}. There are multiple ways to use this class but not all methods can be used in the same situation.
 * 1. This connector can be used in the DSL infrastructure if you wish to plug in an existing spout implementation that
 *    you already have but also wish to use a DSL converter (in a separate bolt or in the spout) and/or a DSL
 *    deserializer. In this case, you must implement the {@link #read()} method. The DSL spout will not invoke
 *    {@link #nextTuple()} and rely on the {@link #read()} to read and pass the read objects to the rest of the DSL
 *    infrastructure. It will not invoke the {@link #initialize()} method and instead call the {@link #activate()} and
 *    the {@link #open(Map, TopologyContext, SpoutOutputCollector)} in the appropriate methods in the spout.
 * 2. It can also be used as a {@link BulletConnector}. In this case, the {@link #initialize()} method will call
 *    {@link IRichSpout#open(Map, TopologyContext, SpoutOutputCollector)} and {@link IRichSpout#activate()} on the
 *    spout. You can use the {@code #setContext(TopologyContext)}, {@code #setOutputCollector(SpoutOutputCollector)} and
 *    {@code #setStormConfiguration(Map)} to pass in the arguments to the open. The {@link #close()} will invoke the
 *    {@link IRichSpout#deactivate()} method on the call. In this case, you must extend this class and override the
 *    {@link #read()} method to actually do the transfer of the data from the spout.
 * 3. This connector is itself a {@link IRichSpout} and can be used as one if needed. It would just behave as a proxy to
 *    the underlying spout in that case. The {@link ICredentialsListener} interface is also implemented if necessary.
 *    This usage is not really useful as you could have used the spout directly but it may be helpful to have a wrapper
 *    for your spout that works with the {@link BulletConfig}.
 *
 * The {@link #getSpout()} method is called on construction to create the composed spout. By default, it uses the
 * {@link BulletStormConfig#DSL_SPOUT_CONNECTOR_CLASS_NAME} from the {@link BulletConfig} and reflection to load the
 * spout. You may override it if you need a more elaborate creation mechanism.
 *
 * @param <T> The type of the spout being composed.
 */
public abstract class SpoutConnector<T extends IRichSpout> extends BulletConnector implements IRichSpout, ICredentialsListener {
    private static final long serialVersionUID = -4270291448244475213L;

    protected T spout;
    @Setter
    protected Map<String, Object> stormConfiguration;
    @Setter
    protected transient TopologyContext context;
    @Setter
    protected transient SpoutOutputCollector outputCollector;

    /**
     * Constructor that takes a {@link BulletConfig}.
     *
     * @param bulletConfig The {@link BulletConfig} to use.
     */
    public SpoutConnector(BulletConfig bulletConfig) {
        super(bulletConfig);
        spout = getSpout();
    }

    @Override
    public void initialize() {
        spout.open(stormConfiguration, context, outputCollector);
        activate();
    }

    @Override
    public void close() {
        deactivate();
    }

    @Override
    public void nextTuple() {
        spout.nextTuple();
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        stormConfiguration = map;
        context = topologyContext;
        outputCollector = spoutOutputCollector;
        spout.open(map, topologyContext, spoutOutputCollector);
    }

    @Override
    public void setCredentials(Map<String, String> map) {
        if (spout instanceof ICredentialsListener) {
            ((ICredentialsListener) spout).setCredentials(map);
        }
    }

    @Override
    public void activate() {
        spout.activate();
    }

    @Override
    public void deactivate() {
        spout.deactivate();
    }

    @Override
    public void ack(Object o) {
        spout.ack(o);
    }

    @Override
    public void fail(Object o) {
        spout.fail(o);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        spout.declareOutputFields(outputFieldsDeclarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return spout.getComponentConfiguration();
    }

    /**
     * Creates an instance of the composed spout. By default, uses the config to get the
     * {@link BulletStormConfig#DSL_SPOUT_CONNECTOR_CLASS_NAME} and reflection to initialize an instance of the spout.
     * The provided class must have a constructor that accepts a {@link BulletConfig}.
     *
     * @return The created spout.
     */
    protected T getSpout() {
        return this.config.loadConfiguredClass(BulletStormConfig.DSL_SPOUT_CONNECTOR_CLASS_NAME);
    }
}
