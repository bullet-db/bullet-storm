/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

import static com.yahoo.bullet.storm.TopologyConstants.ID_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.TICK_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.TICK_STREAM;

@Slf4j @Getter(AccessLevel.PACKAGE)
public class TickSpout extends ConfigComponent implements IRichSpout {
    private static final long serialVersionUID = 4448013633000246058L;

    protected transient SpoutOutputCollector collector;

    public static final long GRACEFUL_SLEEP = BulletStormConfig.TICK_INTERVAL_MINIMUM / 2;

    private final int tickInterval;
    private int id;
    private long tick = 0;
    @Setter(AccessLevel.PACKAGE)
    private long lastTickTime;

    /**
     * Creates an instance of this class with the given non-null config.
     *
     * @param config The non-null {@link BulletStormConfig} which is the config for this component.
     */
    public TickSpout(BulletStormConfig config) {
        super(config);
        tickInterval = config.getAs(BulletStormConfig.TICK_SPOUT_INTERVAL, Integer.class);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        tick = 0;
        lastTickTime = System.currentTimeMillis();
        id = new Random().nextInt();
    }

    @Override
    public void nextTuple() {
        long timeNow = System.currentTimeMillis();
        if (timeNow - lastTickTime < tickInterval) {
            Utils.sleep(GRACEFUL_SLEEP);
        } else {
            lastTickTime = timeNow;
            tick++;
            collector.emit(new Values(id, tick));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TICK_STREAM, new Fields(ID_FIELD, TICK_FIELD));
    }

    // Unused methods

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public void close() {
    }
}
