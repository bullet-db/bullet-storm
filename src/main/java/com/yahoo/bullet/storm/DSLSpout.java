/*
 *  Copyright 2019, Verizon Media.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.connector.BulletConnector;
import com.yahoo.bullet.dsl.converter.BulletRecordConverter;
import com.yahoo.bullet.record.BulletRecord;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class DSLSpout extends ConfigComponent implements IRichSpout {
    private static final long serialVersionUID = 9218045272408135524L;
    private static final Long DUMMY_ID = 42L;

    private SpoutOutputCollector collector;
    private TopologyContext context;
    private BulletDSLConfig bulletDSLConfig;

    // Exposed for testing only
    @Getter(AccessLevel.PACKAGE)
    private BulletConnector connector;

    @Getter(AccessLevel.PACKAGE)
    private BulletRecordConverter converter;

    /**
     * Creates a DSLSpout with a {@link BulletStormConfig} constructed from a configuration file. This constructor is
     * used in the {@link Topology} class.
     *
     * @param args A list of arguments where the first argument is expected to be the path to a configuration file.
     */
    public DSLSpout(List<String> args) {
        this(new BulletStormConfig(args.get(0)));
    }

    /**
     * Creates a DSLSpout with a given {@link BulletStormConfig}.
     *
     * @param config The non-null BulletStormConfig to use. It should contain the settings to initialize a BulletConnector and a BulletRecordConverter.
     */
    public DSLSpout(BulletStormConfig config) {
        super(config);
        bulletDSLConfig = new BulletDSLConfig(config);
        connector = BulletConnector.from(bulletDSLConfig);
        converter = BulletRecordConverter.from(bulletDSLConfig);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.context = context;
    }

    @Override
    public void activate() {
        try {
            connector.initialize();
        } catch (BulletDSLException e) {
            throw new RuntimeException("Could not activate DSLSpout.", e);
        }
        log.info("DSLSpout activated");
    }

    @Override
    public void deactivate() {
        try {
            connector.close();
        } catch (Exception e) {
            log.error("Could not close BulletConnector.", e);
        }
        log.info("DSLSpout deactivated");
    }

    @Override
    public void nextTuple() {
        List<Object> objects = Collections.emptyList();
        try {
            objects = connector.read();
        } catch (BulletDSLException e) {
            log.error("Could not read from BulletConnector.", e);
        }
        long time = System.currentTimeMillis();
        for (Object object : objects) {
            try {
                if (object != null) {
                    BulletRecord record = converter.convert(object);
                    collector.emit(new Values(record, time), DUMMY_ID);
                }
            } catch (BulletDSLException e) {
                log.error("Could not convert object.", e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TopologyConstants.RECORD_FIELD, TopologyConstants.RECORD_TIMESTAMP_FIELD));
    }

    @Override
    public void ack(Object id) {

    }

    @Override
    public void fail(Object id) {

    }

    @Override
    public void close() {
        try {
            connector.close();
        } catch (Exception e) {
            log.error("Could not close BulletConnector.", e);
        }
        log.info("DSLSpout closed");
    }
}
