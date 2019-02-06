/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.connector.BulletConnector;
import com.yahoo.bullet.dsl.converter.BulletRecordConverter;
import com.yahoo.bullet.dsl.deserializer.BulletDeserializer;
import com.yahoo.bullet.record.BulletRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class DSLSpout extends ConfigComponent implements IRichSpout {
    private static final long serialVersionUID = 9218045272408135524L;
    private static final Long DUMMY_ID = 42L;

    private SpoutOutputCollector collector;
    private BulletConnector connector;
    private BulletRecordConverter converter;
    private BulletDeserializer deserializer;
    private boolean dslBoltEnable;

    /**
     * Creates a DSLSpout with a given {@link BulletStormConfig}.
     *
     * @param bulletStormConfig The non-null BulletStormConfig to use. It should contain the settings to initialize a BulletConnector and a BulletRecordConverter.
     */
    public DSLSpout(BulletStormConfig bulletStormConfig) {
        super(bulletStormConfig);
        BulletDSLConfig config = new BulletDSLConfig(bulletStormConfig);
        connector = BulletConnector.from(config);
        dslBoltEnable = config.getAs(BulletStormConfig.DSL_BOLT_ENABLE, Boolean.class);
        if (!dslBoltEnable) {
            boolean dslDeserializerEnable = config.getAs(BulletStormConfig.DSL_DESERIALIZER_ENABLE, Boolean.class);
            converter = BulletRecordConverter.from(config);
            deserializer = dslDeserializerEnable ? BulletDeserializer.from(config) : new IdentityDeserializer(config);
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            connector.initialize();
        } catch (BulletDSLException e) {
            throw new RuntimeException("Could not open DSLSpout.", e);
        }
    }

    @Override
    public void activate() {
        log.info("DSLSpout activated");
    }

    @Override
    public void deactivate() {
        log.info("DSLSpout deactivated");
    }

    @Override
    public void nextTuple() {
        List<Object> objects = readObjects();
        if (objects.isEmpty()) {
            return;
        }
        if (dslBoltEnable) {
            collector.emit(new Values(objects, System.currentTimeMillis()), DUMMY_ID);
            return;
        }
        objects.forEach(this::convertAndEmit);
    }

    private List<Object> readObjects() {
        try {
            return connector.read().stream().filter(Objects::nonNull).collect(Collectors.toList());
        } catch (BulletDSLException e) {
            log.error("Could not read from BulletConnector.", e);
        }
        return Collections.emptyList();
    }

    private void convertAndEmit(Object object) {
        BulletRecord record;
        try {
            record = converter.convert(deserializer.deserialize(object));
        } catch (BulletDSLException e) {
            log.error("Could not convert object.", e);
            return;
        }
        collector.emit(new Values(record, System.currentTimeMillis()), DUMMY_ID);
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
