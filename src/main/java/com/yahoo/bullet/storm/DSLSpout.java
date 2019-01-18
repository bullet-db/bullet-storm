/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.connector.BulletConnector;
import com.yahoo.bullet.dsl.converter.BulletRecordConverter;
import com.yahoo.bullet.record.BulletRecord;
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
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class DSLSpout extends ConfigComponent implements IRichSpout {
    private static final long serialVersionUID = 9218045272408135524L;
    private static final Long DUMMY_ID = 42L;

    private SpoutOutputCollector collector;
    private BulletDSLConfig dslConfig;
    private BulletConnector connector;
    private BulletRecordConverter converter;
    private boolean dslBoltEnable;

    /**
     * Creates a DSLSpout with a given {@link BulletStormConfig}.
     *
     * @param config The non-null BulletStormConfig to use. It should contain the settings to initialize a BulletConnector and a BulletRecordConverter.
     */
    public DSLSpout(BulletStormConfig config) {
        super(config);
        dslConfig = new BulletDSLConfig(config);
        connector = BulletConnector.from(dslConfig);
        dslBoltEnable = config.getAs(BulletStormConfig.DSL_BOLT_ENABLE, Boolean.class);
        if (!dslBoltEnable) {
            converter = BulletRecordConverter.from(dslConfig);
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
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
        List<Object> objects = readObjects();
        if (objects.isEmpty()) {
            return;
        }
        if (dslBoltEnable) {
            collector.emit(new Values(objects, System.currentTimeMillis()), DUMMY_ID);
            return;
        }
        objects.forEach(
            object -> {
                BulletRecord record;
                try {
                    record = converter.convert(object);
                } catch (BulletDSLException e) {
                    log.error("Could not convert object.", e);
                    return;
                }
                collector.emit(new Values(record, System.currentTimeMillis()), DUMMY_ID);
            }
        );
    }

    private List<Object> readObjects() {
        try {
            return connector.read().stream().filter(Objects::nonNull).collect(Collectors.toList());
        } catch (BulletDSLException e) {
            log.error("Could not read from BulletConnector.", e);
        }
        return Collections.emptyList();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Keep this the same even if DSL bolt is enabled?
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
