/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.converter.BulletRecordConverter;
import com.yahoo.bullet.dsl.deserializer.BulletDeserializer;
import com.yahoo.bullet.dsl.deserializer.IdentityDeserializer;
import com.yahoo.bullet.record.BulletRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class DSLBolt extends ConfigComponent implements IRichBolt {
    private static final long serialVersionUID = -5431511003546624594L;

    private OutputCollector collector;
    private BulletRecordConverter converter;
    private BulletDeserializer deserializer;

    /**
     * Creates a DSLBolt with a given {@link BulletStormConfig}.
     *
     * @param bulletStormConfig The non-null BulletStormConfig to use. It should contain the settings to initialize a BulletRecordConverter.
     */
    public DSLBolt(BulletStormConfig bulletStormConfig) {
        super(bulletStormConfig);
        BulletDSLConfig config = new BulletDSLConfig(bulletStormConfig);
        converter = BulletRecordConverter.from(config);
        boolean dslDeserializerEnable = config.getAs(BulletStormConfig.DSL_DESERIALIZER_ENABLE, Boolean.class);
        deserializer = dslDeserializerEnable ? BulletDeserializer.from(config) : new IdentityDeserializer(config);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        List<Object> objects = (List<Object>) tuple.getValue(TopologyConstants.RECORD_POSITION);
        objects.forEach(this::convertAndEmit);
        collector.ack(tuple);
    }

    private void convertAndEmit(Object object) {
        BulletRecord record;
        try {
            record = converter.convert(deserializer.deserialize(object));
        } catch (BulletDSLException e) {
            log.error("Could not convert object.", e);
            return;
        }
        collector.emit(new Values(record, System.currentTimeMillis()));
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TopologyConstants.RECORD_FIELD, TopologyConstants.RECORD_TIMESTAMP_FIELD));
    }
}
