/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.converter.BulletRecordConverter;
import com.yahoo.bullet.record.BulletRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

@Slf4j
public class DSLBolt extends ConfigComponent implements IRichBolt {
    private static final long serialVersionUID = -5431511003546624594L;

    private OutputCollector collector;
    private BulletDSLConfig dslConfig;
    private BulletRecordConverter converter;

    /**
     *
     * @param config
     */
    public DSLBolt(BulletStormConfig config) {
        super(config);
        dslConfig = new BulletDSLConfig(config);
        converter = BulletRecordConverter.from(dslConfig);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        List<Object> objects = (List<Object>) tuple.getValue(TopologyConstants.RECORD_POSITION);
        objects.forEach(
            object -> {
                BulletRecord record;
                try {
                    record = converter.convert(object);
                } catch (BulletDSLException e) {
                    log.error("Could not convert object.", e);
                    return;
                }
                collector.emit(new Values(record, System.currentTimeMillis()));
            }
        );
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TopologyConstants.RECORD_FIELD, TopologyConstants.RECORD_TIMESTAMP_FIELD));
    }
}
