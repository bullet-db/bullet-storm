/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * This class is copied from org.apache.storm.drpc.PrepareRequest but does not anchor. This exists so that acking
 * can be enabled for the topology but not influence DRPC tuple not being acked within the timeout. Since this
 * will not anchor the DRPC tuple, it will be immediately acked. The downside is that if the tuple is dropped,
 * there will no longer be a fast-fail mechanism but since bullet-storm depends on arbitrarily long DRPC based
 * queries, this is a compromise that has one solution.
 */
public class PrepareRequestBolt extends BaseRichBolt {
    public static final String ARGS_STREAM = Utils.DEFAULT_STREAM_ID;
    public static final String RETURN_STREAM = "ret";
    public static final String ID_STREAM = "id";

    public static final String REQUEST_FIELD = "request";
    public static final String RETURN_FIELD = "return";
    public static final String ARGS_FIELD = "args";

    private OutputCollector collector;

    Random rand;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        rand = new Random();
    }

    @Override
    public void execute(Tuple tuple) {
        String args = tuple.getString(0);
        String returnInfo = tuple.getString(1);
        long requestId = rand.nextLong();
        collector.emit(ARGS_STREAM, new Values(requestId, args));
        collector.emit(RETURN_STREAM, new Values(requestId, returnInfo));
        collector.emit(ID_STREAM, new Values(requestId));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(ARGS_STREAM, new Fields(REQUEST_FIELD, ARGS_FIELD));
        declarer.declareStream(RETURN_STREAM, new Fields(REQUEST_FIELD, RETURN_FIELD));
        declarer.declareStream(ID_STREAM, new Fields(REQUEST_FIELD));
    }
}
