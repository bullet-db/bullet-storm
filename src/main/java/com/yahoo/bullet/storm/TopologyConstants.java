/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import backtype.storm.Constants;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.utils.Utils;

public class TopologyConstants {
    public static final String ID_FIELD = "request";
    public static final String RECORD_FIELD = "record";
    public static final String JOIN_FIELD = "result";
    public static final String RETURN_FIELD = "return-info";
    public static final int ID_POSITION = 0;
    public static final int RULE_POSITION = 1;
    public static final int RETURN_POSITION = 1;
    public static final int RECORD_POSITION = 1;

    public static final String RECORD_COMPONENT = "DataSource";
    public static final String TICK_COMPONENT = Constants.SYSTEM_COMPONENT_ID;
    public static final String DRPC_COMPONENT = DRPCSpout.class.getSimpleName();
    public static final String PREPARE_COMPONENT = PrepareRequestBolt.class.getSimpleName();
    public static final String FILTER_COMPONENT = FilterBolt.class.getSimpleName();
    public static final String JOIN_COMPONENT = JoinBolt.class.getSimpleName();
    public static final String RETURN_COMPONENT = ReturnResults.class.getSimpleName();
    public static final String RECORD_STREAM = Utils.DEFAULT_STREAM_ID;
    public static final String TICK_STREAM = Constants.SYSTEM_TICK_STREAM_ID;
    public static final String FILTER_STREAM = FilterBolt.FILTER_STREAM;
    public static final String JOIN_STREAM = JoinBolt.JOIN_STREAM;
    public static final String RETURN_STREAM = PrepareRequestBolt.RETURN_STREAM;
    public static final String ARGS_STREAM = PrepareRequestBolt.ARGS_STREAM;
    public static final String ID_STREAM = PrepareRequestBolt.ID_STREAM;

    public static final String METRIC_PREFIX = "bullet_";
}
