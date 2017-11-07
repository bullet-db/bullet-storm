/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import org.apache.storm.Constants;
import org.apache.storm.utils.Utils;

public class TopologyConstants {
    public static final String ID_FIELD = QuerySpout.ID_FIELD;
    public static final String QUERY_FIELD = QuerySpout.QUERY_FIELD;
    public static final String METADATA_FIELD = QuerySpout.METADATA_FIELD;
    public static final String JOIN_FIELD = JoinBolt.JOIN_FIELD;
    public static final String RECORD_FIELD = "record";

    public static final int ID_POSITION = 0;
    public static final int QUERY_POSITION = 1;
    public static final int METADATA_POSITION = 2;
    public static final int RECORD_POSITION = 1;

    public static final String RECORD_COMPONENT = "DataSource";
    public static final String TICK_COMPONENT = Constants.SYSTEM_COMPONENT_ID;
    public static final String QUERY_COMPONENT = QuerySpout.class.getSimpleName();
    public static final String FILTER_COMPONENT = FilterBolt.class.getSimpleName();
    public static final String JOIN_COMPONENT = JoinBolt.class.getSimpleName();
    public static final String RESULT_COMPONENT = ResultBolt.class.getSimpleName();

    public static final String RECORD_STREAM = Utils.DEFAULT_STREAM_ID;
    public static final String TICK_STREAM = Constants.SYSTEM_TICK_STREAM_ID;
    public static final String FILTER_STREAM = FilterBolt.FILTER_STREAM;
    public static final String JOIN_STREAM = JoinBolt.JOIN_STREAM;
    public static final String QUERY_STREAM = QuerySpout.QUERY_STREAM;

    public static final String METRIC_PREFIX = "bullet_";
}
