/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import org.apache.storm.utils.Utils;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

public class TopologyConstants {
    public static final String ID_FIELD = "id";
    public static final String QUERY_FIELD = "query";
    public static final String METADATA_FIELD = "metadata";
    public static final String TICK_FIELD = "tick";
    public static final String DATA_FIELD = "data";
    public static final String ERROR_FIELD = "error";
    public static final String JOIN_FIELD = "result";
    public static final String RECORD_FIELD = "record";

    public static final int ID_POSITION = 0;
    public static final int QUERY_POSITION = 1;
    public static final int QUERY_METADATA_POSITION = 2;
    public static final int ERROR_POSITION = 1;
    public static final int METADATA_POSITION = 1;
    public static final int RECORD_POSITION = 0;
    public static final int RECORD_TIMESTAMP_POSITION = 1;
    public static final int DATA_POSITION = 1;

    // This is the default name.
    public static final String RECORD_COMPONENT = "DataSource";
    public static final String TICK_COMPONENT = TickSpout.class.getSimpleName();
    public static final String QUERY_COMPONENT = QuerySpout.class.getSimpleName();
    public static final String FILTER_COMPONENT = FilterBolt.class.getSimpleName();
    public static final String JOIN_COMPONENT = JoinBolt.class.getSimpleName();
    public static final String RESULT_COMPONENT = ResultBolt.class.getSimpleName();

    public static final String RECORD_STREAM = Utils.DEFAULT_STREAM_ID;
    public static final String TICK_STREAM = Utils.DEFAULT_STREAM_ID;
    public static final String FILTER_STREAM = Utils.DEFAULT_STREAM_ID;
    public static final String ERROR_STREAM = "error";
    public static final String JOIN_STREAM = Utils.DEFAULT_STREAM_ID;
    public static final String QUERY_STREAM = Utils.DEFAULT_STREAM_ID;
    public static final String META_STREAM = "meta";

    public static final String METRIC_PREFIX = "bullet_";
    public static final String ACTIVE_QUERIES_METRIC = METRIC_PREFIX + "active_queries";
    public static final String CREATED_QUERIES_METRIC = METRIC_PREFIX + "created_queries";
    public static final String IMPROPER_QUERIES_METRIC = METRIC_PREFIX + "improper_queries";
    public static final String RATE_EXCEEDED_QUERIES_METRIC = METRIC_PREFIX + "rate_exceeded_queries";
    public static final String LATENCY_METRIC = METRIC_PREFIX + "filter_latency";
    public static final String DEFAULT_METRIC = "default";
    public static final Set<String> BUILT_IN_METRICS =
        new HashSet<>(asList(ACTIVE_QUERIES_METRIC, CREATED_QUERIES_METRIC, IMPROPER_QUERIES_METRIC,
                             RATE_EXCEEDED_QUERIES_METRIC, LATENCY_METRIC, DEFAULT_METRIC));
}
