/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.Config;
import com.yahoo.bullet.storm.BulletStormConfig;

public class DRPCConfig extends BulletStormConfig {
    public static final String PREFIX = "bullet.pubsub.storm.drpc.";

    // The location of DRPC servers.
    public static final String DRPC_SERVERS = PREFIX + "servers";

    // The timeout and retry limits for HTTP connections to DRPC servers.
    public static final String DRPC_CONNECT_TIMEOUT = PREFIX + "connect.timeout";
    public static final String DRPC_CONNECT_RETRY_LIMIT = PREFIX + "connect.retry.limit";

    // This is the HTTP protocol to use when submitting to the DRPC server.
    public static final String DRPC_HTTP_PROTOCOL = PREFIX + "http.protocol";
    // This is the port that the QUERY_SUBMISSION end talks to.
    public static final String DRPC_HTTP_PORT = PREFIX + "http.port";
    // The path that queries must be POSTed to. This generally is "drpc"
    public static final String DRPC_HTTP_PATH = PREFIX + "http.path";

    // This is the name of the DRPC function used to register with the DRPC servers
    public static final String DRPC_FUNCTION = PREFIX + "function";

    // This is the maximum number of pending queries that can be read by a single subscriber in QUERY_PROCESSING
    // before a commit is needed.
    public static final String DRPC_MAX_UNCOMMITED_MESSAGES = PREFIX + "max.uncommitted.messages";

    /**
     * Create a new DRPCConfig by reading in a file.
     *
     * @param file The file containing DRPC settings.
     */
    public DRPCConfig(String file) {
        // Load and merge with default bullet-storm settings. Storm defaults also contain the DRPC settings.
        this(new BulletStormConfig(file));
    }

    /**
     * Creates a new DRPCConfig wrapping the given config.
     *
     * @param config The config to wrap.
     */
    public DRPCConfig(Config config) {
        super(config);
    }
}
