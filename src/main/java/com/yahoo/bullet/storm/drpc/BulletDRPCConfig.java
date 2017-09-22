/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.storm.BulletStormConfig;

import java.io.IOException;

public class BulletDRPCConfig extends BulletStormConfig {
    // The next three parameters must be tuned depending on the volume of requests and the distribution of the duration
    // of queries for the best performance. If the thread pool or queues are full, DRPC PubSub sends will block.
    public static final String DRPC_THREAD_POOL_SIZE = "bullet.pubsub.drpc.thread.pool.size";
    public static final String DRPC_REQUEST_QUEUE_SIZE = "bullet.pubsub.drpc.request.queue.size";
    public static final String DRPC_RESPONSE_QUEUE_SIZE = "bullet.pubsub.drpc.response.queue.size";
    // The timeout and retry limits for connections to DRPC servers.
    public static final String DRPC_CONNECT_TIMEOUT = "bullet.pubsub.drpc.connect.timeout";
    public static final String DRPC_CONNECT_RETRY_LIMIT = "bullet.pubsub.drpc.connect.retry.limit";
    // The path that queries must be POSTed to.
    public static final String DRPC_PATH = "bullet.pubsub.drpc.path";
    // The index of the current instance of
    public static final String DRPC_INSTANCE_INDEX = "bullet.pubsub.drpc.index";
    // This is the port that the QUERY_SUBMISSION end talks to. This overrides a variable with the same name and setting
    // from Config.
    public static final String DRPC_HTTP_PORT = "drpc.http.port";
    // This is the port that the QUERY_PROCESSING end talks to. This overrides a variable with the same name and setting
    // from Config.
    public static final String DRPC_INVOCATIONS_PORT = "drpc.invocations.port";
    // The location of DRPC servers.
    public static final String DRPC_SERVERS = "drpc.servers";
    // The interval at which request are fetched from the DRPC server.
    public static final String DRPC_REQUEST_FETCH_INTERVAL_MS = "bullet.pubsub.drpc.request.fetch.ms";

    public static final String DEFAULT_DRPC_CONFIGURATION = "bullet_storm_defaults.yaml";

    /**
     * Create a new BulletDRPCConfig by reading in a file.
     *
     * @param file The file containing DRPC settings.
     * @throws IOException if the input file is malformed or DRPC defaults could not be loaded.
     */
    public BulletDRPCConfig(String file) throws IOException {
        // Load and merge with default bullet-storm settings. Storm defaults also contain the DRPC settings.
        super(file);
    }
}
