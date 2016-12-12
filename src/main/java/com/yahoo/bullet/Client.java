/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet;

import lombok.extern.slf4j.Slf4j;
import backtype.storm.utils.DRPCClient;
import backtype.storm.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Random;

@Slf4j
public class Client {
    public static final String DRPC_SERVERS_KEY = "drpc.servers";
    public static final String DRPC_PORT_KEY = "drpc.port";

    private static String pickRandomlyFrom(List<String> strings) {
        if (strings == null && strings.isEmpty()) {
            return null;
        }
        Random random = new Random(System.currentTimeMillis());
        return strings.get(random.nextInt(strings.size()));
    }

    /**
     * Main. Makes a request to the DRPC topology.
     *
     * @param args Input arguments.
     * @throws Exception if any.
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            log.error("Usage: Requires 2 arguments: DRPC function name and the function argument");
            return;
        }
        // Till https://issues.apache.org/jira/browse/STORM-440 is resolved, need to load config.
        Map config = Utils.readStormConfig();
        List<String> drpcServers = (List<String>) config.get(DRPC_SERVERS_KEY);
        String drpcServer = pickRandomlyFrom(drpcServers);
        int drpcPort = (Integer) config.get(DRPC_PORT_KEY);
        log.info("Using Server: {}, Port: {}", drpcServer, drpcPort);

        DRPCClient client = new DRPCClient(config, drpcServer, drpcPort);
        String output = client.execute(args[0], args[1]);
        log.info("Received output:\n{}", output);
    }
}
