/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.storm.drpc.BulletDRPCConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.drpc.DRPCInvocationsClient;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DistributedRPCInvocations;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This code is copied over from ReturnResults.java (org.apache.storm.drpc.ReturnResults) from the Apache Storm
 * project and modified. It writes responses to a set of DRPC servers.
 */
@Slf4j @Getter
public class DRPCResponseHandler {
    private Map<List, DRPCInvocationsClient> clients = new HashMap<>();
    private DRPCRequestHandler drpcRequestHandler;
    private Map clientConfiguration;
    private int maxRetries = 1;

    /**
     * Create a DRPCResponsePublisher from a {@link BulletConfig}.
     *
     * @param drpcRequestHandler The {@link DRPCRequestHandler} used to fail requests.
     * @param clientConfiguration The BulletConfig containing all the settings required for a client to connect to the
     *                            DRPC server. See {@link DRPCInvocationsClient} for more details.
     */
    public DRPCResponseHandler(DRPCRequestHandler drpcRequestHandler, BulletConfig clientConfiguration) {
        this.drpcRequestHandler = drpcRequestHandler;
        this.clientConfiguration = clientConfiguration.getAll(Optional.empty());
        maxRetries = Utils.getInt(clientConfiguration.getOrDefault(BulletDRPCConfig.DRPC_CONNECT_RETRY_LIMIT, 1));
    }

    /**
     * Update the result for a query on the DRPC server.
     *
     * @param messageID The ID object corresponding to the query to be updated.
     * @param result The content of the response to the query.
     * @param retMap The {@link Map} containing the return information required.
     */
    public void result(Object messageID, String result, Map retMap) {
        DistributedRPCInvocations.Iface client;
        try {
            client = getInvocationsClient(retMap);
        } catch (Exception e) {
            fail(messageID);
            return;
        }
        String id = (String) retMap.get("id");
        int retryCnt = 0;
        while (retryCnt < maxRetries) {
            retryCnt++;
            try {
                client.result(id, result);
                break;
            } catch (AuthorizationException aze) {
                log.error("Not authorized to return results to DRPC server", aze);
                fail(messageID);
                throw new RuntimeException(aze);
            } catch (TException tex) {
                if (retryCnt >= maxRetries) {
                    log.error("Failed to return results to DRPC server", tex);
                    fail(messageID);
                    return;
                }
                reconnectClient((DRPCInvocationsClient) client);
            }
        }
    }

    /**
     * Mark the processing of a query as a failure.
     *
     * @param messageID The ID of the query to be marked as a failure.
     */
    public void fail(Object messageID) {
        drpcRequestHandler.fail(messageID);
    }

    /**
     * Close the DRPCResponseHandler by closing all {@link DRPCInvocationsClient} objects it owns.
     */
    public void close() {
        for (DRPCInvocationsClient c: clients.values()) {
            c.close();
        }
    }

    void reconnectClient(DRPCInvocationsClient client) {
        if (client instanceof DRPCInvocationsClient) {
            try {
                log.info("reconnecting... ");
                client.reconnectClient(); //Blocking call
            } catch (TException e2) {
                log.error("Failed to connect to DRPC server", e2);
            }
        }
    }

    DRPCInvocationsClient getInvocationsClient(Map retMap) {
        String host = (String) retMap.get("host");
        int port = Utils.getInt(retMap.get("port"));
        List server = new ArrayList() {
            {
                add(host);
                add(port);
            }
        };

        if (!clients.containsKey(server)) {
            try {
                clients.put(server, new DRPCInvocationsClient(clientConfiguration, host, port));
            } catch (TTransportException ex) {
                throw new RuntimeException(ex);
            }
        }
        return clients.get(server);
    }
}
