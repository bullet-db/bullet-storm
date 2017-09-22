/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import com.google.gson.Gson;
import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.storm.BulletStormConfig;
import com.yahoo.bullet.storm.drpc.BulletDRPCConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.drpc.DRPCInvocationsClient;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.generated.DistributedRPCInvocations;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.ExtendedThreadPoolExecutor;
import org.apache.storm.utils.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * This code is copied over from DRPCSpout.java (org.apache.storm.drpc.DRPCSpout) from the Apache Storm project
 * and modified. It reads pending requests from a set of DRPC servers.
 */
@Slf4j @Getter
public class DRPCRequestHandler {
    private List<DRPCInvocationsClient> clients = new ArrayList<>();
    private transient LinkedList<Future<Void>> futures = null;
    private transient ExecutorService backround = null;
    private String function;
    private BlockingQueue<PubSubMessage> requests;
    private boolean isClosed;
    private static final Gson GSON = new Gson();

    @Getter
    private static class DRPCMessageId  implements Serializable {
        String id;
        int index;
        Map<String, Object> properties = new HashMap<>();
        final String idField = "id";
        final String indexField = "index";

        DRPCMessageId(String id, int index) {
            this.id = id;
            this.index = index;
            properties.put(idField, id);
            properties.put(indexField, index);
        }
    }

    /**
     * Make a DRPCRequestHandler from a {@link BulletStormConfig}.
     *
     * @param config The BulletStormConfig containing DRPC settings.
     */
    public DRPCRequestHandler(BulletConfig config) {
        function = config.get(BulletStormConfig.TOPOLOGY_FUNCTION).toString();
        int queueSize = Utils.getInt(config.get(BulletDRPCConfig.DRPC_REQUEST_QUEUE_SIZE));
        requests = new ArrayBlockingQueue<>(queueSize);
        isClosed = false;
        open(config);
    }

    private class Adder implements Callable<Void> {
        private String server;
        private int port;
        private Map conf;

        Adder(String server, int port, Map conf) {
            this.server = server;
            this.port = port;
            this.conf = conf;
        }

        @Override
        public Void call() throws Exception {
            try {
                DRPCInvocationsClient c = new DRPCInvocationsClient(conf, server, port);
                synchronized (clients) {
                    clients.add(c);
                }
            } catch (Exception e) {
                log.error("Could not create a client. " + e);
            }
            return null;
        }
    }

    private void reconnect(final DRPCInvocationsClient c) {
        futures.add(backround.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                c.reconnectClient();
                return null;
            }
        }));
    }

    private void checkFutures() {
        Iterator<Future<Void>> i = futures.iterator();
        while (i.hasNext()) {
            Future<Void> f = i.next();
            if (f.isDone()) {
                i.remove();
            }
            try {
                f.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Initialize the request handler by making connecting with the DRPC servers mentioned in the config.
     *
     * @param conf The {@link BulletStormConfig} containing DRPC settings.
     */
    private void open(BulletConfig conf) {
        backround = new ExtendedThreadPoolExecutor(0, Integer.MAX_VALUE,
                    60L, TimeUnit.SECONDS,
                    new SynchronousQueue<Runnable>());
        futures = new LinkedList<>();

        int port = Utils.getInt(conf.get(BulletDRPCConfig.DRPC_INVOCATIONS_PORT));
        List<String> servers = (List<String>) conf.get(BulletDRPCConfig.DRPC_SERVERS);
        int numTasks = Utils.getInt(conf.get(BulletStormConfig.QUERY_SPOUT_PARALLELISM));
        int index = Utils.getInt(conf.get(BulletDRPCConfig.DRPC_INSTANCE_INDEX));

        if (servers == null || servers.isEmpty()) {
            throw new RuntimeException("No DRPC servers configured for topology");
        }
        if (numTasks < servers.size() || index == -1) {
            for (String s: servers) {
                futures.add(backround.submit(new Adder(s, port, conf.getAll(Optional.empty()))));
            }
        } else {
            int i = index % servers.size();
            futures.add(backround.submit(new Adder(servers.get(i), port, conf.getAll(Optional.empty()))));
        }
    }

    /**
     * Close all DRPC clients and fail all requests in the local buffer.
     */
    public void close() {
        for (DRPCInvocationsClient client: clients) {
            client.close();
        }
        for (PubSubMessage message : requests) {
            fail(GSON.fromJson(message.getId(), Object.class));
        }
        backround.shutdownNow();
        isClosed = true;
    }

    /**
     * Fetch a request from the DRPC servers and add it to the local buffer.
     */
    public void fetchRequest() {
        if (requests.remainingCapacity() <= 0 || isClosed) {
            Utils.sleep(1);
            return;
        }
        boolean gotRequest = false;
        int size;
        synchronized (clients) {
            size = clients.size(); //This will only ever grow, so no need to worry about falling off the end
        }
        if (size == 0) {
            log.error("Not connected to DRPC servers.");
        }
        for (int i = 0; i < size; i++) {
            DRPCInvocationsClient client;
            synchronized (clients) {
                client = clients.get(i);
            }
            if (!client.isConnected()) {
                log.warn("DRPCInvocationsClient [{}:{}] is not connected.", client.getHost(), client.getPort());
                reconnect(client);
                continue;
            }
            try {
                DRPCRequest req = client.fetchRequest(function);
                if (req.get_request_id().length() > 0) {
                    Map returnInfo = new HashMap();
                    returnInfo.put("id", req.get_request_id());
                    returnInfo.put("host", client.getHost());
                    returnInfo.put("port", client.getPort());
                    gotRequest = true;
                    DRPCMessageId messageId = new DRPCMessageId(req.get_request_id(), i);
                    requests.add(new PubSubMessage(GSON.toJson(messageId.getProperties()), req.get_func_args(),
                            new Metadata(Metadata.Signal.COMPLETE, GSON.toJson(returnInfo))));
                    log.info("Received query with ID: " + messageId.id  + " value: " + req.get_func_args());
                    break;
                }
            } catch (AuthorizationException aze) {
                reconnect(client);
                log.error("Not authorized to fetch DRPC result from DRPC server", aze);
            } catch (TException e) {
                reconnect(client);
                log.error("Failed to fetch DRPC result from DRPC server", e);
            } catch (Exception e) {
                log.error("Failed to fetch DRPC result from DRPC server", e);
            }
        }
        checkFutures();
        if (!gotRequest) {
            Utils.sleep(1);
        }
    }

    /**
     * Fail the request with the given ID.
     *
     * @param msgId The ID of the request to be failed.
     */
    public void fail(Object msgId) {
        DRPCMessageId did = (DRPCMessageId) msgId;
        DistributedRPCInvocations.Iface client = clients.get(did.index);
        try {
            client.failRequest(did.id);
            log.error("Failed request: " + did.id);
        } catch (AuthorizationException aze) {
            log.error("Not authorized to failREquest from DRPC server", aze);
        } catch (TException e) {
            log.error("Failed to fail request", e);
        }
    }

    /**
     * Acknowledge is NOOP since DRPC servers only accept fails.
     *
     * @param messageID The message ID to be acknowledged.
     */
    public void ack(Object messageID) {
    }

    /**
     * Get the next request from the local buffer waiting if required for up to timeout_ms milliseconds.
     *
     * @param timeout_ms The maximum time in milliseconds to wait for a new request to be fetched.
     * @return A {@link PubSubMessage} containing the request.
     */
    public PubSubMessage getNextRequest(long timeout_ms) {
        try {
            return requests.poll(timeout_ms, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }
}
