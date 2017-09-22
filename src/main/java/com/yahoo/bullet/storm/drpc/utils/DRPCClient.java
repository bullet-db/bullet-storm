/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.storm.drpc.BulletDRPCConfig;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.utils.Utils;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j @Getter @Setter
public class DRPCClient {
    private RandomPool<String> urls;

    private int connectTimeout;
    private int retryLimit;

    public static final String URL_DELIMITER = ",";
    public static final String PATH_SEPARATOR = "/";
    public static final String PORT_PREFIX = ":";
    public static final String PROTOCOL_PREFIX = "http://";
    private static final String TEMPLATE = PROTOCOL_PREFIX + "%1$s" + PORT_PREFIX + "%2$s" + PATH_SEPARATOR + "%3$s";

    /**
     * Constructor that takes in a {@link BulletDRPCConfig} containing all the relevant settings.
     *
     * @param config The {@link BulletDRPCConfig} containing all settings.
     *
     */
    public DRPCClient(BulletConfig config) {
        connectTimeout = Objects.requireNonNull(Utils.getInt(config.get(BulletDRPCConfig.DRPC_CONNECT_TIMEOUT)));
        retryLimit = Objects.requireNonNull(Utils.getInt(config.get(BulletDRPCConfig.DRPC_CONNECT_RETRY_LIMIT)));
        List<String> urls = Objects.requireNonNull((List<String>) config.get(BulletDRPCConfig.DRPC_SERVERS));
        String port = Objects.requireNonNull(config.get(BulletDRPCConfig.DRPC_HTTP_PORT).toString());
        String path = Objects.requireNonNull(config.get(BulletDRPCConfig.DRPC_PATH).toString());
        this.urls = new RandomPool(getURLs(urls, port, path));
    }

    private List<String> getURLs(@NonNull List<String> urls, @NonNull String port, @NonNull String path) {
        return urls.stream().map(s -> String.format(TEMPLATE, s, port, path))
                     .collect(Collectors.toList());
    }

    private Client getClient() {
        ClientConfig config = new ClientConfig();
        config.property(ClientProperties.CONNECT_TIMEOUT, connectTimeout);
        config.property(ClientProperties.READ_TIMEOUT, connectTimeout);
        return ClientBuilder.newClient(config);
    }

    /**
     * POSTs the request to the given url using the given client. Internal use only.
     *
     * @param client A client to use for making the call.
     * @param url The URL to make the call to.
     * @param request The String request to POST.
     * @return The response from the call.
     */
    DRPCResponse makeRequest(Client client, String url, String request) {
        log.info("Posting to " + url + " with \n" + request);
        WebTarget target = client.target(url);
        Response response = target.request(MediaType.TEXT_PLAIN).post(Entity.entity(request, MediaType.TEXT_PLAIN));
        Response.StatusType status = response.getStatusInfo();
        log.info("Received context code " + response.getStatus());
        String content = response.readEntity(String.class);
        log.info("Response content " + content);
        if (status.getFamily() == Response.Status.Family.SUCCESSFUL) {
            return new DRPCResponse(content);
        }
        return new DRPCResponse(DRPCError.CANNOT_REACH_DRPC);
    }

    private DRPCResponse tryRequest(String url, String request, Client client) {
        for (int i = 1; i <= retryLimit; ++i) {
            try {
                return makeRequest(client, url, request);
            } catch (ProcessingException pe) {
                log.warn("Attempt {} of {} failed for {}", i, retryLimit, url);
            }
        }
        throw new ProcessingException("Request to DRPC server " + url + " failed");
    }

    private String getErrorMessage(DRPCError cause) {
        Clip responseEntity = Clip.of(com.yahoo.bullet.result.Metadata.of(Error.makeError(cause.getError(), cause.getResolution())));
        return responseEntity.asJSON();
    }

    /**
     * Submit a query to the DRPC server, wait and fetch the results.
     *
     * @param query The query to be executed.
     * @return The result of the query execution from Bullet.
     */
    public String exeuteQuery(String query) {
        Client client = getClient();
        String url = urls.get();
        DRPCResponse response;
        try {
            response = tryRequest(url, query, client);
        } catch (ProcessingException e) {
            log.warn("Retry limit exceeded. Could not reach DRPC endpoint url {}", url);
            response = new DRPCResponse(DRPCError.RETRY_LIMIT_EXCEEDED);
        }
        return response.hasError() ? getErrorMessage(response.getError()) : response.getContent();
    }
}
