package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.RandomPool;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.storm.drpc.utils.DRPCError;
import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class is both a query {@link Publisher} and a result {@link Subscriber} because it uses HTTP to do a request-
 * response call where the request is the Bullet query and the response is the result from the backend.
 */
@Slf4j
public class DRPCQueryResultPubscriber implements Publisher, Subscriber {
    private RandomPool<String> urls;
    private AsyncHttpClient client;
    private Queue<PubSubMessage> responses;

    public static final int NO_TIMEOUT = -1;

    public static final int OK_200 = 200;

    public static final String PATH_SEPARATOR = "/";
    public static final String PORT_PREFIX = ":";
    public static final String PROTOCOL_SUFFIX = "://";
    private static final String TEMPLATE = "%1$s" + PROTOCOL_SUFFIX + "%2$s" + PORT_PREFIX + "%3$s" + PATH_SEPARATOR + "%4$s";

    /**
     * Create an instance from a given {@link DRPCConfig}. Requires {@link DRPCConfig#DRPC_CONNECT_TIMEOUT},
     * {@link DRPCConfig#DRPC_CONNECT_RETRY_LIMIT}, {@link DRPCConfig#DRPC_SERVERS}, {@link DRPCConfig#DRPC_HTTP_PORT},
     * and {@link DRPCConfig#DRPC_HTTP_PATH} to be set. To be used in PubSub.Context#QUERY_SUBMISSION mode.
     *
     * @param config A non-null config that contains the required settings.
     */
    public DRPCQueryResultPubscriber(DRPCConfig config) {
        Objects.requireNonNull(config);

        Number connectTimeout = get(config, DRPCConfig.DRPC_CONNECT_TIMEOUT);
        Number retryLimit = get(config, DRPCConfig.DRPC_CONNECT_RETRY_LIMIT);
        List<String> urls = get(config, DRPCConfig.DRPC_SERVERS);
        String protocol = get(config, DRPCConfig.DRPC_HTTP_PROTOCOL).toString();
        String port = get(config, DRPCConfig.DRPC_HTTP_PORT).toString();
        String path = get(config, DRPCConfig.DRPC_HTTP_PATH).toString();

        this.urls = new RandomPool<>(getURLs(protocol, urls, port, path));
        AsyncHttpClientConfig clientConfig = new DefaultAsyncHttpClientConfig.Builder()
                                                    .setConnectTimeout(connectTimeout.intValue())
                                                    .setMaxRequestRetry(retryLimit.intValue())
                                                    .setReadTimeout(NO_TIMEOUT)
                                                    .setRequestTimeout(NO_TIMEOUT)
                                                    .build();
        client = new DefaultAsyncHttpClient(clientConfig);
        responses = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        String url = urls.get();
        String id = message.getId();
        String json = DRPCMessage.toJSON(message);
        log.info("Posting to {}: with \n {}", url, json);
        client.preparePost(url).setBody(json).execute()
              .toCompletableFuture()
              .exceptionally(createErrorResponse(id))
              .thenAcceptAsync(createResponseConsumer(id));
    }

    @Override
    public PubSubMessage receive() throws PubSubException {
        PubSubMessage message = responses.poll();
        if (message == null) {
            return null;
        }
        return message;
    }

    @Override
    public void commit(String id, int sequence) {
        // Do nothing
    }

    @Override
    public void fail(String id, int sequence) {
        // Do nothing
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException ioe) {
            log.error("Error while closing AsyncHTTPClient", ioe);
        }
    }

    private Consumer<Response> createResponseConsumer(String id) {
        return response -> handleResponse(id, response);
    }

    private Function<Throwable, Response> createErrorResponse(String id) {
        return throwable -> handleException(id, throwable);

    }

    private Response handleException(String id, Throwable throwable) {
        log.error("Received error", throwable);
        log.error("Creating error response for {}", id);
        return null;
    }

    private void handleResponse(String id, Response response) {
        if (response == null || response.getStatusCode() != OK_200) {
            log.error("Handling error response for {}", id);
            responses.offer(new PubSubMessage(id, getErrorMessage(DRPCError.CANNOT_REACH_DRPC)));
            return;
        }
        log.info("Received status {}: {}", response.getStatusCode(), response.getStatusText());
        String body = response.getResponseBody();
        PubSubMessage message = DRPCMessage.fromJSON(body);
        log.info("Received content for {} with:\n{}", message.getId(), message.getContent());
        responses.offer(message);
    }

    private static String getErrorMessage(DRPCError cause) {
        Clip responseEntity = Clip.of(Metadata.of(Error.makeError(cause.getError(), cause.getResolution())));
        return responseEntity.asJSON();
    }

    private static <T> T get(DRPCConfig config, String key) {
        return (T) Objects.requireNonNull(config.get(key), key + " not found in config");
    }

    private static List<String> getURLs(String protocol, List<String> urls, String port, String path) {
        return urls.stream().map(s -> String.format(TEMPLATE, protocol, s, port, path)).collect(Collectors.toList());
    }
}
