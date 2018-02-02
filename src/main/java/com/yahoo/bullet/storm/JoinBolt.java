/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.ParsingError;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.querying.Querier;
import com.yahoo.bullet.querying.RateLimitError;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.RotatingMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.yahoo.bullet.storm.TopologyConstants.ID_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.METADATA_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.METADATA_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.RESULT_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.RESULT_STREAM;

@Slf4j
public class JoinBolt extends QueryBolt {
    private static final long serialVersionUID = 3312434064971532267L;

    private transient Map<String, Metadata> bufferedMetadata;
    // For doing a join between Queries and intermediate windows, if the windows are time based and arriving slowly.
    private transient RotatingMap<String, Querier> bufferedQueries;
    // For doing a join between expired queries and final data, if query has expired.
    private transient RotatingMap<String, Querier> bufferedWindows;

    // Variable
    private transient AbsoluteCountMetric activeQueriesCount;
    // Monotonically increasing
    private transient AbsoluteCountMetric createdQueriesCount;
    private transient AbsoluteCountMetric improperQueriesCount;
    private transient AbsoluteCountMetric rateExceededQueries;

    /**
     * Constructor that creates an instance of this JoinBolt using the given config.
     *
     * @param config The validated {@link BulletStormConfig} to use.
     */
    public JoinBolt(BulletStormConfig config) {
        super(config);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        bufferedMetadata = new HashMap<>();

        int windowTickOut = config.getAs(BulletStormConfig.JOIN_BOLT_WINDOW_TICK_TIMEOUT, Integer.class);
        bufferedWindows = new RotatingMap<>(windowTickOut);

        int queryTickOut = config.getAs(BulletStormConfig.JOIN_BOLT_QUERY_TICK_TIMEOUT, Integer.class);
        bufferedQueries = new RotatingMap<>(queryTickOut);

        if (metricsEnabled) {
            activeQueriesCount = registerAbsoluteCountMetric(TopologyConstants.ACTIVE_QUERIES_METRIC, context);
            createdQueriesCount = registerAbsoluteCountMetric(TopologyConstants.CREATED_QUERIES_METRIC, context);
            improperQueriesCount = registerAbsoluteCountMetric(TopologyConstants.IMPROPER_QUERIES_METRIC, context);
            rateExceededQueries = registerAbsoluteCountMetric(TopologyConstants.RATE_EXCEEDED_QUERIES_METRIC, context);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        TupleClassifier.Type type = classifier.classifyNonRecord(tuple).orElse(TupleClassifier.Type.UNKNOWN_TUPLE);
        switch (type) {
            case TICK_TUPLE:
                onTick();
                break;
            case METADATA_TUPLE:
                onMeta(tuple);
                break;
            case QUERY_TUPLE:
                onQuery(tuple);
                break;
            case ERROR_TUPLE:
                onError(tuple);
                break;
            case DATA_TUPLE:
                onData(tuple);
                break;
            default:
                // May want to throw an error here instead of not acking
                log.error("Unknown tuple encountered in join: {}", type);
                return;
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // This is where the data for each queries go
        declarer.declareStream(RESULT_STREAM, new Fields(ID_FIELD, RESULT_FIELD, METADATA_FIELD));
        // This is where the metadata that is used for feedback is sent
        declarer.declareStream(METADATA_STREAM, new Fields(ID_FIELD, METADATA_FIELD));
    }

    private void onTick() {
        // Force emit all the done queries or closed queries that are being rotated out.
        Map<String, Querier> forceDone = bufferedQueries.rotate();
        forceDone.entrySet().forEach(this::emitFinished);

        // Close all the buffered windows and re-add them back to queries
        Map<String, Querier> forceClosed = bufferedWindows.rotate();
        forceClosed.entrySet().forEach(this::emitBufferedWindow);

        // Categorize all in queries and do the roll over or emit as necessary
        handleCategorizedQueries(new QueryCategorizer().categorize(queries, false));
    }

    private void onQuery(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        String query = tuple.getString(TopologyConstants.QUERY_POSITION);
        Metadata metadata = (Metadata) tuple.getValue(TopologyConstants.QUERY_METADATA_POSITION);

        Querier querier;
        try {
            querier = new Querier(id, query, config);
            Optional<List<BulletError>> optionalErrors = querier.initialize();
            if (!optionalErrors.isPresent()) {
                // TODO Might create a query whose filter error was received and ignored before. This could be a leak.
                setupQuery(id, query, metadata, querier);
                return;
            }
            emitErrorsAsResult(id, metadata, optionalErrors.get());
        } catch (RuntimeException re) {
            // Includes JSONParseException
            emitErrorsAsResult(id, metadata, ParsingError.makeError(re, query));
        }
        log.error("Failed to initialize query for request {} with query {}", id, query);
    }

    private void onData(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        Querier querier = getQuery(id);
        if (querier == null) {
            log.debug("Received data for query {} before query. Ignoring...", id);
            return;
        }
        byte[] data = (byte[]) tuple.getValue(TopologyConstants.DATA_POSITION);
        querier.combine(data);
        if (querier.isDone()) {
            emitOrBufferFinished(id, querier);
        } else if (querier.isExceedingRateLimit()) {
            emitRateLimitError(id, querier, querier.getRateLimitError());
        } else if (querier.isClosed()) {
            emitOrBufferWindow(id, querier);
        }
    }

    private void onError(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        Querier querier = getQuery(id);
        if (querier == null) {
            log.debug("Received error for {} without the query existing", id);
            return;
        }
        RateLimitError error = (RateLimitError) tuple.getValue(TopologyConstants.ERROR_POSITION);
        emitRateLimitError(id, querier, error);
    }

    private void handleCategorizedQueries(QueryCategorizer category) {
        Map<String, Querier> done = category.getDone();
        done.entrySet().forEach(this::emitOrBufferFinished);

        Map<String, Querier> rateLimited = category.getRateLimited();
        rateLimited.entrySet().forEach(this::emitRateLimitError);
        updateCount(rateExceededQueries, rateLimited.size());

        Map<String, Querier> closed = category.getClosed();
        closed.entrySet().forEach(this::emitOrBufferWindow);

        log.info("Done: {}, Rate limited: {}, Closed: {}, Pending Emit: {}, Active: {}",
                 done.size(), rateLimited.size(), closed.size(), bufferedWindows.size(), queries.size());
    }

    // RESULT_STREAM and METADATA_STREAM emitters

    private void emitRateLimitError(Map.Entry<String, Querier> query) {
        Querier querier = query.getValue();
        emitRateLimitError(query.getKey(), querier, querier.getRateLimitError());
    }

    private void emitRateLimitError(String id, Querier querier, RateLimitError error) {
        Metadata metadata = bufferedMetadata.get(id);
        Meta meta = error.makeMeta();
        Clip clip = querier.finish();
        clip.getMeta().merge(meta);
        emitResult(id, metadata, clip);
        emitMetaSignal(id, Metadata.Signal.KILL);
        removeQuery(id);
    }

    // RESULT_STREAM emitters

    private void emitOrBufferFinished(Map.Entry<String, Querier> query) {
        emitOrBufferFinished(query.getKey(), query.getValue());
    }

    private void emitOrBufferFinished(String id, Querier querier) {
        if (querier.isTimeBasedWindow()) {
            log.debug("Buffering final result for time-windowed query {}...", id);
            rotateInto(id, querier, bufferedQueries);
        } else {
            emitFinished(id, querier);
        }
    }

    private void emitFinished(Map.Entry<String, Querier> query) {
        emitFinished(query.getKey(), query.getValue());
    }

    private void emitFinished(String id, Querier querier) {
        log.info("Query is done {}...", id);
        emitResult(id, bufferedMetadata.get(id), querier.finish());
        removeQuery(id);
    }

    private void emitOrBufferWindow(Map.Entry<String, Querier> query) {
        emitOrBufferWindow(query.getKey(), query.getValue());
    }

    private void emitOrBufferWindow(String id, Querier querier) {
        if (querier.isTimeBasedWindow()) {
            log.debug("Buffering window for time-windowed query {}...", id);
            rotateInto(id, querier, bufferedWindows);
        } else {
            log.debug("Emitting window for {} and resetting...", id);
            emitResult(id, bufferedMetadata.get(id), querier.getResult());
            querier.reset();
        }
    }

    private void emitBufferedWindow(Map.Entry<String, Querier> query) {
        String id = query.getKey();
        Querier querier = query.getValue();
        emitResult(id, bufferedMetadata.get(id), querier.getResult());
        querier.reset();
        bufferedWindows.remove(id);
        queries.put(id, querier);
    }

    private void emitErrorsAsResult(String id, Metadata metadata, BulletError... errors) {
        emitErrorsAsResult(id, metadata, Arrays.asList(errors));
    }

    private void emitErrorsAsResult(String id, Metadata metadata, List<BulletError> errors) {
        updateCount(improperQueriesCount, 1L);
        emitResult(id, metadata, Clip.of(Meta.of(errors)));
    }

    private void emitResult(String id, Metadata metadata, Clip result) {
        if (metadata == null) {
            log.debug("Unable to emit data since there is no metadata for {}", id);
            return;
        }
        collector.emit(RESULT_STREAM, new Values(id, result.asJSON(), metadata));
    }

    // METADATA_STREAM emitters

    private void emitMetaSignal(String id, Metadata.Signal signal) {
        log.error("Emitting for {}: {} signal", id, signal);
        collector.emit(METADATA_STREAM, new Values(id, new Metadata(signal, null)));
    }

    // Other helpers

    private void rotateInto(String id, Querier querier, RotatingMap<String, Querier> into) {
        // Make sure it's not in queries
        queries.remove(id);
        into.put(id, querier);
    }

    private void setupQuery(String id, String query, Metadata metadata, Querier querier) {
        queries.put(id, querier);
        bufferedMetadata.put(id, metadata);
        updateCount(createdQueriesCount, 1L);
        updateCount(activeQueriesCount, 1L);
        log.info("Initialized query {}: {}", id, query);
    }

    private Querier getQuery(String id) {
        // JoinBolt has three places where the query might be
        Querier query = queries.get(id);
        if (query == null) {
            log.debug("Query might be buffered: {}", id);
            query = bufferedWindows.get(id);
        }
        if (query == null) {
            log.debug("Query might be done: {}", id);
            query = bufferedQueries.get(id);
        }
        return query;
    }

    private void removeQuery(String id) {
        if (getQuery(id) != null) {
            updateCount(activeQueriesCount, -1L);
        }
        queries.remove(id);
        bufferedWindows.remove(id);
        bufferedQueries.remove(id);
        bufferedMetadata.remove(id);
    }
}
