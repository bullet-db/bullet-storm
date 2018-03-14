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

import static com.yahoo.bullet.storm.TopologyConstants.FEEDBACK_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.ID_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.METADATA_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.RESULT_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.RESULT_STREAM;

@Slf4j
public class JoinBolt extends QueryBolt {
    private static final long serialVersionUID = 3312434064971532267L;

    private transient Map<String, Metadata> bufferedMetadata;
    // For doing a join between Queries and intermediate windows, if the windows are time based and arriving slowly.
    private transient RotatingMap<String, Querier> bufferedQueries;
    // For doing a join between expired queries and final windows, if query has expired and last data is arriving slowly.
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
        TupleClassifier.Type type = classifier.classifyInternalTypes(tuple).orElse(TupleClassifier.Type.UNKNOWN_TUPLE);
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
        declarer.declareStream(FEEDBACK_STREAM, new Fields(ID_FIELD, METADATA_FIELD));
    }

    private void onTick() {
        // Force emit all the done queries or closed queries that are being rotated out.
        Map<String, Querier> forceDone = bufferedQueries.rotate();
        forceDone.entrySet().forEach(this::emitFinished);
        // The active queries count is not updated for these since these queries are not in any map, so do it here
        updateCount(activeQueriesCount, -forceDone.size());

        // Close all the buffered windows and re-add them back to queries
        Map<String, Querier> forceClosed = bufferedWindows.rotate();
        forceClosed.entrySet().forEach(this::emitBufferedWindow);

        // Categorize all in queries in non-partition mode and do the roll over or emit as necessary
        handleCategorizedQueries(new QueryCategorizer(Querier::isClosed).categorize(queries));
    }

    private void onQuery(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        String query = tuple.getString(TopologyConstants.QUERY_POSITION);
        Metadata metadata = (Metadata) tuple.getValue(TopologyConstants.QUERY_METADATA_POSITION);

        Querier querier;
        try {
            querier = createQuerier(id, query, config);
            Optional<List<BulletError>> optionalErrors = querier.initialize();
            if (!optionalErrors.isPresent()) {
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

        // If already in buffer, then don't check isClosed() and don't emit window. Tick will emit it.
        if (querier.isDone()) {
            emitOrBufferFinished(id, querier);
        } else if (querier.isExceedingRateLimit()) {
            emitRateLimitError(id, querier, querier.getRateLimitError());
        } else if (!bufferedWindows.containsKey(id) && querier.isClosed()) {
            emitOrBufferWindow(id, querier);
        }
    }

    private void onError(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        Querier querier = getQuery(id);
        if (querier == null) {
            log.debug("Received error for {} without the query existing", id);
            // TODO Might later create this query whose error ignored here. This is a leak.
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

        Map<String, Querier> closed = category.getClosed();
        closed.entrySet().forEach(this::emitOrBufferWindow);

        log.debug("Done: {}, Rate limited: {}, Closed: {}, Pending Windows: {}, Pending Done: {}, Active: {}", done.size(),
                  rateLimited.size(), closed.size(), bufferedWindows.size(), bufferedQueries.size(), queries.size());
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
        emitResult(id, withSignal(metadata, Metadata.Signal.FAIL), clip);
        emitMetaSignal(id, Metadata.Signal.KILL);
        updateCount(rateExceededQueries, 1L);
        removeQuery(id);
    }

    private void emitOrBufferFinished(Map.Entry<String, Querier> query) {
        emitOrBufferFinished(query.getKey(), query.getValue());
    }

    private void emitOrBufferFinished(String id, Querier querier) {
        // Only buffer if not already buffered in EITHER bufferedQueries or bufferedWindows.
        // It might have been in bufferedWindows and a combine might have caused it to be done. If so, we should emit.
        boolean shouldBuffer = queries.containsKey(id) && querier.shouldBuffer();
        if (shouldBuffer) {
            log.debug("Buffering while waiting for more final results for query {}...", id);
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
        emitResult(id, withSignal(bufferedMetadata.get(id), Metadata.Signal.COMPLETE), querier.finish());
        emitMetaSignal(id, Metadata.Signal.COMPLETE);
        removeQuery(id);
    }

    // RESULT_STREAM emitters

    private void emitOrBufferWindow(Map.Entry<String, Querier> query) {
        emitOrBufferWindow(query.getKey(), query.getValue());
    }

    private void emitOrBufferWindow(String id, Querier querier) {
        if (querier.shouldBuffer()) {
            log.debug("Buffering while waiting for more windows for query {}...", id);
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
        emitResult(id, withSignal(metadata, Metadata.Signal.FAIL), Clip.of(Meta.of(errors)));
    }

    private void emitResult(String id, Metadata metadata, Clip result) {
        // Metadata should not be checked. It could be null.
        collector.emit(RESULT_STREAM, new Values(id, result.asJSON(), metadata));
    }

    // METADATA_STREAM emitters

    private void emitMetaSignal(String id, Metadata.Signal signal) {
        log.error("Emitting {} signal to the feedback stream for {}", signal, id);
        collector.emit(FEEDBACK_STREAM, new Values(id, new Metadata(signal, null)));
    }

    // Override hooks

    @Override
    protected void setupQuery(String id, String query, Metadata metadata, Querier querier) {
        super.setupQuery(id, query, metadata, querier);
        bufferedMetadata.put(id, metadata);
        updateCount(createdQueriesCount, 1L);
        updateCount(activeQueriesCount, 1L);
    }

    @Override
    protected void removeQuery(String id) {
        if (getQuery(id) != null) {
            updateCount(activeQueriesCount, -1L);
        }
        super.removeQuery(id);
        bufferedWindows.remove(id);
        bufferedQueries.remove(id);
        bufferedMetadata.remove(id);
    }

    // Other helpers

    private void rotateInto(String id, Querier querier, RotatingMap<String, Querier> into) {
        // Make sure it's not in queries
        queries.remove(id);
        into.put(id, querier);
    }

    private Metadata withSignal(Metadata metadata, Metadata.Signal signal) {
        // Don't change the non-readonly bits of metadata in place since that might affect tuples emitted but pending
        Metadata copy = new Metadata(signal, null);
        if (metadata != null) {
            copy.setContent(metadata.getContent());
        }
        return copy;
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
}
