/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.google.gson.JsonParseException;
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
import static com.yahoo.bullet.storm.TopologyConstants.JOIN_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.JOIN_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.METADATA_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.META_STREAM;

@Slf4j
public class JoinBolt extends QueryBolt {
    public static class JoinCategory extends QueryCategory {
        @Override
        protected boolean isClosed(Querier querier) {
            return querier.isClosed();
        }
    }

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
            case META_TUPLE:
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
        declarer.declareStream(JOIN_STREAM, new Fields(ID_FIELD, JOIN_FIELD, METADATA_FIELD));
        // This is where the metadata that is used for feedback is sent
        declarer.declareStream(META_STREAM, new Fields(ID_FIELD, METADATA_FIELD));
    }

    private void onTick() {
        // All queries that are now done, roll them over into bufferedQueries
        // TODO

        // Force emit all the done queries or closed queries that are being rotated out.
        Map<String, Querier> forceDone = bufferedQueries.rotate();
        forceDone.entrySet().forEach(this::emitFinishedData);

        // Close all the buffered windows and re-add them back to queries
        Map<String, Querier> forceClosed = bufferedWindows.rotate();
        forceClosed.entrySet().forEach(this::emitAndReintroduce);

        // Whatever we're retiring now MUST not have been satisfied since we emit Queries when FILTER_TUPLES satisfy them.
        // We already decreased activeQueriesCount by emitted. The others that are thrown away should decrease the count too.
        // TODO: updateCount(activeQueriesCount, -forceDone.size() + emitted);
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
                setupQuery(id, query, metadata, querier);
                return;
            }
            emitErrorData(id, metadata, optionalErrors.get());
        } catch (JsonParseException jpe) {
            emitErrorData(id, metadata, ParsingError.makeError(jpe, query));
        } catch (RuntimeException re) {
            log.error("Unhandled exception.", re);
            emitErrorData(id, metadata, ParsingError.makeError(re, query));
        }
        log.error("Failed to initialize query for request {} with query {}", id, query);
    }

    private void onData(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        byte[] data = (byte[]) tuple.getValue(TopologyConstants.DATA_POSITION);

        // Do this check before emitCategorizedQueries cleans up or rotates data
        boolean wasBuffered = wasBuffered(id);

        emitCategorizedQueries(new JoinCategory().categorize(data, queries));

        // Feed it into queries sitting in the buffered maps regardless of if they are closed or not
        if (wasBuffered) {
            addToBufferedQuery(id, data, bufferedWindows);
            addToBufferedQuery(id, data, bufferedQueries);
        }
    }

    private void onError(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        RateLimitError error = (RateLimitError) tuple.getValue(TopologyConstants.ERROR_POSITION);
        Querier querier = getQuery(id);
        emitRateLimitError(id, querier, error);
    }

    private void emitCategorizedQueries(QueryCategory category) {
        Map<String, Querier> done = category.getDone();
        done.entrySet().forEach(this::emitFinishedData);

        Map<String, Querier> rateLimited = category.getRateLimited();
        rateLimited.entrySet().forEach(this::emitRateLimitError);

        Map<String, Querier> closed = category.getClosed();
        closed.entrySet().forEach(this::emitOrRotate);

        log.info("Done: {}, Rate limited: {}, Closed: {}, Pending Emit: {}, Active: {}",
                 done.size(), rateLimited.size(), closed.size(), bufferedWindows.size(), queries.size());
    }

    // JOIN_STREAM and META_STREAM emitters

    private void emitRateLimitError(Map.Entry<String, Querier> query) {
        Querier querier = query.getValue();
        updateCount(rateExceededQueries, 1L);
        emitRateLimitError(query.getKey(), querier, querier.getRateLimitError());
    }

    private void emitRateLimitError(String id, Querier querier, RateLimitError error) {
        Metadata metadata = bufferedMetadata.get(id);
        Meta meta = error.makeMeta();
        Clip clip = querier.finish();
        clip.getMeta().merge(meta);
        log.info("Received rate limit error and cleaning up {}...", id);
        emitResult(id, metadata, clip);
        emitMetaSignal(id, Metadata.Signal.KILL);
        removeQuery(id);
    }

    // JOIN_STREAM emitters

    private void emitFinishedData(Map.Entry<String, Querier> query) {
        String id = query.getKey();
        Querier querier  = query.getValue();
        Metadata metadata = bufferedMetadata.get(id);
        log.info("Finishing up {}...", id);
        emitResult(id, metadata, querier.finish());
        removeQuery(id);
        updateCount(activeQueriesCount, -1L);
    }

    private void emitOrRotate(Map.Entry<String, Querier> query) {
        String id = query.getKey();
        Querier querier  = query.getValue();
        if (querier.isTimeBasedWindow()) {
            log.debug("Buffering window for time-windowed query {}...", id);
            rotateInto(id, querier, bufferedWindows);
        } else {
            log.debug("Emitting window for {} and resetting...", id);
            emitResult(id, bufferedMetadata.get(id), querier.getResult());
            querier.reset();
        }
    }

    private void emitAndReintroduce(Map.Entry<String, Querier> query) {
        String id = query.getKey();
        Querier querier = query.getValue();
        emitResult(id, bufferedMetadata.get(id), querier.getResult());
        querier.reset();
        bufferedWindows.remove(id);
        queries.put(id, querier);
    }

    private void emitErrorData(String id, Metadata metadata, BulletError... errors) {
        emitErrorData(id, metadata, Arrays.asList(errors));
    }

    private void emitErrorData(String id, Metadata metadata, List<BulletError> errors) {
        updateCount(improperQueriesCount, 1L);
        emitResult(id, metadata, Clip.of(Meta.of(errors)));
    }

    private void emitResult(String id, Metadata metadata, Clip result) {
        collector.emit(JOIN_STREAM, new Values(id, result.asJSON(), metadata));
    }

    // META_STREAM emitters

    private void emitMetaSignal(String id, Metadata.Signal signal) {
        log.error("Emitting for {}: {} signal", id, signal);
        collector.emit(META_STREAM, new Values(id, new Metadata(signal, null)));
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
            query = bufferedWindows.get(id);
        }
        if (query == null) {
            query = bufferedQueries.get(id);
        }
        return query;
    }

    private void removeQuery(String id) {
        queries.remove(id);
        bufferedWindows.remove(id);
        bufferedQueries.remove(id);
        bufferedMetadata.remove(id);
    }

    private boolean wasBuffered(String id) {
        return !queries.containsKey(id) && (bufferedWindows.containsKey(id) || bufferedQueries.containsKey(id));
    }

    private void addToBufferedQuery(String id, byte[] data, RotatingMap<String, Querier> buffer) {
        Querier querier = buffer.get(id);
        if (querier != null) {
            log.debug("Combining data into time-windowed buffered query {}...", id);
            querier.combine(data);
        }
    }
}
