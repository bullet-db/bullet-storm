/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.querying.Querier;
import com.yahoo.bullet.querying.QueryCategorizer;
import com.yahoo.bullet.querying.RateLimitError;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import com.yahoo.bullet.storm.metric.AbsoluteCountMetric;
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

import static com.yahoo.bullet.storm.TopologyConstants.FEEDBACK_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.ID_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.METADATA_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.RESULT_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.RESULT_STREAM;

@Slf4j
public class JoinBolt extends QueryBolt {
    private static final long serialVersionUID = 3312434064971532267L;

    private transient Map<String, Metadata> bufferedMetadata;
    private transient Map<String, Querier> queries;
    // For buffering queries for their final windows or results, if the query windows are record based or have no windows.
    private transient RotatingMap<String, Querier> postFinishBuffer;
    // For buffering queries initially before they are restarted to offset windows if they are time based.
    private transient RotatingMap<String, Querier> preStartBuffer;

    // Variable
    private transient AbsoluteCountMetric activeQueriesCount;
    // Monotonically increasing
    private transient AbsoluteCountMetric createdQueriesCount;
    private transient AbsoluteCountMetric improperQueriesCount;
    private transient AbsoluteCountMetric rateExceededQueries;
    private transient AbsoluteCountMetric duplicatedQueriesCount;

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
        queries = new HashMap<>();

        int preStartDelayTicks = config.getAs(BulletStormConfig.JOIN_BOLT_WINDOW_PRE_START_DELAY_TICKS, Integer.class);
        preStartBuffer = new RotatingMap<>(preStartDelayTicks);

        int postFinishBufferTicks = config.getAs(BulletStormConfig.JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS, Integer.class);
        postFinishBuffer = new RotatingMap<>(postFinishBufferTicks);

        if (metrics.isEnabled()) {
            activeQueriesCount = metrics.registerAbsoluteCountMetric(TopologyConstants.ACTIVE_QUERIES_METRIC, context);
            createdQueriesCount = metrics.registerAbsoluteCountMetric(TopologyConstants.CREATED_QUERIES_METRIC, context);
            improperQueriesCount = metrics.registerAbsoluteCountMetric(TopologyConstants.IMPROPER_QUERIES_METRIC, context);
            rateExceededQueries = metrics.registerAbsoluteCountMetric(TopologyConstants.RATE_EXCEEDED_QUERIES_METRIC, context);
            duplicatedQueriesCount = metrics.registerAbsoluteCountMetric(TopologyConstants.DUPLICATED_QUERIES_METRIC, context);
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
            case BATCH_TUPLE:
                onBatch(tuple);
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
        // Force emit all the done queries queries that are being rotated out.
        Map<String, Querier> forceDone = postFinishBuffer.rotate();
        forceDone.entrySet().forEach(this::emitFinished);
        // The active queries count is not updated for these since these queries are not in any map, so do it here
        metrics.updateCount(activeQueriesCount, -forceDone.size());

        // Start all the delayed queries and add them to queries
        Map<String, Querier> delayed = preStartBuffer.rotate();
        delayed.entrySet().forEach(this::startDelayed);

        // Categorize all the active queries and do the buffering or emit as necessary
        handleCategorizedQueries(new QueryCategorizer().categorize(queries));

        emitReplayRequestIfNecessary();
    }

    private void onQuery(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        byte[] queryData = (byte[]) tuple.getValue(TopologyConstants.QUERY_POSITION);
        Metadata metadata = (Metadata) tuple.getValue(TopologyConstants.QUERY_METADATA_POSITION);
        initializeQuery(id, queryData, metadata);
    }

    private void initializeQuery(String id, byte[] queryData, Metadata metadata) {
        // bufferedMetadata has an entry for each query that exists in the JoinBolt; therefore, we check bufferedMetadata
        // for existing queries (as opposed to individually checking the queries, preStartBuffer, and postFinishBuffer maps)
        if (bufferedMetadata.containsKey(id)) {
            log.debug("Duplicate for request {}", id);
            metrics.updateCount(duplicatedQueriesCount, 1L);
            return;
        }
        try {
            Query query = SerializerDeserializer.fromBytes(queryData);
            Querier querier = createQuerier(Querier.Mode.ALL, id, query, metadata, config);
            setupQuery(id, metadata, querier);
            return;
        } catch (RuntimeException re) {
            // Includes JSONParseException
            emitErrorsAsResult(id, metadata, BulletError.makeError(re.toString(), "Error initializing query"));
        }
        log.error("Failed to initialize query for request {}", id);
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
            emitWindow(id, querier);
        }
    }

    private void onError(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        Querier querier = getQuery(id);
        if (querier == null) {
            log.debug("Received error for {} without the query existing", id);
            // TODO Might later create this query if it is received late but whose error was ignored here. This is a leak.
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
        closed.entrySet().forEach(this::emitWindow);

        log.debug("Done: {}, Rate limited: {}, Closed: {}, Starting delayed: {}, Buffered finished: {}, Active: {}",
                  done.size(), rateLimited.size(), closed.size(), preStartBuffer.size(), postFinishBuffer.size(), queries.size());
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
        metrics.updateCount(rateExceededQueries, 1L);
        removeQuery(id);
    }

    private void emitOrBufferFinished(Map.Entry<String, Querier> query) {
        emitOrBufferFinished(query.getKey(), query.getValue());
    }

    private void emitOrBufferFinished(String id, Querier querier) {
        /*
         * Three cases:
         * 1) If we shouldn't buffer, then emit it and return. If it was being delayed and somehow finished, it is
         *    cleaned up and removed. There should be no query that needs delaying AND buffering.
         * 2) If the query became closed after it finished (wherever it is), we emit it. We should still honor isClosed.
         * 3) If it should buffer and it isn't closed, postFinishBuffer it till it becomes closed or ticks emit it.
         */
        if (!querier.shouldBuffer()) {
            log.debug("Emitting query since it shouldn't be buffered {}", id);
            emitFinished(id, querier);
        } else if (querier.isClosed()) {
            log.debug("Emitting query since it finished but this is the last window for it {}", id);
            emitFinished(id, querier);
        } else if (queries.containsKey(id)) {
            log.debug("Starting to buffer while waiting for more final results for query {}", id);
            queries.remove(id);
            postFinishBuffer.put(id, querier);
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

    private void emitWindow(Map.Entry<String, Querier> query) {
        emitWindow(query.getKey(), query.getValue());
    }

    private void emitWindow(String id, Querier querier) {
        // No matter where it is - emit and reset.
        log.debug("Emitting window for {} and resetting...", id);
        emitResult(id, bufferedMetadata.get(id), querier.getResult());
        querier.reset();
        // We should not receive window for queries in the pre-start buffer because those are only time-based windowed
        // queries that the config ensures have a minimum emit time greater than the pre-start delay.
    }

    private void emitErrorsAsResult(String id, Metadata metadata, BulletError... errors) {
        emitErrorsAsResult(id, metadata, Arrays.asList(errors));
    }

    private void emitErrorsAsResult(String id, Metadata metadata, List<BulletError> errors) {
        metrics.updateCount(improperQueriesCount, 1L);
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
    protected void initializeQuery(PubSubMessage message) {
        initializeQuery(message.getId(), message.getContent(), message.getMetadata());
    }

    @Override
    protected void removeQuery(String id) {
        // Only update count if query was in queries or postFinishBuffer.
        if (queries.containsKey(id) || postFinishBuffer.containsKey(id)) {
            metrics.updateCount(activeQueriesCount, -1L);
        }
        queries.remove(id);
        postFinishBuffer.remove(id);
        bufferedMetadata.remove(id);
        // It should not be in the preStartBuffer under normal operations but could be if it was killed.
        preStartBuffer.remove(id);
    }

    // Other helpers

    private void setupQuery(String id, Metadata metadata, Querier querier) {
        metrics.updateCount(createdQueriesCount, 1L);
        bufferedMetadata.put(id, metadata);
        // If the query should be post-finish buffered, it should not be pre-start delayed.
        if (querier.shouldBuffer()) {
            queries.put(id, querier);
            metrics.updateCount(activeQueriesCount, 1L);
            log.info("Received and started query {} : {}", querier.getRunningQuery().getId(), querier.getRunningQuery().getQueryString());
            log.debug("Received and started query {}", querier);
        } else {
            preStartBuffer.put(id, querier);
            log.info("Received but delaying starting query {}", id);
        }
    }

    private void startDelayed(Map.Entry<String, Querier> query) {
        String id = query.getKey();
        Querier querier = query.getValue();
        preStartBuffer.remove(id);
        // Make the query start again to mark the correct start for the query.
        querier.restart();
        queries.put(id, querier);
        // Now it's active
        metrics.updateCount(activeQueriesCount, 1L);
        log.info("Started delayed query {}", id);
    }

    private Metadata withSignal(Metadata metadata, Metadata.Signal signal) {
        // Don't change the non-readonly bits of metadata in place since that might affect tuples emitted but pending.
        if (metadata == null) {
            return new Metadata(signal, null);
        }
        Metadata copy = metadata.copy();
        copy.setSignal(signal);
        return copy;
    }

    private Querier getQuery(String id) {
        // JoinBolt has two regular places where the query might be.
        Querier query = queries.get(id);
        if (query == null) {
            log.debug("Query might be done: {}", id);
            query = postFinishBuffer.get(id);
        }
        if (query == null) {
            log.debug("Fetching delayed query {}", id);
            query = preStartBuffer.get(id);
        }
        return query;
    }
}
