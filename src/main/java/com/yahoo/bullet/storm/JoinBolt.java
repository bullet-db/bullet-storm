/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RotatingMap;
import backtype.storm.utils.Utils;
import com.google.gson.JsonParseException;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.parsing.ParsingException;
import com.yahoo.bullet.querying.AggregationQuery;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.result.Metadata.Concept;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

@Slf4j
public class JoinBolt extends QueryBolt<AggregationQuery> {
    public static final String JOIN_STREAM = Utils.DEFAULT_STREAM_ID;
    public static final String JOIN_FIELD = "result";

    /** This is the default number of ticks for which we will buffer an individual error message. */
    public static final int DEFAULT_ERROR_TICKOUT = 3;
    /** This is the default number of ticks for which we will buffer a query. */
    public static final int DEFAULT_QUERY_TICKOUT = 3;

    private Map<String, Tuple> bufferedMetadata;
    // For doing a LEFT OUTER JOIN between Queries and intermediate aggregation, if the aggregations are lagging.
    private RotatingMap<String, AggregationQuery> bufferedQueries;

    // Metrics
    public static final String ACTIVE_QUERIES = TopologyConstants.METRIC_PREFIX + "active_queries";
    public static final String CREATED_QUERIES = TopologyConstants.METRIC_PREFIX + "created_queries";
    public static final String IMPROPER_QUERIES = TopologyConstants.METRIC_PREFIX + "improper_queries";
    // Variable
    private transient AbsoluteCountMetric activeQueriesCount;
    // Monotonically increasing
    private transient AbsoluteCountMetric createdQueriesCount;
    private transient AbsoluteCountMetric improperQueriesCount;

    /**
     * Default constructor.
     */
    public JoinBolt() {
        super();
    }

    /**
     * Constructor that accepts the tick interval.
     * @param tickInterval The tick interval in seconds.
     */
    public JoinBolt(Integer tickInterval) {
        super(tickInterval);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        bufferedMetadata = new HashMap<>();

        Number queryTickoutNumber = (Number) configuration.getOrDefault(BulletStormConfig.JOIN_BOLT_QUERY_TICK_TIMEOUT,
                                                                        DEFAULT_QUERY_TICKOUT);
        int queryTickout = queryTickoutNumber.intValue();
        bufferedQueries = new RotatingMap<>(queryTickout);

        if (metricsEnabled) {
            activeQueriesCount = registerAbsoluteCountMetric(ACTIVE_QUERIES, context);
            createdQueriesCount = registerAbsoluteCountMetric(CREATED_QUERIES, context);
            improperQueriesCount = registerAbsoluteCountMetric(IMPROPER_QUERIES, context);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        TupleType.Type type = TupleType.classify(tuple).orElse(null);
        switch (type) {
            case TICK_TUPLE:
                handleTick();
                break;
            case QUERY_TUPLE:
                log.info("got query_tuple in joinBolt");  // <--- REMOVE THIS
                handleQuery(tuple);
                break;
            case FILTER_TUPLE:
                log.info("got filter_tuple in joinBolt");  // <--- REMOVE THIS
                emit(tuple);
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
        declarer.declare(new Fields(TopologyConstants.ID_FIELD, JOIN_FIELD, TopologyConstants.METADATA_FIELD));
    }

    @Override
    protected AggregationQuery getQuery(String id, String queryString) {
        try {
            return new AggregationQuery(queryString, configuration);
        } catch (JsonParseException jpe) {
            emitError(id, com.yahoo.bullet.parsing.Error.makeError(jpe, queryString));
        } catch (ParsingException pe) {
            emitError(id, pe.getErrors());
        } catch (RuntimeException re) {
            log.error("Unhandled exception.", re);
            emitError(id, Error.makeError(re, queryString));
        }
        return null;
    }

    private void handleQuery(Tuple tuple) {
        bufferedMetadata.put(tuple.getString(TopologyConstants.ID_POSITION), tuple);
        AggregationQuery query = initializeQuery(tuple);
        if (query != null) {
            updateCount(createdQueriesCount, 1L);
            updateCount(activeQueriesCount, 1L);
        }
    }

    private void handleTick() {
        // Buffer whatever we're retiring now and forceEmit all the bufferedQueries that are being rotated out.
        // Whatever we're retiring now MUST not have been satisfied since we emit Queries when FILTER_TUPLES satisfy them.
        emitRetired(bufferedQueries.rotate());
    }

    private void emitError(String id, Error... errors) {
        emitError(id, Arrays.asList(errors));
    }

    private void emitError(String id, List<Error> errors) {
        Metadata meta = Metadata.of(errors);
        Clip clip = Clip.of(meta);
        Tuple queryTuple = bufferedMetadata.remove(id);
        updateCount(improperQueriesCount, 1L);
        emit(clip, queryTuple);
    }

    private void emitRetired(Map<String, AggregationQuery> forceEmit) {
        // Force emit everything that was asked to be emitted if we can. These are rotated out queries from bufferedQueries.
        long emitted = 0;
        for (Map.Entry<String, AggregationQuery> e : forceEmit.entrySet()) {
            String id = e.getKey();
            AggregationQuery query = e.getValue();
            Tuple queryTuple = bufferedMetadata.remove(id);
            if (canEmit(id, query, queryTuple)) {
                emitted++;
                emit(id, query, queryTuple);
            }
        }
        // We already decreased activeQueriesCount by emitted. The others that are thrown away should decrease the count too.
        updateCount(activeQueriesCount, -forceEmit.size() + emitted);

        // For the others that were just retired, roll them over into bufferedQueries
        retireQueries().forEach(bufferedQueries::put);
    }

    private boolean canEmit(String id, AggregationQuery query, Tuple queryTuple) {
        // Deliberately only doing joins if both query and return are here. Can do an OUTER join if needed later...
        if (query == null) {
            log.debug("Received tuples for request {} before query or too late. Skipping...", id);
            log.info("Received tuples for request {} before query or too late. Skipping...", id); // <--- REMOVE THIS
            return false;
        }
        if (queryTuple == null) {
            log.debug("Received tuples for request {} before return information. Skipping...", id);
            log.info("Received tuples for request {} before return information. Skipping...", id); // <--- REMOVE THIS
            return false;
        }
        log.info("in canEmit - returning true");  // <--- REMOVE THIS
        return true;
    }

    private void emit(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);
        byte[] data = (byte[]) tuple.getValue(TopologyConstants.RECORD_POSITION);

        // We have two places we could have Queries in
        AggregationQuery query = queriesMap.get(id);
        if (query == null) {
            query = bufferedQueries.get(id);
        }

        emit(id, query, bufferedMetadata.get(id), data);
    }

    private void emit(String id, AggregationQuery query, Tuple queryTuple, byte[] data) {
        if (!canEmit(id, query, queryTuple)) {
            return;
        }
        // If the query is not satisfied after consumption, we should not emit.
        if (!query.consume(data)) {
            return;
        }
        emit(id, query, queryTuple);
    }

    private void emit(String id, AggregationQuery query, Tuple queryTuple) {
        Objects.requireNonNull(id);
        Objects.requireNonNull(query);
        Objects.requireNonNull(queryTuple);

        Clip records = query.getData();
        records.add(getMetadata(id, query));
        emit(records, queryTuple);
        int emitted = records.getRecords().size();
        log.info("Query {} has been satisfied with {} records. Cleaning up...", id, emitted);
        queriesMap.remove(id);
        bufferedQueries.remove(id);
        bufferedMetadata.remove(id);
        updateCount(activeQueriesCount, -1L);
    }

    private void emit(Clip clip, Tuple queryTuple) {
        Objects.requireNonNull(clip);
        Objects.requireNonNull(queryTuple);
        String id = queryTuple.getString(TopologyConstants.ID_POSITION);
        com.yahoo.bullet.pubsub.Metadata metadata = (com.yahoo.bullet.pubsub.Metadata) queryTuple.getValue(TopologyConstants.METADATA_POSITION);
        collector.emit(new Values(id, clip.asJSON(), metadata));
    }

    private Metadata getMetadata(String id, AggregationQuery query) {
        if (metadataKeys.isEmpty()) {
            return null;
        }
        Metadata meta = new Metadata();
        consumeRegisteredConcept(Concept.QUERY_ID, (k) -> meta.add(k, id));
        consumeRegisteredConcept(Concept.QUERY_BODY, (k) -> meta.add(k, query.toString()));
        consumeRegisteredConcept(Concept.QUERY_CREATION_TIME, (k) -> meta.add(k, query.getStartTime()));
        consumeRegisteredConcept(Concept.QUERY_TERMINATION_TIME, (k) -> meta.add(k, query.getLastAggregationTime()));
        return meta;
    }

    private void consumeRegisteredConcept(Concept concept, Consumer<String> action) {
        // Only consume the concept if we have a key for it: i.e. it was registered
        String key = metadataKeys.get(concept.getName());
        if (key != null) {
            action.accept(key);
        }
    }

    private void updateCount(AbsoluteCountMetric metric, long updateValue) {
        if (metricsEnabled) {
            metric.add(updateValue);
        }
    }

    private AbsoluteCountMetric registerAbsoluteCountMetric(String name, TopologyContext context) {
        Number interval = metricsIntervalMapping.getOrDefault(name, metricsIntervalMapping.get(DEFAULT_BUILT_IN_METRICS_INTERVAL_KEY));
        log.info("Registered {} with interval {}", name, interval);
        return context.registerMetric(name, new AbsoluteCountMetric(), interval.intValue());
    }
}
