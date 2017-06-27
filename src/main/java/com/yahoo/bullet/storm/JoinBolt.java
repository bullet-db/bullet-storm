/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.google.gson.JsonParseException;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.parsing.ParsingException;
import com.yahoo.bullet.querying.AggregationQuery;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.result.Metadata.Concept;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

@Slf4j
public class JoinBolt extends QueryBolt<AggregationQuery> {
    public static final String JOIN_STREAM = Utils.DEFAULT_STREAM_ID;

    /** This is the default number of ticks for which we will buffer an individual error message. */
    public static final int DEFAULT_ERROR_TICKOUT = 3;
    /** This is the default number of ticks for which we will a query post expiry. */
    public static final int DEFAULT_QUERY_TICKOUT = 3;

    private Map<Long, Tuple> activeReturns;
    // For doing a LEFT OUTER JOIN between Queries and ReturnInfo if the Query has validation issues
    private RotatingMap<Long, Clip> bufferedErrors;
    // For doing a LEFT OUTER JOIN between Queries and intermediate aggregation, if the aggregations are lagging.
    private RotatingMap<Long, AggregationQuery> bufferedQueries;

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

        activeReturns = new HashMap<>();

        Number errorTickoutNumber = (Number) configuration.getOrDefault(BulletStormConfig.JOIN_BOLT_ERROR_TICK_TIMEOUT,
                                                                        DEFAULT_ERROR_TICKOUT);
        int errorTickout = errorTickoutNumber.intValue();
        bufferedErrors = new RotatingMap<>(errorTickout);

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
                handleQuery(tuple);
                break;
            case RETURN_TUPLE:
                initializeReturn(tuple);
                break;
            case FILTER_TUPLE:
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
        declarer.declare(new Fields(TopologyConstants.JOIN_FIELD, TopologyConstants.RETURN_FIELD));
    }

    @Override
    protected AggregationQuery getQuery(Long id, String queryString) {
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

    private void initializeReturn(Tuple tuple) {
        Long id = tuple.getLong(TopologyConstants.ID_POSITION);
        // Check if we have any buffered errors.
        Clip error = bufferedErrors.get(id);
        if (error != null) {
            emit(error, tuple);
            return;
        }
        // Otherwise buffer the return information
        activeReturns.put(id, tuple);
    }

    private void handleQuery(Tuple tuple) {
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
        // We'll just rotate and lose any buffered errors (if rotated enough times) as designed.
        bufferedErrors.rotate();
    }

    private void emitError(Long id, Error... errors) {
        emitError(id, Arrays.asList(errors));
    }

    private void emitError(Long id, List<Error> errors) {
        Metadata meta = Metadata.of(errors);
        Clip returnValue = Clip.of(meta);
        Tuple returnTuple = activeReturns.remove(id);
        updateCount(improperQueriesCount, 1L);

        if (returnTuple != null) {
            emit(returnValue, returnTuple);
            return;
        }
        log.debug("Return information not present for sending error. Buffering it...");
        bufferedErrors.put(id, returnValue);
    }

    private void emitRetired(Map<Long, AggregationQuery> forceEmit) {
        // Force emit everything that was asked to be emitted if we can. These are rotated out queries from bufferedQueries.
        long emitted = 0;
        for (Map.Entry<Long, AggregationQuery> e : forceEmit.entrySet()) {
            Long id = e.getKey();
            AggregationQuery query = e.getValue();
            Tuple returnTuple = activeReturns.remove(id);
            if (canEmit(id, query, returnTuple)) {
                emitted++;
                emit(id, query, returnTuple);
            }
        }
        // We already decreased activeQueriesCount by emitted. The others that are thrown away should decrease the count too.
        updateCount(activeQueriesCount, -forceEmit.size() + emitted);

        // For the others that were just retired, roll them over into bufferedQueries
        retireQueries().forEach(bufferedQueries::put);
    }

    private boolean canEmit(Long id, AggregationQuery query, Tuple returnTuple) {
        // Deliberately only doing joins if both query and return are here. Can do an OUTER join if needed later...
        if (query == null) {
            log.debug("Received tuples for request {} before query or too late. Skipping...", id);
            return false;
        }
        if (returnTuple == null) {
            log.debug("Received tuples for request {} before return information. Skipping...", id);
            return false;
        }
        return true;
    }

    private void emit(Tuple tuple) {
        Long id = tuple.getLong(TopologyConstants.ID_POSITION);
        byte[] data = (byte[]) tuple.getValue(TopologyConstants.RECORD_POSITION);

        // We have two places we could have Queries in
        AggregationQuery query = queriesMap.get(id);
        if (query == null) {
            query = bufferedQueries.get(id);
        }

        emit(id, query, activeReturns.get(id), data);
    }

    private void emit(Long id, AggregationQuery query, Tuple returnTuple, byte[] data) {
        if (!canEmit(id, query, returnTuple)) {
            return;
        }
        // If the query is not satisfied after consumption, we should not emit.
        if (!query.consume(data)) {
            return;
        }
        emit(id, query, returnTuple);
    }

    private void emit(Long id, AggregationQuery query, Tuple returnTuple) {
        Objects.requireNonNull(id);
        Objects.requireNonNull(query);
        Objects.requireNonNull(returnTuple);

        Clip records = query.getData();
        records.add(getMetadata(id, query));
        emit(records, returnTuple);
        int emitted = records.getRecords().size();
        log.info("Query {} has been satisfied with {} records. Cleaning up...", id, emitted);
        queriesMap.remove(id);
        bufferedQueries.remove(id);
        activeReturns.remove(id);
        updateCount(activeQueriesCount, -1L);
    }

    private void emit(Clip clip, Tuple returnTuple) {
        Objects.requireNonNull(clip);
        Objects.requireNonNull(returnTuple);
        Object returnInfo = returnTuple.getValue(TopologyConstants.RETURN_POSITION);
        collector.emit(new Values(clip.asJSON(), returnInfo));
    }

    private Metadata getMetadata(Long id, AggregationQuery query) {
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
        Number interval = metricsIntervalMapping.getOrDefault(name,
                                                              metricsIntervalMapping.get(DEFAULT_METRICS_INTERVAL_KEY));
        log.info("Registered {} with interval {}", name, interval);
        return context.registerMetric(name, new AbsoluteCountMetric(), interval.intValue());
    }
}
