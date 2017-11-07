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

    /** This is the default number of ticks for which we will buffer a query. */
    public static final int DEFAULT_QUERY_TICKOUT = 3;

    private Map<String, com.yahoo.bullet.pubsub.Metadata> bufferedMetadata;
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
                handleQuery(tuple);
                break;
            case FILTER_TUPLE:
                handleFilter(tuple);
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
    protected AggregationQuery createQuery(Tuple queryTuple) {
        String queryString = queryTuple.getString(TopologyConstants.QUERY_POSITION);
        try {
            return new AggregationQuery(queryString, configuration);
        } catch (JsonParseException jpe) {
            emitError(queryTuple, Arrays.asList(Error.makeError(jpe, queryString)));
        } catch (ParsingException pe) {
            emitError(queryTuple, pe.getErrors());
        } catch (RuntimeException re) {
            log.error("Unhandled exception.", re);
            emitError(queryTuple, Arrays.asList(Error.makeError(re, queryString)));
        }
        return null;
    }

    private void emitError(Tuple queryTuple, List<Error> errors) {
        Metadata meta = Metadata.of(errors);
        Clip clip = Clip.of(meta);
        updateCount(improperQueriesCount, 1L);
        collector.emit(new Values(queryTuple.getString(TopologyConstants.ID_POSITION), clip.asJSON(), getMetadata(queryTuple)));
    }

    private void handleQuery(Tuple tuple) {
        AggregationQuery query = initializeQuery(tuple);
        if (query != null) {
            bufferedMetadata.put(tuple.getString(TopologyConstants.ID_POSITION), getMetadata(tuple));
            updateCount(createdQueriesCount, 1L);
            updateCount(activeQueriesCount, 1L);
        }
    }

    private void handleTick() {
        // Buffer whatever we're retiring now and forceEmit all the bufferedQueries that are being rotated out.
        // Whatever we're retiring now MUST not have been satisfied since we emit Queries when FILTER_TUPLES satisfy them.
        Map<String, AggregationQuery> forceEmit = bufferedQueries.rotate();
        long emitted = 0;
        for (Map.Entry<String, AggregationQuery> e : forceEmit.entrySet()) {
            String id = e.getKey();
            AggregationQuery query = e.getValue();

            if (!isNull(query, id)) {
                emitted++;
                emit(query, id, bufferedMetadata.remove(id));
            }
        }
        // We already decreased activeQueriesCount by emitted. The others that are thrown away should decrease the count too.
        updateCount(activeQueriesCount, -forceEmit.size() + emitted);

        // For the others that were just retired, roll them over into bufferedQueries
        retireQueries().forEach(bufferedQueries::put);
    }

    private void handleFilter(Tuple filterTuple) {
        AggregationQuery query = getQueryFromMaps(filterTuple);
        String id = filterTuple.getString(TopologyConstants.ID_POSITION);
        if (isNull(query, id)) {
            return;
        }

        byte[] data = (byte[]) filterTuple.getValue(TopologyConstants.RECORD_POSITION);
        if (query.consume(data)) {
            emit(query, id, bufferedMetadata.get(id));
        }
    }

    private AggregationQuery getQueryFromMaps(Tuple tuple) {
        String id = tuple.getString(TopologyConstants.ID_POSITION);

        // JoinBolt has two places where the query might be
        AggregationQuery query = queriesMap.get(id);
        if (query == null) {
            query = bufferedQueries.get(id);
        }
        return query;
    }

    private boolean isNull(AggregationQuery query, String id) {
        if (query == null) {
            log.debug("Received tuples for request {} before query or too late. Skipping...", id);
            return true;
        }
        return false;
    }

    private void emit(AggregationQuery query, String id, com.yahoo.bullet.pubsub.Metadata metadata) {
        Objects.requireNonNull(id);
        Objects.requireNonNull(query);

        Clip records = query.getData();
        records.add(getMetadata(id, query));
        collector.emit(new Values(id, records.asJSON(), metadata));
        int emitted = records.getRecords().size();
        log.info("Query {} has been satisfied with {} records. Cleaning up...", id, emitted);
        queriesMap.remove(id);
        bufferedQueries.remove(id);
        bufferedMetadata.remove(id);
        updateCount(activeQueriesCount, -1L);
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

    private com.yahoo.bullet.pubsub.Metadata getMetadata(Tuple queryTuple) {
        return (com.yahoo.bullet.pubsub.Metadata) queryTuple.getValue(TopologyConstants.METADATA_POSITION);
    }
}
