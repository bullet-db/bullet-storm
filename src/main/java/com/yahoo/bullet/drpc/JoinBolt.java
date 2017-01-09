/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.drpc;

import com.google.gson.JsonParseException;
import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.parsing.ParsingException;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.result.Metadata.Concept;
import com.yahoo.bullet.tracing.AggregationRule;
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
public class JoinBolt extends RuleBolt<AggregationRule> {
    public static final String JOIN_STREAM = Utils.DEFAULT_STREAM_ID;

    /** This is the default number of ticks for which we will buffer an individual error message. */
    public static final int DEFAULT_ERROR_TICKOUT = 3;
    /** This is the default number of ticks for which we will a rule post expiry. */
    public static final int DEFAULT_RULE_TICKOUT = 3;

    private Map<Long, Tuple> activeReturns;
    // For doing a LEFT OUTER JOIN between Rules and ReturnInfo if the Rule has validation issues
    private RotatingMap<Long, Clip> bufferedErrors;
    // For doing a LEFT OUTER JOIN between Rules and intermediate aggregation, if the aggregations are lagging.
    private RotatingMap<Long, AggregationRule> bufferedRules;

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

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        activeReturns = new HashMap<>();

        Number errorTickoutNumber = (Number) configuration.getOrDefault(BulletConfig.JOIN_BOLT_ERROR_TICK_TIMEOUT,
                                                                        DEFAULT_ERROR_TICKOUT);
        int errorTickout = errorTickoutNumber.intValue();
        bufferedErrors = new RotatingMap<>(errorTickout);

        Number ruleTickoutNumber = (Number) configuration.getOrDefault(BulletConfig.JOIN_BOLT_RULE_TICK_TIMEOUT,
                                                                       DEFAULT_RULE_TICKOUT);
        int ruleTickout = ruleTickoutNumber.intValue();
        bufferedRules = new RotatingMap<>(ruleTickout);
    }

    @Override
    public void execute(Tuple tuple) {
        TupleType.Type type = TupleType.classify(tuple).orElse(null);
        switch (type) {
            case TICK_TUPLE:
                handleTick();
                break;
            case RULE_TUPLE:
                initializeRule(tuple);
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
    protected AggregationRule getRule(Long id, String ruleString) {
        try {
            return new AggregationRule(ruleString, configuration);
        } catch (JsonParseException jpe) {
            emitError(id, com.yahoo.bullet.parsing.Error.makeError(jpe, ruleString));
        } catch (ParsingException pe) {
            emitError(id, pe.getErrors());
        } catch (RuntimeException re) {
            log.error("Unhandled exception.", re);
            emitError(id, Error.makeError(re, ruleString));
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

    private void handleTick() {
        // Buffer whatever we're retiring now and forceEmit all the bufferedRules that are being rotated out.
        // Whatever we're retiring now MUST not have been satisfied since we emit Rules when FILTER_TUPLES satisfy them.
        emitRetired(bufferedRules.rotate());
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
        if (returnTuple != null) {
            emit(returnValue, returnTuple);
            return;
        }
        log.debug("Return information not present for sending error. Buffering it...");
        bufferedErrors.put(id, returnValue);
    }

    private void emitRetired(Map<Long, AggregationRule> forceEmit) {
        // Force emit everything that was asked to be emitted if we can. These are rotated out rules from bufferedRules.
        for (Map.Entry<Long, AggregationRule> e : forceEmit.entrySet()) {
            Long id = e.getKey();
            AggregationRule rule = e.getValue();
            Tuple returnTuple = activeReturns.remove(id);
            if (canEmit(id, rule, returnTuple)) {
                emit(id, rule, returnTuple);
            }
        }
        // For the others that were just retired, roll them over into bufferedRules
        retireRules().forEach(bufferedRules::put);
    }

    private boolean canEmit(Long id, AggregationRule rule, Tuple returnTuple) {
        // Deliberately only doing joins if both rule and return are here. Can do an OUTER join if needed later...
        if (rule == null) {
            log.debug("Received tuples for request {} before rule or too late. Skipping...", id);
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

        // We have two places we could have Rules in
        AggregationRule rule = rulesMap.get(id);
        if (rule == null) {
            rule = bufferedRules.get(id);
        }

        emit(id, rule, activeReturns.get(id), data);
    }

    private void emit(Long id, AggregationRule rule, Tuple returnTuple, byte[] data) {
        if (!canEmit(id, rule, returnTuple)) {
            return;
        }
        // If the rule is not satisfied after consumption, we should not emit.
        if (!rule.consume(data)) {
            return;
        }
        emit(id, rule, returnTuple);
    }

    private void emit(Long id, AggregationRule rule, Tuple returnTuple) {
        Objects.requireNonNull(id);
        Objects.requireNonNull(rule);
        Objects.requireNonNull(returnTuple);

        // TODO Anchor this tuple to all tuples that caused its emission : rule tuple, return tuple, data tuple(s)
        Clip records = rule.getData();
        records.add(getMetadata(id, rule));
        emit(records, returnTuple);
        int emitted = records.getRecords().size();
        log.info("Rule {} has been satisfied with {} records. Cleaning up...", id, emitted);
        rulesMap.remove(id);
        bufferedRules.remove(id);
        activeReturns.remove(id);
    }

    private void emit(Clip clip, Tuple returnTuple) {
        Objects.requireNonNull(clip);
        Objects.requireNonNull(returnTuple);
        Object returnInfo = returnTuple.getValue(TopologyConstants.RETURN_POSITION);
        collector.emit(new Values(clip.asJSON(), returnInfo));
    }

    private Metadata getMetadata(Long id, AggregationRule rule) {
        if (metadataKeys.isEmpty()) {
            return null;
        }
        Metadata meta = new Metadata();
        consumeRegisteredConcept(Concept.RULE_ID, (k) -> meta.add(k, id));
        consumeRegisteredConcept(Concept.RULE_BODY, (k) -> meta.add(k, rule.toString()));
        consumeRegisteredConcept(Concept.CREATION_TIME, (k) -> meta.add(k, rule.getStartTime()));
        consumeRegisteredConcept(Concept.TERMINATION_TIME, (k) -> meta.add(k, rule.getLastAggregationTime()));
        return meta;
    }

    private void consumeRegisteredConcept(Concept concept, Consumer<String> action) {
        // Only consume the concept if we have a key for it: i.e. it was registered
        String key = metadataKeys.get(concept.getName());
        if (key != null) {
            action.accept(key);
        }
    }
}
