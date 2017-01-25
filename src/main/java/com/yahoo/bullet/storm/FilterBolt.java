/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.parsing.ParsingException;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.tracing.FilterRule;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

@Slf4j
public class FilterBolt extends RuleBolt<FilterRule> {
    public static final String FILTER_STREAM = Utils.DEFAULT_STREAM_ID;
    private String recordComponent;

    /**
     * Default constructor.
     */
    public FilterBolt() {
        this(TopologyConstants.RECORD_COMPONENT, RuleBolt.DEFAULT_TICK_INTERVAL);
    }

    /**
     * Constructor that accepts the name of the component that the records are coming from.
     * @param recordComponent The source component name for records.
     */
    public FilterBolt(String recordComponent) {
        this(recordComponent, RuleBolt.DEFAULT_TICK_INTERVAL);
    }

    /**
     * Constructor that accepts the name of the component that the records are coming from and the tick interval.
     * @param recordComponent The source component name for records.
     * @param tickInterval The tick interval in seconds.
     */
    public FilterBolt(String recordComponent, Integer tickInterval) {
        super(tickInterval);
        this.recordComponent = recordComponent;
    }

    private TupleType.Type getCustomType(Tuple tuple) {
        return recordComponent.equals(tuple.getSourceComponent()) ? TupleType.Type.RECORD_TUPLE : null;
    }

    @Override
    public void execute(Tuple tuple) {
        // If it isn't any of our default TupleTypes, check if the component is from our custom source
        TupleType.Type type = TupleType.classify(tuple).orElse(getCustomType(tuple));
        switch (type) {
            case TICK_TUPLE:
                emitForRules(retireRules());
                break;
            case RULE_TUPLE:
                initializeRule(tuple);
                break;
            case RECORD_TUPLE:
                checkRule(tuple);
                break;
            default:
                // May want to throw an error here instead of not acking
                log.error("Unknown tuple encountered: {}", type);
                return;
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TopologyConstants.ID_FIELD, TopologyConstants.RECORD_FIELD));
    }

    @Override
    protected FilterRule getRule(Long id, String ruleString) {
        // No need to handle any errors here. The JoinBolt reports all errors.
        try {
            return new FilterRule(ruleString, configuration);
        } catch (ParsingException | RuntimeException e) {
            return null;
        }
    }

    private void checkRule(Tuple tuple) {
        BulletRecord record = (BulletRecord) tuple.getValue(0);
        // TODO Consider how to anchor this tuple
        // For each rule that is satisfied, we will emit the data but we will not expire the rule.
        rulesMap.entrySet().stream().filter(e -> e.getValue().consume(record)).forEach(this::emitForRule);
    }

    private void emitForRules(Map<Long, FilterRule> entries) {
        entries.entrySet().stream().forEach(this::emitForRule);
    }

    private void emitForRule(Map.Entry<Long, FilterRule> pair) {
        // The FilterRule will handle giving us the right data - a byte[] to emit
        byte[] data = pair.getValue().getData();
        if (data != null) {
            collector.emit(new Values(pair.getKey(), data));
        }
    }
}
