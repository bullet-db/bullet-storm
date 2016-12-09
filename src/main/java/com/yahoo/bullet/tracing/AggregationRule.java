/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.tracing;

import com.yahoo.bullet.parsing.ParsingException;
import com.yahoo.bullet.record.BulletRecord;
import lombok.Getter;

import java.util.List;
import java.util.Map;

public class AggregationRule extends AbstractRule<byte[], List<BulletRecord>> {
    @Getter
    protected long lastAggregationTime = 0L;

    /**
     * Constructor that takes a String representation of the rule.
     *
     * @param ruleString The rule as a string.
     * @param configuration A map of configurations to use.
     * @throws ParsingException if there was an issue.
     */
    public AggregationRule(String ruleString, Map configuration) throws ParsingException {
        super(ruleString, configuration);
    }

    @Override
    public boolean consume(byte[] data) {
        specification.aggregate(data);
        // If the specification is no longer accepting data, then the Rule has been satisfied.
        return !specification.isAcceptingData();
    }

    /**
     * {@inheritDoc}
     *
     * Get the aggregate so far.
     *
     * @return A non-null aggregated resulting List of {@link BulletRecord}.
     */
    @Override
    public List<BulletRecord> getData() {
        List<BulletRecord> result = specification.getAggregate();
        lastAggregationTime = System.currentTimeMillis();
        return result;
    }
}
