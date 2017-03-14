/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.tracing;

import com.yahoo.bullet.parsing.ParsingException;
import com.yahoo.bullet.record.BulletRecord;

import java.util.Map;

public class FilterRule extends AbstractRule<BulletRecord, byte[]> {
    /**
     * Default constructor.
     *
     * @param input The rule as a String.
     * @param configuration A map of configurations to use.
     * @throws ParsingException if there was an issue.
     */
    public FilterRule(String input, Map configuration) throws ParsingException {
        super(input, configuration);
    }

    /**
     * {@inheritDoc}
     *
     * Returns the raw byte[] representation of the data. This could be projected records, Sketches or GroupData.
     *
     * @return a byte[] representation of the data
     */
    @Override
    public byte[] getData() {
        return specification.getSerializedAggregate();
    }

    @Override
    public boolean consume(BulletRecord record) {
        // If rule is expired, not accepting data or does not match filters, don't consume...
        if (isExpired() || !specification.isAcceptingData() || !specification.filter(record)) {
            return false;
        }
        specification.aggregate(specification.project(record));
        return specification.isMicroBatch();
    }
}
