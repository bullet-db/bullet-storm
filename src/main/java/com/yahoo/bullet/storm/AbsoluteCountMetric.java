/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import lombok.NoArgsConstructor;
import org.apache.storm.metric.api.IMetric;

@NoArgsConstructor
public class AbsoluteCountMetric implements IMetric {
    private long count;

    /**
     * Adds a long (can be negative) to the current count.
     *
     * @param value The count to add.
     */
    public void add(long value) {
        count += value;
    }

    @Override
    public Object getValueAndReset() {
        return count;
    }
}
