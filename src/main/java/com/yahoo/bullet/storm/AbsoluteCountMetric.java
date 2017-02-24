package com.yahoo.bullet.storm;

import lombok.NoArgsConstructor;
import backtype.storm.metric.api.IMetric;

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
