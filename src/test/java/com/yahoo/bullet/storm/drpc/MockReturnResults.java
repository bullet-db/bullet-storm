/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import backtype.storm.drpc.ReturnResults;
import backtype.storm.tuple.Tuple;
import com.yahoo.bullet.storm.drpc.utils.DRPCOutputCollector;
import lombok.Getter;
import lombok.Setter;

@Getter
public class MockReturnResults extends ReturnResults {
    private DRPCOutputCollector collector;
    private boolean cleanedUp = false;
    @Setter
    private int failNumber = 1;
    private int count = 0;

    public MockReturnResults(DRPCOutputCollector collector, int failEveryNTuples) {
        super();
        this.collector = collector;
        failNumber = failEveryNTuples;
    }

    @Override
    public void execute(Tuple input) {
        count++;
        if (count % failNumber == 0) {
            collector.fail(input);
        } else {
            collector.ack(input);
        }
    }

    @Override
    public void cleanup() {
        cleanedUp = true;
    }
}
