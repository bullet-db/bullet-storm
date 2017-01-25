/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.tracing.AbstractRule;
import lombok.Getter;
import org.apache.storm.Config;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;

public class RuleBoltTest {

    private class TestRuleBolt extends RuleBolt<AbstractRule> {
        @Getter
        private boolean cleaned = false;

        @Override
        public void execute(Tuple input) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public AbstractRule getRule(Long id, String ruleString) {
            return null;
        }

        @Override
        public void cleanup() {
            super.cleanup();
            cleaned = true;
        }
    }

    @Test
    public void testCleanup() {
        TestRuleBolt testRuleBolt = new TestRuleBolt();
        testRuleBolt.cleanup();
        Assert.assertTrue(testRuleBolt.isCleaned());
    }

    @Test
    public void testDefaultConfiguration() {
        Map<String, Object> expected = Collections.singletonMap(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
                                                                RuleBolt.DEFAULT_TICK_INTERVAL);
        JoinBolt joinBolt = new JoinBolt();
        Assert.assertEquals(joinBolt.getComponentConfiguration(), expected);
        joinBolt = new JoinBolt(null);
        Assert.assertEquals(joinBolt.getComponentConfiguration(), expected);
    }

    @Test
    public void testCustomConfiguration() {
        Map<String, Object> expected = Collections.singletonMap(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 88);
        JoinBolt joinBolt = new JoinBolt(88);
        Assert.assertEquals(joinBolt.getComponentConfiguration(), expected);
    }
}
