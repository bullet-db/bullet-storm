/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.querying.AbstractQuery;
import lombok.Getter;
import backtype.storm.Config;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;

public class QueryBoltTest {

    private class TestQueryBolt extends QueryBolt<AbstractQuery> {
        @Getter
        private boolean cleaned = false;

        @Override
        public void execute(Tuple input) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public AbstractQuery getQuery(Long id, String queryString) {
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
        TestQueryBolt testQueryBolt = new TestQueryBolt();
        testQueryBolt.cleanup();
        Assert.assertTrue(testQueryBolt.isCleaned());
    }

    @Test
    public void testDefaultConfiguration() {
        Map<String, Object> expected = Collections.singletonMap(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
                                                                QueryBolt.DEFAULT_TICK_INTERVAL);
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
