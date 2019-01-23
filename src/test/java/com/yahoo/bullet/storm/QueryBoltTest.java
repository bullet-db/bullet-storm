/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.Metadata.Signal;
import com.yahoo.bullet.querying.Querier;
import com.yahoo.bullet.storm.TupleClassifier.Type;
import com.yahoo.bullet.storm.testing.ComponentUtils;
import com.yahoo.bullet.storm.testing.CustomCollector;
import com.yahoo.bullet.storm.testing.CustomTopologyContext;
import lombok.Getter;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.yahoo.bullet.storm.testing.TupleUtils.makeIDTuple;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class QueryBoltTest {
    @Getter
    private static class TestQueryBolt extends QueryBolt {
        private boolean cleaned = false;
        private ReducedMetric averagingMetric;
        private AbsoluteCountMetric countMetric;
        private int tupleCount;
        private Map<String, Querier> queries;

        TestQueryBolt(BulletStormConfig config) {
            super(config);
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            super.prepare(stormConf, context, collector);
            queries = new HashMap<>();
            averagingMetric = registerAveragingMetric("foo", context);
            countMetric = registerAbsoluteCountMetric("bar", context);
        }

        @Override
        public void execute(Tuple input) {
            tupleCount++;
            updateCount(countMetric, 1L);
            if (metricsEnabled) {
                // (1 + 2 + 3 + 4 + ...) / (1 + 1 + 1 + 1 + ...)
                averagingMetric.update(tupleCount);
            }
            if (input != null) {
                onMeta(input);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void cleanup() {
            super.cleanup();
            cleaned = true;
        }

        @Override
        protected void removeQuery(String id) {
            queries.remove(id);
        }

        boolean isMetricsEnabled() {
            return metricsEnabled;
        }

        Map<String, Number> getMetricsMapping() {
            return metricsIntervalMapping;
        }

        OutputCollector getCollector() {
            return collector;
        }

        TupleClassifier getClassifier() {
            return classifier;
        }

        Map<String, Querier> getQueries() {
            return queries;
        }
    }

    @Test
    public void testCleanup() {
        TestQueryBolt bolt = new TestQueryBolt(new BulletStormConfig());
        bolt.cleanup();
        Assert.assertTrue(bolt.isCleaned());
    }

    @Test
    public void testPrepare() {
        CustomCollector collector = new CustomCollector();
        TestQueryBolt bolt = new TestQueryBolt(new BulletStormConfig());
        ComponentUtils.prepare(bolt, collector);

        Assert.assertEquals(collector.getAckedCount(), 0);
        bolt.getCollector().ack(null);
        Assert.assertEquals(collector.getAckedCount(), 1);

        Assert.assertEquals(bolt.getQueries().size(), 0);
        Assert.assertEquals(bolt.getMetricsMapping(), BulletStormConfig.DEFAULT_TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING);
        Assert.assertEquals(bolt.isMetricsEnabled(), BulletStormConfig.DEFAULT_TOPOLOGY_METRICS_ENABLE);

        Tuple tuple = mock(Tuple.class);
        doReturn(TopologyConstants.RECORD_COMPONENT).when(tuple).getSourceComponent();
        Assert.assertEquals(bolt.getClassifier().classify(tuple), Optional.of(Type.RECORD_TUPLE));

        // coverage
        Assert.assertEquals(Type.valueOf("RECORD_TUPLE"), Type.RECORD_TUPLE);
    }

    @Test
    public void testMetricsUpdateOnMetricsDisabled() {
        CustomTopologyContext context = new CustomTopologyContext();
        CustomCollector collector = new CustomCollector();
        TestQueryBolt bolt = new TestQueryBolt(new BulletStormConfig());
        ComponentUtils.prepare(new HashMap<>(), bolt, context, collector);

        Assert.assertFalse(bolt.isMetricsEnabled());
        bolt.execute(null);

        IMetric averager = context.getRegisteredMetricByName("foo");
        IMetric counter = context.getRegisteredMetricByName("bar");
        Assert.assertNull(averager.getValueAndReset());
        Assert.assertEquals(counter.getValueAndReset(), 0L);
    }

    @Test
    public void testMetricsUpdateOnMetricsEnabled() {
        CustomTopologyContext context = new CustomTopologyContext();
        CustomCollector collector = new CustomCollector();
        BulletStormConfig config = new BulletStormConfig();
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();

        TestQueryBolt bolt = new TestQueryBolt(config);
        ComponentUtils.prepare(new HashMap<>(), bolt, context, collector);

        Assert.assertTrue(bolt.isMetricsEnabled());
        bolt.execute(null);

        IMetric averager = context.getRegisteredMetricByName("foo");
        IMetric counter = context.getRegisteredMetricByName("bar");
        Assert.assertEquals(averager.getValueAndReset(), 1.0);
        Assert.assertEquals(counter.getValueAndReset(), 1L);
    }

    @Test
    public void testMetaTupleRemovingQueries() {
        CustomCollector collector = new CustomCollector();
        TestQueryBolt bolt = new TestQueryBolt(new BulletStormConfig());
        ComponentUtils.prepare(bolt, collector);

        Map<String, Querier> queries = bolt.getQueries();
        queries.put("foo", null);

        Tuple complete = makeIDTuple(Type.METADATA_TUPLE, "foo", new Metadata(Signal.COMPLETE, null));
        bolt.execute(complete);
        Assert.assertFalse(queries.containsKey("foo"));

        queries.put("foo", null);
        Tuple fail = makeIDTuple(Type.METADATA_TUPLE, "foo", new Metadata(Signal.KILL, null));
        bolt.execute(fail);
        Assert.assertFalse(queries.containsKey("foo"));
    }

    @Test
    public void testRegularMetaTupleIgnored() {
        CustomCollector collector = new CustomCollector();
        TestQueryBolt bolt = new TestQueryBolt(new BulletStormConfig());
        ComponentUtils.prepare(bolt, collector);

        Map<String, Querier> queries = bolt.getQueries();
        queries.put("foo", null);

        Tuple meta = makeIDTuple(Type.METADATA_TUPLE, "foo", new Metadata(Signal.ACKNOWLEDGE, null));
        bolt.execute(meta);
        Assert.assertTrue(queries.containsKey("foo"));
    }

    @Test
    public void testNullMetaTupleIgnored() {
        CustomCollector collector = new CustomCollector();
        TestQueryBolt bolt = new TestQueryBolt(new BulletStormConfig());
        ComponentUtils.prepare(bolt, collector);

        Map<String, Querier> queries = bolt.getQueries();
        queries.put("foo", null);

        Tuple meta = makeIDTuple(Type.METADATA_TUPLE, "foo", null);
        bolt.execute(meta);
        Assert.assertTrue(queries.containsKey("foo"));
    }
}
