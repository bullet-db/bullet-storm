/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.Metadata.Signal;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.querying.Querier;
import com.yahoo.bullet.storm.TupleClassifier.Type;
import com.yahoo.bullet.storm.metric.AbsoluteCountMetric;
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

import static com.yahoo.bullet.query.QueryUtils.makeSimpleAggregationFieldFilterQuery;
import static com.yahoo.bullet.storm.TopologyConstants.FEEDBACK_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_BATCH_POSITION;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_INDEX_POSITION;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_TIMESTAMP_POSITION;
import static com.yahoo.bullet.storm.testing.TupleUtils.makeIDTuple;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryBoltTest {
    @Getter
    private static class TestQueryBolt extends QueryBolt {
        private boolean cleaned = false;
        private ReducedMetric averagingMetric;
        private AbsoluteCountMetric countMetric;
        private int tupleCount;
        private Map<String, Querier> queries;
        private int initializedQueryCount;

        TestQueryBolt(BulletStormConfig config) {
            super(config);
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            super.prepare(stormConf, context, collector);
            queries = new HashMap<>();
            averagingMetric = metrics.registerAveragingMetric("foo", context);
            countMetric = metrics.registerAbsoluteCountMetric("bar", context);
        }

        @Override
        public void execute(Tuple input) {
            tupleCount++;
            metrics.updateCount(countMetric, 1L);
            if (metrics.isEnabled()) {
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
        protected void initializeQuery(PubSubMessage message) {
            initializedQueryCount++;
        }

        @Override
        protected void removeQuery(String id) {
            queries.remove(id);
        }

        boolean isMetricsEnabled() {
            return metrics.isEnabled();
        }

        Map<String, Number> getMetricsMapping() {
            return metrics.getMetricsIntervalMapping();
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

    @Test
    public void testBatchInitializeQuery() {
        CustomCollector collector = new CustomCollector();
        TestQueryBolt bolt = new TestQueryBolt(new BulletStormConfig("src/test/resources/test_config.yaml"));
        ComponentUtils.prepare(bolt, collector);

        Assert.assertEquals(bolt.replayedQueriesCount, 0);
        Assert.assertEquals(bolt.initializedQueryCount, 0);

        Map<String, PubSubMessage> batch = new HashMap<>();

        makeSimpleAggregationFieldFilterQuery("b235gf23b", 5, Window.Unit.RECORD, 1, Window.Unit.RECORD, 1);

        batch.put("42", new PubSubMessage("42", SerializerDeserializer.toBytes(makeSimpleAggregationFieldFilterQuery("b235gf23b", 5, Window.Unit.RECORD, 1, Window.Unit.RECORD, 1)), new Metadata()));
        batch.put("43", new PubSubMessage("43", SerializerDeserializer.toBytes(makeSimpleAggregationFieldFilterQuery("b235gf23b", 5, Window.Unit.RECORD, 1, Window.Unit.RECORD, 1)), new Metadata()));

        Tuple tuple = makeIDTuple(TupleClassifier.Type.BATCH_TUPLE, "FilterBolt-18");
        when(tuple.getLong(REPLAY_TIMESTAMP_POSITION)).thenReturn(bolt.startTimestamp);
        when(tuple.getInteger(REPLAY_INDEX_POSITION)).thenReturn(0);
        when(tuple.getValue(REPLAY_BATCH_POSITION)).thenReturn(batch);
        bolt.onBatch(tuple);

        Assert.assertEquals(bolt.replayedQueriesCount, 2);
        Assert.assertEquals(bolt.initializedQueryCount, 2);
    }

    @Test
    public void testBatchReplayCompleted() {
        CustomTopologyContext context = new CustomTopologyContext();
        CustomCollector collector = new CustomCollector();
        BulletStormConfig config = new BulletStormConfig("src/test/resources/test_config.yaml");
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        TestQueryBolt bolt = new TestQueryBolt(config);
        ComponentUtils.prepare(new HashMap<>(), bolt, context, collector);

        bolt.replayCompleted = true;

        Tuple tuple = makeIDTuple(TupleClassifier.Type.BATCH_TUPLE, "FilterBolt-18");
        bolt.onBatch(tuple);

        Assert.assertEquals(bolt.replayedQueriesCount, 0);
        Assert.assertEquals(bolt.initializedQueryCount, 0);
    }

    @Test
    public void testBatchNonMatchingTimestamp() {
        CustomTopologyContext context = new CustomTopologyContext();
        CustomCollector collector = new CustomCollector();
        BulletStormConfig config = new BulletStormConfig("src/test/resources/test_config.yaml");
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        TestQueryBolt bolt = new TestQueryBolt(config);
        ComponentUtils.prepare(new HashMap<>(), bolt, context, collector);

        Tuple tuple = makeIDTuple(TupleClassifier.Type.BATCH_TUPLE, "FilterBolt-18");
        when(tuple.getLong(REPLAY_TIMESTAMP_POSITION)).thenReturn(0L);
        when(tuple.getInteger(REPLAY_INDEX_POSITION)).thenReturn(0);
        when(tuple.getValue(REPLAY_BATCH_POSITION)).thenReturn(null);
        bolt.onBatch(tuple);

        Assert.assertEquals(bolt.replayedQueriesCount, 0);
        Assert.assertEquals(bolt.initializedQueryCount, 0);
    }

    @Test
    public void testBatchNullEndsReplay() {
        CustomCollector collector = new CustomCollector();
        TestQueryBolt bolt = new TestQueryBolt(new BulletStormConfig("src/test/resources/test_config.yaml"));
        ComponentUtils.prepare(bolt, collector);

        Assert.assertFalse(bolt.replayCompleted);

        Tuple tuple = makeIDTuple(TupleClassifier.Type.BATCH_TUPLE, "FilterBolt-18");
        when(tuple.getLong(REPLAY_TIMESTAMP_POSITION)).thenReturn(bolt.startTimestamp);
        when(tuple.getInteger(REPLAY_INDEX_POSITION)).thenReturn(0);
        when(tuple.getValue(REPLAY_BATCH_POSITION)).thenReturn(null);
        bolt.onBatch(tuple);

        Assert.assertTrue(bolt.replayCompleted);
    }

    @Test
    public void testEmitReplayRequest() {
        CustomCollector collector = new CustomCollector();
        TestQueryBolt bolt = new TestQueryBolt(new BulletStormConfig("src/test/resources/test_config.yaml"));

        Assert.assertEquals(collector.getEmittedCount(), 0);

        ComponentUtils.prepare(new HashMap<>(), bolt, new CustomTopologyContext(null, "QueryBolt", 0, 5), collector);

        Assert.assertEquals(collector.getEmittedCount(), 1);

        CustomCollector.Triplet triplet = collector.getEmitted().get(0);

        Assert.assertEquals(triplet.getStreamId(), FEEDBACK_STREAM);
        Assert.assertEquals(triplet.getTuple().size(), 2);
        Assert.assertEquals(triplet.getTuple().get(0), "QueryBolt-5");
        Assert.assertEquals(((Metadata) triplet.getTuple().get(1)).getSignal(), Signal.ACKNOWLEDGE);
        Assert.assertEquals(((Metadata) triplet.getTuple().get(1)).getContent(), bolt.startTimestamp);

        // Too early
        bolt.emitReplayRequestIfNecessary();

        Assert.assertEquals(collector.getEmittedCount(), 1);

        bolt.lastReplayRequest -= bolt.replayRequestInterval;

        bolt.emitReplayRequestIfNecessary();

        Assert.assertEquals(collector.getEmittedCount(), 2);
    }

    @Test
    public void testHandleForcedReplay() {
        CustomCollector collector = new CustomCollector();
        TestQueryBolt bolt = new TestQueryBolt(new BulletStormConfig());
        ComponentUtils.prepare(bolt, collector);

        bolt.replayEnabled = true;
        bolt.startTimestamp = 0;
        bolt.replayCompleted = true;
        bolt.batchCount = 1;
        bolt.replayedQueriesCount = 1;
        Assert.assertEquals(bolt.lastReplayRequest, 0);
        Assert.assertEquals(collector.getEmittedCount(), 0);

        Tuple tuple = makeIDTuple(TupleClassifier.Type.METADATA_TUPLE, "123", new Metadata(Signal.REPLAY, null));

        bolt.onMeta(tuple);

        Assert.assertNotEquals(bolt.startTimestamp, 0);
        Assert.assertFalse(bolt.replayCompleted);
        Assert.assertEquals(bolt.batchCount, 0);
        Assert.assertEquals(bolt.replayedQueriesCount, 0);
        Assert.assertNotEquals(bolt.lastReplayRequest, 0);
        Assert.assertEquals(collector.getEmittedCount(), 1);
    }
}
