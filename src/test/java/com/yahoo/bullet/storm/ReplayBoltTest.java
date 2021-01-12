/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.query.QueryUtils;
import com.yahoo.bullet.storage.MemoryStorageManager;
import com.yahoo.bullet.storage.StorageManager;
import com.yahoo.bullet.storm.grouping.TaskIndexCaptureGrouping;
import com.yahoo.bullet.storm.testing.ComponentUtils;
import com.yahoo.bullet.storm.testing.CustomCollector;
import com.yahoo.bullet.storm.testing.CustomOutputFieldsDeclarer;
import com.yahoo.bullet.storm.testing.CustomTopologyContext;
import com.yahoo.bullet.storm.testing.TupleUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_ACK_POSITION;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_BATCH_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_INDEX_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_TIMESTAMP_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_TIMESTAMP_POSITION;
import static org.mockito.Mockito.doReturn;

public class ReplayBoltTest {
    public static class TestStorageManager extends MemoryStorageManager<PubSubMessage> {
        public TestStorageManager(BulletConfig config) {
            super(config);
            put("0", new PubSubMessage("0", SerializerDeserializer.toBytes(QueryUtils.makeRawQuery(1)), new Metadata()));
            put("1", new PubSubMessage("1", SerializerDeserializer.toBytes(QueryUtils.makeRawQuery(1)), new Metadata()));
            put("2", new PubSubMessage("2", SerializerDeserializer.toBytes(QueryUtils.makeRawQuery(1)), new Metadata()));
        }
    }

    public static class ThrowingStorageManager extends MemoryStorageManager<PubSubMessage> {
        public ThrowingStorageManager(BulletConfig config) {
            super(config);
        }

        @Override
        public CompletableFuture<Map<String, PubSubMessage>> getAll() {
            throw new RuntimeException();
        }
    }

    private static final int NUM_PARTITIONS = 4;
    private ReplayBolt bolt;
    private CustomCollector collector;
    private CustomTopologyContext context;
    private BulletStormConfig config;
    private StorageManager<PubSubMessage> storageManager;

    @BeforeMethod
    public void setup() {
        collector = new CustomCollector();
        context = new CustomTopologyContext();
        config = new BulletStormConfig("src/test/resources/test_config.yaml");
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.set(BulletConfig.STORAGE_CLASS_NAME, "com.yahoo.bullet.storm.ReplayBoltTest$TestStorageManager");
        config.validate();
        bolt = ComponentUtils.prepare(new HashMap<>(), new ReplayBolt(config), context, collector);
        Assert.assertFalse(bolt.isReplayBatchCompressEnable());
        storageManager = bolt.getStorageManager();
    }

    @Test
    public void testPrepare() {
        Assert.assertEquals(collector.getAckedCount(), 0);
        bolt.getCollector().ack(null);
        Assert.assertEquals(collector.getAckedCount(), 1);

        Assert.assertNotNull(bolt.getClassifier());

        Assert.assertTrue(bolt.getMetrics().isEnabled());

        Assert.assertNotNull(bolt.getBatchedQueriesCount());
        Assert.assertNotNull(bolt.getActiveReplaysCount());
        Assert.assertNotNull(bolt.getCreatedReplaysCount());
        Assert.assertEquals(context.getRegisteredMetricByName(TopologyConstants.BATCHED_QUERIES_METRIC), bolt.getBatchedQueriesCount());
        Assert.assertEquals(context.getRegisteredMetricByName(TopologyConstants.ACTIVE_REPLAYS_METRIC), bolt.getActiveReplaysCount());
        Assert.assertEquals(context.getRegisteredMetricByName(TopologyConstants.CREATED_REPLAYS_METRIC), bolt.getCreatedReplaysCount());

        Assert.assertEquals(bolt.getBatchManager().size(), 3L);
        Assert.assertEquals(bolt.getBatchedQueriesCount().getValueAndReset(), 3L);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.BATCHED_QUERIES_METRIC).longValue(), 3L);

        Assert.assertTrue(bolt.getReplays().isEmpty());
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Could not create StorageManager\\.")
    public void testPrepareCouldNotCreateStorageManager() {
        config = new BulletStormConfig("src/test/resources/test_config.yaml");
        config.set(BulletConfig.STORAGE_CLASS_NAME, "");
        config.validate();
        bolt = ComponentUtils.prepare(new HashMap<>(), new ReplayBolt(config), new CustomTopologyContext(), new CustomCollector());
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Failed to get queries from storage\\.")
    public void testPrepareCouldNotGetStoredQueries() {
        config = new BulletStormConfig("src/test/resources/test_config.yaml");
        config.set(BulletConfig.STORAGE_CLASS_NAME, "com.yahoo.bullet.storm.ReplayBoltTest$ThrowingStorageManager");
        config.validate();
        bolt = ComponentUtils.prepare(new HashMap<>(), new ReplayBolt(config), new CustomTopologyContext(), new CustomCollector());
    }

    @Test
    public void testDeclareOutputFields() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        bolt.declareOutputFields(declarer);
        Fields expectedReplayFields = new Fields(TopologyConstants.ID_FIELD, REPLAY_TIMESTAMP_FIELD, REPLAY_INDEX_FIELD, REPLAY_BATCH_FIELD);
        Fields expectedCaptureFields = new Fields();
        Assert.assertTrue(declarer.areFieldsPresent(TopologyConstants.REPLAY_STREAM, false, expectedReplayFields));
        Assert.assertTrue(declarer.areFieldsPresent(TopologyConstants.CAPTURE_STREAM, false, expectedCaptureFields));
    }

    @Test
    public void testUnknownTuple() {
        Tuple tuple = TupleUtils.makeTuple(TupleClassifier.Type.UNKNOWN_TUPLE, "", "");
        bolt.execute(tuple);
        Assert.assertFalse(collector.wasAcked(tuple));
    }

    @Test
    public void testCleanup() {
        // coverage
        bolt.cleanup();
    }

    @Test
    public void testOnQuery() {
        Tuple tuple = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "123", null);

        bolt.execute(tuple);

        Assert.assertEquals(bolt.getBatchManager().size(), 4);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.BATCHED_QUERIES_METRIC).longValue(), 4L);

        bolt.execute(tuple);

        Assert.assertEquals(bolt.getBatchManager().size(), 4);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.BATCHED_QUERIES_METRIC).longValue(), 4L);
    }

    @Test
    public void testOnMeta() {
        Assert.assertEquals(bolt.getBatchManager().size(), 3);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.BATCHED_QUERIES_METRIC).longValue(), 3L);

        bolt.execute(TupleUtils.makeIDTuple(TupleClassifier.Type.METADATA_TUPLE, "0", new Metadata(Metadata.Signal.COMPLETE, null)));

        Assert.assertEquals(bolt.getBatchManager().size(), 2);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.BATCHED_QUERIES_METRIC).longValue(), 2L);

        bolt.execute(TupleUtils.makeIDTuple(TupleClassifier.Type.METADATA_TUPLE, "1", new Metadata(Metadata.Signal.COMPLETE, null)));

        Assert.assertEquals(bolt.getBatchManager().size(), 1);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.BATCHED_QUERIES_METRIC).longValue(), 1L);

        bolt.execute(TupleUtils.makeIDTuple(TupleClassifier.Type.METADATA_TUPLE, "2", new Metadata(Metadata.Signal.COMPLETE, null)));

        Assert.assertEquals(bolt.getBatchManager().size(), 0);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.BATCHED_QUERIES_METRIC).longValue(), 0L);
    }

    @Test
    public void testOnMetaIgnoreTuple() {
        // coverage - null metadata ignored
        bolt.execute(TupleUtils.makeIDTuple(TupleClassifier.Type.METADATA_TUPLE, "", null));

        // coverage - non-kill/replay signal ignored
        bolt.execute(TupleUtils.makeIDTuple(TupleClassifier.Type.METADATA_TUPLE, "", new Metadata(Metadata.Signal.CUSTOM, null)));
    }

    @Test
    public void testHandleReplaySignal() {
        bolt.getReplays().put("FilterBolt-18", new ReplayBolt.Replay(18, 0, null));

        Assert.assertEquals(bolt.getReplays().size(), 1);
        Assert.assertEquals(bolt.getBatchManager().size(), 3);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.BATCHED_QUERIES_METRIC).longValue(), 3L);

        storageManager.remove("0");

        bolt.execute(TupleUtils.makeIDTuple(TupleClassifier.Type.METADATA_TUPLE, "123", new Metadata(Metadata.Signal.REPLAY, null)));

        Assert.assertEquals(bolt.getReplays().size(), 0);
        Assert.assertEquals(bolt.getBatchManager().size(), 2);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.BATCHED_QUERIES_METRIC).longValue(), 2L);
    }

    @Test
    public void testReplayFilterBolt() {
        long timestamp = System.currentTimeMillis();

        Tuple tupleA = TupleUtils.makeIDTuple(TupleClassifier.Type.REPLAY_TUPLE, "FilterBolt-18", timestamp, false);
        Tuple tupleB = TupleUtils.makeIDTuple(TupleClassifier.Type.REPLAY_TUPLE, "FilterBolt-18", timestamp, true);
        doReturn(4).when(tupleA).getSourceTask();
        doReturn(4).when(tupleB).getSourceTask();
        doReturn(timestamp).when(tupleA).getLong(REPLAY_TIMESTAMP_POSITION);
        doReturn(timestamp).when(tupleB).getLong(REPLAY_TIMESTAMP_POSITION);
        doReturn(false).when(tupleA).getBoolean(REPLAY_ACK_POSITION);
        doReturn(true).when(tupleB).getBoolean(REPLAY_ACK_POSITION);

        bolt.execute(tupleA);

        Assert.assertEquals(bolt.getReplays().size(), 1);
        Assert.assertEquals(context.getDimensionLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC, "FilterBolt-18").longValue(), 1L);
        Assert.assertEquals(context.getDimensionLongMetric(TopologyConstants.CREATED_REPLAYS_METRIC, "FilterBolt-18").longValue(), 1L);

        ReplayBolt.Replay replay = bolt.getReplays().get("FilterBolt-18");

        Assert.assertEquals(replay.getBatches().size(), NUM_PARTITIONS);
        Assert.assertTrue(replay.getBatches().get(0) instanceof Map);
        Assert.assertTrue(replay.getAnchors().contains(4));
        Assert.assertEquals(replay.getIndex(), 0);
        Assert.assertEquals(replay.getTaskID(), 18);
        Assert.assertEquals(replay.getTimestamp(), timestamp);

        for (int i = 1; i <= NUM_PARTITIONS; i++) {
            bolt.execute(tupleB);
            Assert.assertTrue(replay.getAnchors().contains(4));
            Assert.assertEquals(replay.getIndex(), i);
        }

        bolt.execute(tupleA);

        Assert.assertTrue(replay.getAnchors().contains(4));
        Assert.assertEquals(replay.getIndex(), NUM_PARTITIONS);

        bolt.execute(tupleB);

        Assert.assertNull(replay.getBatches());
        Assert.assertNull(replay.getAnchors());
        Assert.assertEquals(collector.getEmittedCount(), NUM_PARTITIONS + 2);
        Assert.assertEquals(collector.getAckedCount(), NUM_PARTITIONS + 2);
        Assert.assertEquals(collector.getFailedCount(), 1);
        Assert.assertEquals(context.getDimensionLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC, "FilterBolt-18").longValue(), 0L);
        Assert.assertEquals(context.getDimensionLongMetric(TopologyConstants.CREATED_REPLAYS_METRIC, "FilterBolt-18").longValue(), 1L);

        bolt.execute(tupleB);

        Assert.assertEquals(collector.getFailedCount(), 2);
    }

    @Test
    public void testReplayJoinBolt() {
        long timestamp = System.currentTimeMillis();

        Tuple tupleA = TupleUtils.makeIDTuple(TupleClassifier.Type.REPLAY_TUPLE, "JoinBolt-21", timestamp, false);
        Tuple tupleB = TupleUtils.makeIDTuple(TupleClassifier.Type.REPLAY_TUPLE, "JoinBolt-21", timestamp + 1, true);
        doReturn(4).when(tupleA).getSourceTask();
        doReturn(5).when(tupleB).getSourceTask();
        doReturn(timestamp).when(tupleA).getLong(REPLAY_TIMESTAMP_POSITION);
        doReturn(timestamp + 1).when(tupleB).getLong(REPLAY_TIMESTAMP_POSITION);
        doReturn(false).when(tupleA).getBoolean(REPLAY_ACK_POSITION);
        doReturn(true).when(tupleB).getBoolean(REPLAY_ACK_POSITION);

        // Set temporarily so ReplayBolt can get the partition index for JoinBolt-21
        TaskIndexCaptureGrouping.TASK_INDEX_MAP.put(21, 0);

        bolt.execute(tupleA);

        Assert.assertEquals(bolt.getReplays().size(), 1);
        Assert.assertEquals(context.getDimensionLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC, "JoinBolt-21").longValue(), 1L);
        Assert.assertEquals(context.getDimensionLongMetric(TopologyConstants.CREATED_REPLAYS_METRIC, "JoinBolt-21").longValue(), 1L);

        ReplayBolt.Replay replay = bolt.getReplays().get("JoinBolt-21");

        Assert.assertEquals(replay.getBatches().size(), 1);
        Assert.assertTrue(replay.getBatches().get(0) instanceof Map);
        Assert.assertTrue(replay.getAnchors().contains(4));
        Assert.assertEquals(replay.getIndex(), 0);
        Assert.assertEquals(replay.getTaskID(), 21);
        Assert.assertEquals(replay.getTimestamp(), timestamp);

        // New timestamp
        bolt.execute(tupleB);

        Assert.assertNotEquals(bolt.getReplays().get("JoinBolt-21"), replay);

        Assert.assertEquals(bolt.getReplays().size(), 1);
        Assert.assertEquals(context.getDimensionLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC, "JoinBolt-21").longValue(), 1L);
        Assert.assertEquals(context.getDimensionLongMetric(TopologyConstants.CREATED_REPLAYS_METRIC, "JoinBolt-21").longValue(), 2L);

        replay = bolt.getReplays().get("JoinBolt-21");

        Assert.assertEquals(replay.getBatches().size(), 1);
        Assert.assertTrue(replay.getAnchors().contains(5));
        Assert.assertEquals(replay.getAnchors().size(), 1);
        Assert.assertEquals(replay.getIndex(), 0);
        Assert.assertEquals(replay.getTaskID(), 21);
        Assert.assertEquals(replay.getTimestamp(), timestamp + 1);

        bolt.execute(tupleB);

        Assert.assertTrue(replay.getAnchors().contains(5));
        Assert.assertEquals(replay.getAnchors().size(), 1);
        Assert.assertEquals(replay.getIndex(), 1);

        bolt.execute(tupleB);

        Assert.assertNull(replay.getBatches());
        Assert.assertNull(replay.getAnchors());
        Assert.assertEquals(collector.getEmittedCount(), 3);
        Assert.assertEquals(collector.getAckedCount(), 3);
        Assert.assertEquals(collector.getFailedCount(), 1);
        Assert.assertEquals(context.getDimensionLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC, "JoinBolt-21").longValue(), 0L);
        Assert.assertEquals(context.getDimensionLongMetric(TopologyConstants.CREATED_REPLAYS_METRIC, "JoinBolt-21").longValue(), 2L);

        bolt.execute(tupleA);
        bolt.execute(tupleB);

        Assert.assertEquals(collector.getFailedCount(), 3);

        TaskIndexCaptureGrouping.TASK_INDEX_MAP.clear();
    }

    @Test
    public void testReplayFilterBoltWithCompression() {
        config = new BulletStormConfig("src/test/resources/test_config.yaml");
        config.set(BulletStormConfig.REPLAY_BATCH_COMPRESS_ENABLE, true);
        config.validate();
        bolt = ComponentUtils.prepare(new HashMap<>(), new ReplayBolt(config), context, collector);
        Assert.assertTrue(bolt.isReplayBatchCompressEnable());

        long timestamp = System.currentTimeMillis();

        Tuple tupleA = TupleUtils.makeIDTuple(TupleClassifier.Type.REPLAY_TUPLE, "FilterBolt-18", timestamp, false);
        doReturn(4).when(tupleA).getSourceTask();
        doReturn(timestamp).when(tupleA).getLong(REPLAY_TIMESTAMP_POSITION);
        doReturn(false).when(tupleA).getBoolean(REPLAY_ACK_POSITION);

        bolt.execute(tupleA);

        ReplayBolt.Replay replay = bolt.getReplays().get("FilterBolt-18");

        Assert.assertEquals(replay.getBatches().size(), NUM_PARTITIONS);
        Assert.assertTrue(replay.getBatches().get(0) instanceof byte[]);
    }

    @Test
    public void testReplayJoinBoltWithCompression() {
        config = new BulletStormConfig("src/test/resources/test_config.yaml");
        config.set(BulletStormConfig.REPLAY_BATCH_COMPRESS_ENABLE, true);
        config.validate();
        bolt = ComponentUtils.prepare(new HashMap<>(), new ReplayBolt(config), context, collector);
        Assert.assertTrue(bolt.isReplayBatchCompressEnable());

        long timestamp = System.currentTimeMillis();

        Tuple tupleA = TupleUtils.makeIDTuple(TupleClassifier.Type.REPLAY_TUPLE, "JoinBolt-21", timestamp, false);
        doReturn(4).when(tupleA).getSourceTask();
        doReturn(timestamp).when(tupleA).getLong(REPLAY_TIMESTAMP_POSITION);
        doReturn(false).when(tupleA).getBoolean(REPLAY_ACK_POSITION);

        // Set temporarily so ReplayBolt can get the partition index for JoinBolt-21
        TaskIndexCaptureGrouping.TASK_INDEX_MAP.put(21, 0);

        bolt.execute(tupleA);

        ReplayBolt.Replay replay = bolt.getReplays().get("JoinBolt-21");

        Assert.assertEquals(replay.getBatches().size(), 1);
        Assert.assertTrue(replay.getBatches().get(0) instanceof byte[]);
    }

    @Test
    public void testDoubleReplay() {
        long timestamp = System.currentTimeMillis();

        Tuple tupleA = TupleUtils.makeIDTuple(TupleClassifier.Type.REPLAY_TUPLE, "FilterBolt-18", timestamp, false);
        Tuple tupleB = TupleUtils.makeIDTuple(TupleClassifier.Type.REPLAY_TUPLE, "FilterBolt-18", timestamp, true);
        doReturn(4).when(tupleA).getSourceTask();
        doReturn(5).when(tupleB).getSourceTask();
        doReturn(timestamp).when(tupleA).getLong(REPLAY_TIMESTAMP_POSITION);
        doReturn(timestamp).when(tupleB).getLong(REPLAY_TIMESTAMP_POSITION);
        doReturn(false).when(tupleA).getBoolean(REPLAY_ACK_POSITION);
        doReturn(true).when(tupleB).getBoolean(REPLAY_ACK_POSITION);

        bolt.execute(tupleA);
        bolt.execute(tupleB);

        Assert.assertEquals(bolt.getReplays().size(), 1);

        ReplayBolt.Replay replay = bolt.getReplays().get("FilterBolt-18");

        Assert.assertTrue(replay.getAnchors().contains(4));
        Assert.assertTrue(replay.getAnchors().contains(5));
        Assert.assertEquals(replay.getIndex(), 0);
    }
}
