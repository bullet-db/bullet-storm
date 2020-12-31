/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.aggregations.DistributionType;
import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.querying.Querier;
import com.yahoo.bullet.querying.RateLimitError;
import com.yahoo.bullet.querying.aggregations.FrequentItemsSketchingStrategy;
import com.yahoo.bullet.querying.aggregations.FrequentItemsSketchingStrategyTest;
import com.yahoo.bullet.querying.aggregations.QuantileSketchingStrategy;
import com.yahoo.bullet.querying.aggregations.QuantileSketchingStrategyTest;
import com.yahoo.bullet.querying.aggregations.ThetaSketchingStrategy;
import com.yahoo.bullet.querying.aggregations.ThetaSketchingStrategyTest;
import com.yahoo.bullet.querying.aggregations.TupleSketchingStrategy;
import com.yahoo.bullet.querying.aggregations.TupleSketchingStrategyTest;
import com.yahoo.bullet.querying.aggregations.grouping.GroupData;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import com.yahoo.bullet.result.Meta.Concept;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.storm.testing.ComponentUtils;
import com.yahoo.bullet.storm.testing.CustomCollector;
import com.yahoo.bullet.storm.testing.CustomOutputFieldsDeclarer;
import com.yahoo.bullet.storm.testing.CustomTopologyContext;
import com.yahoo.bullet.storm.testing.TupleUtils;
import com.yahoo.bullet.windowing.SlidingRecord;
import com.yahoo.sketches.frequencies.ErrorType;
import lombok.Setter;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.yahoo.bullet.query.QueryUtils.makeCountDistinctQuery;
import static com.yahoo.bullet.query.QueryUtils.makeDistributionQuery;
import static com.yahoo.bullet.query.QueryUtils.makeGroupAllFieldFilterQuery;
import static com.yahoo.bullet.query.QueryUtils.makeGroupByFilterQuery;
import static com.yahoo.bullet.query.QueryUtils.makeRawQuery;
import static com.yahoo.bullet.query.QueryUtils.makeSimpleAggregationQuery;
import static com.yahoo.bullet.query.QueryUtils.makeTopKQuery;
import static com.yahoo.bullet.query.expressions.Operation.EQUALS;
import static com.yahoo.bullet.query.expressions.Operation.EQUALS_ANY;
import static com.yahoo.bullet.querying.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.querying.aggregations.grouping.GroupOperation.GroupOperationType.SUM;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.COUNT_FIELD;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.END_EXCLUSIVE;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.NEGATIVE_INFINITY_START;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.POSITIVE_INFINITY_END;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.PROBABILITY_FIELD;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.RANGE_FIELD;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.SEPARATOR;
import static com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch.START_INCLUSIVE;
import static com.yahoo.bullet.storm.testing.TestHelpers.assertJSONEquals;
import static com.yahoo.bullet.storm.testing.TestHelpers.getListBytes;
import static com.yahoo.bullet.storm.testing.TupleUtils.makeIDTuple;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.AdditionalAnswers.returnsElementsOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class JoinBoltTest {
    private static final Metadata EMPTY = new Metadata();
    private static final Metadata COMPLETED = new Metadata(Metadata.Signal.COMPLETE, null);
    private static final Metadata FAILED = new Metadata(Metadata.Signal.FAIL, null);
    private static final int RAW_MAX_SIZE = 5;

    private BulletStormConfig config;
    private CustomCollector collector;
    private CustomTopologyContext context;
    private JoinBolt bolt;

    private static class DonableJoinBolt extends JoinBolt {
        private final int doneAfter;
        @Setter
        private boolean shouldBuffer;

        public DonableJoinBolt(BulletStormConfig config, int doneAfter, boolean shouldBuffer) {
            super(config);
            this.doneAfter = doneAfter;
            this.shouldBuffer = shouldBuffer;
        }

        @Override
        protected Querier createQuerier(Querier.Mode mode, String id, Query query, Metadata metadata, BulletConfig config) {
            Querier spied = spy(super.createQuerier(mode, id, query, metadata, config));
            List<Boolean> doneAnswers = IntStream.range(0, doneAfter).mapToObj(i -> false)
                                                 .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
            doneAnswers.add(true);
            doAnswer(returnsElementsOf(doneAnswers)).when(spied).isDone();
            doReturn(shouldBuffer).when(spied).shouldBuffer();
            return spied;
        }
    }

    private static class ClosableJoinBolt extends JoinBolt {
        private final int closeAfter;
        @Setter
        private boolean shouldBuffer;

        public ClosableJoinBolt(BulletStormConfig config, int closeAfter, boolean shouldBuffer) {
            super(config);
            this.closeAfter = closeAfter;
            this.shouldBuffer = shouldBuffer;
        }

        @Override
        protected Querier createQuerier(Querier.Mode mode, String id, Query query, Metadata metadata, BulletConfig config) {
            Querier spied = spy(super.createQuerier(mode, id, query, metadata, config));
            List<Boolean> closeAnswers = IntStream.range(0, closeAfter).mapToObj(i -> false)
                                                  .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
            closeAnswers.add(true);
            doAnswer(returnsElementsOf(closeAnswers)).when(spied).isClosed();
            doReturn(shouldBuffer).when(spied).shouldBuffer();
            return spied;
        }
    }
    private static class RateLimitedJoinBolt extends JoinBolt {
        private final int limitedAfter;
        private final RateLimitError error;

        private RateLimitedJoinBolt(int limitedAfter, RateLimitError error, BulletStormConfig config) {
            super(config);
            this.limitedAfter = limitedAfter;
            this.error = error;
        }

        @Override
        protected Querier createQuerier(Querier.Mode mode, String id, Query query, Metadata metadata, BulletConfig config) {
            Querier spied = spy(super.createQuerier(mode, id, query, metadata, config));
            List<Boolean> answers = IntStream.range(0, limitedAfter).mapToObj(i -> false)
                                             .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
            answers.add(true);
            doAnswer(returnsElementsOf(answers)).when(spied).isExceedingRateLimit();
            doReturn(error).when(spied).getRateLimitError();
            return spied;
        }
    }

    // This sends ceil(n / batchSize) batches to the bolt
    private static List<BulletRecord> sendRawRecordTuplesTo(IRichBolt bolt, String id, int n, int batchSize) {
        List<BulletRecord> sent = new ArrayList<>();
        for (int i = 0; i < n; i += batchSize) {
            BulletRecord[] batch = new BulletRecord[batchSize];
            for (int j = 0; j < batchSize; ++j) {
                batch[j] = RecordBox.get().add("field", String.valueOf(i + j)).getRecord();
            }
            Tuple tuple = makeIDTuple(TupleClassifier.Type.DATA_TUPLE, id, getListBytes(batch));
            bolt.execute(tuple);
            sent.addAll(asList(batch));
        }
        return sent;
    }

    private static List<BulletRecord> sendRawRecordTuplesTo(IRichBolt bolt, String id, int n) {
        return sendRawRecordTuplesTo(bolt, id, n, 1);
    }

    private static List<BulletRecord> sendRawRecordTuplesTo(IRichBolt bolt, String id) {
        return sendRawRecordTuplesTo(bolt, id, RAW_MAX_SIZE);
    }

    private static void sendRawByteTuplesTo(IRichBolt bolt, String id, List<byte[]> data) {
        for (byte[] b : data) {
            Tuple tuple = makeIDTuple(TupleClassifier.Type.DATA_TUPLE, id, b);
            bolt.execute(tuple);
        }
    }

    private static List<BulletRecord> sendSlidingWindowWithRawRecordTuplesTo(IRichBolt bolt, String id, int n) {
        BulletRecord[] sent = new BulletRecord[n];
        for (int i = 0; i < n; ++i) {
            sent[i] = RecordBox.get().add("field", String.valueOf(i)).getRecord();
        }
        byte[] listBytes = getListBytes(sent);
        byte[] dataBytes = SerializerDeserializer.toBytes(new SlidingRecord.Data(sent.length, listBytes));
        Tuple tuple = makeIDTuple(TupleClassifier.Type.DATA_TUPLE, id, dataBytes);
        bolt.execute(tuple);
        return asList(sent);
    }

    private static byte[] getGroupDataWithCount(String countField, int count) {
        GroupData groupData = new GroupData(new HashSet<>(singletonList(new GroupOperation(COUNT, null, countField))));
        IntStream.range(0, count).forEach(i -> groupData.consume(RecordBox.get().getRecord()));
        return SerializerDeserializer.toBytes(groupData);
    }

    private static void enableMetadataInConfig(BulletStormConfig config, String metaConcept, String key) {
        Map<String, String> metadataConfig =
                (Map<String, String>) config.getOrDefault(BulletStormConfig.RESULT_METADATA_METRICS, new HashMap<>());
        metadataConfig.put(metaConcept, key);
        config.set(BulletStormConfig.RESULT_METADATA_ENABLE, true);
        config.set(BulletStormConfig.RESULT_METADATA_METRICS, metadataConfig);
    }

    private static BulletStormConfig configWithRawMaxAndNoMeta() {
        BulletStormConfig config = new BulletStormConfig();
        config.set(BulletStormConfig.RAW_AGGREGATION_MAX_SIZE, RAW_MAX_SIZE);
        config.set(BulletStormConfig.RESULT_METADATA_ENABLE, false);
        config.validate();
        return config;
    }

    private static BulletStormConfig configWithRawMaxAndEmptyMeta() {
        BulletStormConfig config = new BulletStormConfig();
        config.set(BulletStormConfig.RAW_AGGREGATION_MAX_SIZE, RAW_MAX_SIZE);
        config.set(BulletStormConfig.RESULT_METADATA_ENABLE, true);
        config.validate();
        config.set(BulletStormConfig.RESULT_METADATA_METRICS, new HashMap<>());
        return config;
    }

    private static boolean isSameResult(List<Object> actual, List<Object> expected) {
        boolean result;
        result = actual.size() == 3;
        result &= actual.size() == expected.size();
        result &= actual.get(0).equals(expected.get(0));
        result &= actual.get(1).equals(expected.get(1));
        return result;
    }

    private static boolean isSameMetadata(Object actual, Object expected) {
        Metadata actualMetadata = (Metadata) actual;
        Metadata expectedMetadata = (Metadata) expected;
        if (actualMetadata.getSignal() != expectedMetadata.getSignal()) {
            return false;
        }
        Serializable actualContent = actualMetadata.getContent();
        Serializable expectedContent = expectedMetadata.getContent();
        return actualContent == expectedContent || actualContent.equals(expectedContent);
    }

    private static boolean resultTupleEquals(List<Object> actual, Tuple expectedTuple) {
        List<Object> expected = expectedTuple.getValues();
        return isSameResult(actual, expected) && isSameMetadata(actual.get(2), expected.get(2));
    }

    private static boolean metadataTupleEquals(List<Object> actual, Tuple metadataTuple)  {
        List<Object> expected = metadataTuple.getValues();
        boolean result;
        result = actual.size() == 2;
        result &= actual.size() == expected.size();
        result &= actual.get(0).equals(expected.get(0));
        return result;
    }

    private boolean wasResultEmittedTo(String stream, Tuple expectedTuple) {
        return collector.getAllEmittedTo(stream).anyMatch(t -> resultTupleEquals(t.getTuple(), expectedTuple));
    }

    private boolean wasResultEmitted(Tuple expectedTuple) {
        return collector.getTuplesEmittedTo(TopologyConstants.RESULT_STREAM).anyMatch(t -> resultTupleEquals(t, expectedTuple));
    }

    private boolean wasMetadataEmittedTo(String stream, Tuple metadata) {
        return collector.getAllEmittedTo(stream).anyMatch(t -> metadataTupleEquals(t.getTuple(), metadata));
    }

    @BeforeMethod
    public void setup() {
        config = configWithRawMaxAndNoMeta();
        setup(new JoinBolt(config));
    }

    public void setup(JoinBolt bolt) {
        collector = new CustomCollector();
        context = new CustomTopologyContext();
        this.bolt = ComponentUtils.prepare(new HashMap<>(), bolt, context, collector);
    }

    @Test
    public void testOutputFields() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        bolt.declareOutputFields(declarer);
        Fields expectedResultFields = new Fields(TopologyConstants.ID_FIELD, TopologyConstants.RESULT_FIELD, TopologyConstants.METADATA_FIELD);
        Fields expectedFeedbackFields = new Fields(TopologyConstants.ID_FIELD, TopologyConstants.METADATA_FIELD);
        Assert.assertTrue(declarer.areFieldsPresent(TopologyConstants.RESULT_STREAM, false, expectedResultFields));
        Assert.assertTrue(declarer.areFieldsPresent(TopologyConstants.FEEDBACK_STREAM, false, expectedFeedbackFields));
    }

    @Test
    public void testUnknownTuple() {
        Tuple query = TupleUtils.makeRawTuple(TopologyConstants.RECORD_COMPONENT, TopologyConstants.RECORD_STREAM,
                                              RecordBox.get().add("a", "b").getRecord());
        bolt.execute(query);
        Assert.assertFalse(collector.wasAcked(query));
    }

    @Test
    public void testJoining() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(RAW_MAX_SIZE)), EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42");

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), COMPLETED);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testFailJoiningForNoQuery() {
        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42");

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), COMPLETED);
        Assert.assertFalse(wasResultEmitted(expected));
        Assert.assertEquals(collector.getEmittedCount(), 0);
    }

    @Test
    public void testQueryNotDoneButIsDurationBased() {
        // This bolt will be done after combining RAW_MAX_SIZE - 1 times and is a shouldBuffer, so it is buffered.
        bolt = new DonableJoinBolt(config, RAW_MAX_SIZE - 1, true);
        setup(bolt);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(RAW_MAX_SIZE)), EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", RAW_MAX_SIZE - 1);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        // Should make isDone true but query is a a shouldBuffer so it gets buffered.
        bolt.execute(tick);

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), COMPLETED);

        // We need to tick the default query tickout to make the query emit
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        }
        // Should emit on the last tick
        bolt.execute(tick);

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testJoiningAfterLateArrivalMakingQueryFinishBeforeTickout() {
        bolt = new DonableJoinBolt(config, 2, true);
        setup(bolt);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(3)), EMPTY);
        bolt.execute(query);

        // This calls isDone twice. So the query is done on the next tick
        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", 2);

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), COMPLETED);

        // Tick once to get the query done rotated into buffer.
        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);

        Assert.assertFalse(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));

        // Now we satisfy the query and see if it causes an emission
        List<BulletRecord> sentLate = sendRawRecordTuplesTo(bolt, "42", 1);
        sent.addAll(sentLate);

        expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), COMPLETED);

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testJoiningAfterLateArrivalWithoutFinishingQueryBeforeTickout() {
        bolt = new DonableJoinBolt(config, 2, true);
        setup(bolt);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(5)), EMPTY);
        bolt.execute(query);

        // This calls isDone twice. So the query is done on the next tick
        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", 2);

        // Tick once to get the query done rotated into buffer.
        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);

        // Now send more tuples but not enough to finish query
        List<BulletRecord> sentLate = sendRawRecordTuplesTo(bolt, "42", 2);
        sent.addAll(sentLate);
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS - 1; ++i) {
            bolt.execute(tick);
            Assert.assertEquals(collector.getEmittedCount(), 0);
        }

        // The expected record now should contain the sentLate ones too
        bolt.execute(tick);
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), COMPLETED);

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testMultiJoining() {
        DonableJoinBolt donableJoinBolt = new DonableJoinBolt(config, 2, true);
        bolt = donableJoinBolt;
        setup(bolt);

        Tuple queryA = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(3)), EMPTY);
        bolt.execute(queryA);

        donableJoinBolt.shouldBuffer = false;
        Tuple queryB = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "43", SerializerDeserializer.toBytes(makeRawQuery(3)), EMPTY);
        bolt.execute(queryB);

        // This will satisfy the query and will be buffered
        List<BulletRecord> sentFirst = sendRawRecordTuplesTo(bolt, "42", 2);
        // This will satisfy the query and will not be buffered
        List<BulletRecord> sentSecond = sendRawRecordTuplesTo(bolt, "43", 3);

        Tuple emittedFirst = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "43", Clip.of(sentSecond).asJSON(), COMPLETED);
        Tuple emittedSecond = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sentFirst).asJSON(), COMPLETED);

        Assert.assertTrue(wasResultEmitted(emittedFirst));
        Assert.assertFalse(wasResultEmitted(emittedSecond));

        // This will force queryA to finish and start buffering
        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);

        // We need to tick the default query tickout to make queryA emit
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(wasResultEmitted(emittedSecond));
        }
        // This will cause the emission
        bolt.execute(tick);
        Assert.assertTrue(wasResultEmitted(emittedSecond));

        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
        metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "43", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));

        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 2);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 2);
    }

    @Test
    public void testErrorEmittedProperly() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", new byte[0], EMPTY);
        bolt.execute(query);

        Assert.assertEquals(collector.getEmittedCount(), 1);

        BulletError expectedError = BulletError.makeError("java.lang.NullPointerException", "Error initializing query");
        Meta expectedMetadata = Meta.of(expectedError);
        List<Object> expected = TupleUtils.makeTuple("42", Clip.of(expectedMetadata).asJSON(), FAILED).getValues();
        List<Object> actual = collector.getNthTupleEmittedTo(TopologyConstants.RESULT_STREAM, 1).get();

        Assert.assertTrue(isSameResult(actual, expected));
    }

    @Test
    public void testQueryIdentifierMetadata() {
        config = configWithRawMaxAndEmptyMeta();
        enableMetadataInConfig(config, Concept.QUERY_METADATA.getName(), "meta");
        enableMetadataInConfig(config, Concept.QUERY_ID.getName(), "id");
        setup(new JoinBolt(config));

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(RAW_MAX_SIZE)), EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42");

        Meta meta = new Meta();
        meta.add("meta", singletonMap("id", "42"));
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).add(meta).asJSON(), COMPLETED);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testUnknownConceptMetadata() {
        config = configWithRawMaxAndEmptyMeta();
        enableMetadataInConfig(config, Concept.QUERY_METADATA.getName(), "meta");
        enableMetadataInConfig(config, Concept.QUERY_ID.getName(), "id");
        enableMetadataInConfig(config, "foo", "bar");
        setup(new JoinBolt(config));

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(RAW_MAX_SIZE)), EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42");

        Meta meta = new Meta();
        meta.add("meta", singletonMap("id", "42"));
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).add(meta).asJSON(), COMPLETED);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testMultipleMeta() {
        config = configWithRawMaxAndEmptyMeta();
        enableMetadataInConfig(config, Concept.QUERY_METADATA.getName(), "meta");
        enableMetadataInConfig(config, Concept.QUERY_ID.getName(), "id");
        enableMetadataInConfig(config, Concept.QUERY_OBJECT.getName(), "query object");
        enableMetadataInConfig(config, Concept.QUERY_STRING.getName(), "query string");
        enableMetadataInConfig(config, Concept.QUERY_RECEIVE_TIME.getName(), "created");
        enableMetadataInConfig(config, Concept.QUERY_FINISH_TIME.getName(), "finished");
        setup(new JoinBolt(config));

        long startTime = System.currentTimeMillis();

        Query queryObject = makeRawQuery(RAW_MAX_SIZE);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(queryObject), new Metadata(Metadata.Signal.COMPLETE, "foo"));
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42");

        long endTime = System.currentTimeMillis();

        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);

        String response = (String) collector.getMthElementFromNthTupleEmittedTo(TopologyConstants.RESULT_STREAM, 1, 1).get();
        JsonParser parser = new JsonParser();
        JsonObject actual = parser.parse(response).getAsJsonObject();
        JsonObject expected = parser.parse(Clip.of(sent).asJSON()).getAsJsonObject();

        String actualRecords = actual.get(Clip.RECORDS_KEY).toString();
        String expectedRecords = expected.get(Clip.RECORDS_KEY).toString();
        assertJSONEquals(actualRecords, expectedRecords);

        JsonObject meta = actual.get(Clip.META_KEY).getAsJsonObject();
        JsonObject queryMeta = meta.get("meta").getAsJsonObject();
        String actualID = queryMeta.get("id").getAsString();
        String queryBody = queryMeta.get("query object").getAsString();
        String queryString = queryMeta.get("query string").getAsString();
        long createdTime = queryMeta.get("created").getAsLong();
        long finishedTime = queryMeta.get("finished").getAsLong();

        Assert.assertEquals(actualID, "42");
        Assert.assertEquals(queryBody, queryObject.toString());
        Assert.assertEquals(queryString, "foo");
        Assert.assertTrue(createdTime <= finishedTime);
        Assert.assertTrue(createdTime >= startTime && createdTime <= endTime);
    }

    @Test
    public void testBadBytes() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", new byte[0], EMPTY);
        bolt.execute(query);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 0);
    }

    @Test
    public void testErrorInQueryWithoutMetadata() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(5)));
        bolt.execute(query);

        Assert.assertEquals(collector.getEmittedCount(), 1);
        BulletError expectedError = BulletError.makeError("java.lang.NullPointerException", "Error initializing query");
        Meta expectedMetadata = Meta.of(expectedError);
        List<Object> expected = TupleUtils.makeTuple("42", Clip.of(expectedMetadata).asJSON(), FAILED).getValues();
        List<Object> actual = collector.getNthTupleEmittedTo(TopologyConstants.RESULT_STREAM, 1).get();
        Assert.assertTrue(isSameResult(actual, expected));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getEmittedCount(), 1);
    }

    @Test
    public void testRawQueryDoneButNotTimedOutWithExcessRecords() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(5)), EMPTY);
        bolt.execute(query);

        // This will send 2 batches of 3 records each (total of 6 records).
        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", 5, 3);

        List<BulletRecord> actualSent = sent.subList(0, 5);

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(actualSent).asJSON(), COMPLETED);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testCounting() {
        bolt = new DonableJoinBolt(config, 5, true);
        setup(bolt);

        Query filterQuery = makeGroupAllFieldFilterQuery("timestamp", Arrays.asList("1", "2"), EQUALS_ANY, singletonList(new GroupOperation(COUNT, null, "cnt")));

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(filterQuery), EMPTY);
        bolt.execute(query);

        // Send 5 GroupData with counts 1, 2, 3, 4, 5 to the JoinBolt
        IntStream.range(1, 6).forEach(i -> sendRawByteTuplesTo(bolt, "42", singletonList(getGroupDataWithCount("cnt", i))));

        // 1 + 2 + 3 + 4 + 5
        List<BulletRecord> result = singletonList(RecordBox.get().add("cnt", 15L).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(result).asJSON(), COMPLETED);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        // Should starts buffering the query for the query tickout
        bolt.execute(tick);
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        }
        bolt.execute(tick);

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testCountDistinct() {
        BulletConfig bulletConfig = ThetaSketchingStrategyTest.makeConfiguration(8, 512);

        ThetaSketchingStrategy distinct = ThetaSketchingStrategyTest.makeCountDistinct(bulletConfig, singletonList("field"), "count");

        IntStream.range(0, 256).mapToObj(i -> RecordBox.get().add("field", i).getRecord()).forEach(distinct::consume);
        byte[] first = distinct.getData();

        distinct = ThetaSketchingStrategyTest.makeCountDistinct(bulletConfig, singletonList("field"), "count");

        IntStream.range(128, 256).mapToObj(i -> RecordBox.get().add("field", i).getRecord()).forEach(distinct::consume);
        byte[] second = distinct.getData();

        // Send generated data to JoinBolt
        bolt = new DonableJoinBolt(config, 2, true);
        setup(bolt);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                             SerializerDeserializer.toBytes(makeCountDistinctQuery(singletonList("field"), "count")),
                                             EMPTY);
        bolt.execute(query);

        sendRawByteTuplesTo(bolt, "42", asList(first, second));

        List<BulletRecord> result = singletonList(RecordBox.get().add("count", 256L).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(result).asJSON(), COMPLETED);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        }
        bolt.execute(tick);

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testGroupBy() {
        final int entries = 16;
        BulletConfig bulletConfig = TupleSketchingStrategyTest.makeConfiguration(entries);
        TupleSketchingStrategy groupBy = TupleSketchingStrategyTest.makeGroupBy(bulletConfig, singletonMap("fieldA", "A"), entries,
                                                                                new GroupOperation(COUNT, null, "cnt"),
                                                                                new GroupOperation(SUM, "fieldB", "sumB"));

        IntStream.range(0, 256).mapToObj(i -> RecordBox.get().add("fieldA", i % 16).add("fieldB", i / 16).getRecord())
                               .forEach(groupBy::consume);
        byte[] first = groupBy.getData();

        groupBy = TupleSketchingStrategyTest.makeGroupBy(bulletConfig, singletonMap("fieldA", "A"), entries,
                                                         new GroupOperation(COUNT, null, "cnt"),
                                                         new GroupOperation(SUM, "fieldB", "sumB"));

        IntStream.range(256, 1024).mapToObj(i -> RecordBox.get().add("fieldA", i % 16).add("fieldB", i / 16).getRecord())
                                  .forEach(groupBy::consume);

        byte[] second = groupBy.getData();

        // Send generated data to JoinBolt
        bolt = new DonableJoinBolt(config, 2, true);
        setup(bolt);

        List<GroupOperation> operations = asList(new GroupOperation(COUNT, null, "cnt"),
                                                 new GroupOperation(SUM, "fieldB", "sumB"));

        Query groupQuery = makeGroupByFilterQuery(new BinaryExpression(new FieldExpression("ts"), new ValueExpression("1"), EQUALS),
                                                  entries, singletonMap("fieldA", "A"), operations);
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(groupQuery), EMPTY);
        bolt.execute(query);

        sendRawByteTuplesTo(bolt, "42", asList(first, second));

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS - 1; ++i) {
            bolt.execute(tick);
            Assert.assertEquals(collector.getEmittedCount(), 0);
        }
        bolt.execute(tick);

        Assert.assertEquals(collector.getEmittedCount(), 2);
        String response = (String) collector.getMthElementFromNthTupleEmittedTo(TopologyConstants.RESULT_STREAM, 1, 1).get();
        JsonParser parser = new JsonParser();
        JsonObject actual = parser.parse(response).getAsJsonObject();
        JsonArray actualRecords = actual.get(Clip.RECORDS_KEY).getAsJsonArray();
        Assert.assertEquals(actualRecords.size(), 16);
    }

    @Test
    public void testDistribution() {
        BulletConfig bulletConfig = QuantileSketchingStrategyTest.makeConfiguration(10, 128);

        QuantileSketchingStrategy distribution = QuantileSketchingStrategyTest.makeDistribution(bulletConfig, 10, "field", DistributionType.PMF, 3);

        IntStream.range(0, 50).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                              .forEach(distribution::consume);

        byte[] first = distribution.getData();

        distribution = QuantileSketchingStrategyTest.makeDistribution(bulletConfig, 10, "field", DistributionType.PMF, 3);

        IntStream.range(50, 101).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                                .forEach(distribution::consume);

        byte[] second = distribution.getData();

        bolt = new DonableJoinBolt(config, 2, true);
        setup(bolt);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                             SerializerDeserializer.toBytes(makeDistributionQuery(10, DistributionType.PMF, "field", 3)),
                                             EMPTY);
        bolt.execute(query);

        sendRawByteTuplesTo(bolt, "42", asList(first, second));

        BulletRecord expectedA = RecordBox.get().add(RANGE_FIELD, NEGATIVE_INFINITY_START + SEPARATOR + 0.0 + END_EXCLUSIVE)
                                                .add(COUNT_FIELD, 0.0)
                                                .add(PROBABILITY_FIELD, 0.0).getRecord();
        BulletRecord expectedB = RecordBox.get().add(RANGE_FIELD, START_INCLUSIVE + 0.0 + SEPARATOR + 50.0 + END_EXCLUSIVE)
                                                .add(COUNT_FIELD, 50.0)
                                                .add(PROBABILITY_FIELD, 50.0 / 101).getRecord();
        BulletRecord expectedC = RecordBox.get().add(RANGE_FIELD, START_INCLUSIVE + 50.0 + SEPARATOR + 100.0 + END_EXCLUSIVE)
                                                .add(COUNT_FIELD, 50.0)
                                                .add(PROBABILITY_FIELD, 50.0 / 101).getRecord();
        BulletRecord expectedD = RecordBox.get().add(RANGE_FIELD, START_INCLUSIVE + 100.0 + SEPARATOR + POSITIVE_INFINITY_END)
                                                .add(COUNT_FIELD, 1.0)
                                                .add(PROBABILITY_FIELD, 1.0 / 101).getRecord();

        List<BulletRecord> results = asList(expectedA, expectedB, expectedC, expectedD);
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(results).asJSON(), COMPLETED);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        }
        bolt.execute(tick);

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testTopK() {
        BulletConfig bulletConfig = FrequentItemsSketchingStrategyTest.makeConfiguration(ErrorType.NO_FALSE_NEGATIVES, 16);

        Map<String, String> fields = new HashMap<>();
        fields.put("A", "");
        fields.put("B", "foo");
        FrequentItemsSketchingStrategy topK = FrequentItemsSketchingStrategyTest.makeTopK(bulletConfig, fields, 2, "cnt", 5L, null);

        IntStream.range(0, 32).mapToObj(i -> RecordBox.get().add("A", i % 8).getRecord()).forEach(topK::consume);

        byte[] first = topK.getData();

        topK = FrequentItemsSketchingStrategyTest.makeTopK(bulletConfig, fields, 2, "cnt", 5L, null);

        IntStream.range(0, 8).mapToObj(i -> RecordBox.get().add("A", i % 2).getRecord()).forEach(topK::consume);

        byte[] second = topK.getData();

        bolt = new DonableJoinBolt(config, 2, true);
        setup(bolt);

        Query aggregationQuery = makeTopKQuery(2, 5L, "cnt", fields);
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(aggregationQuery), EMPTY);
        bolt.execute(query);

        sendRawByteTuplesTo(bolt, "42", asList(first, second));

        BulletRecord expectedA = RecordBox.get().add("A", "0").add("foo", "null").add("cnt", 8L).getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "1").add("foo", "null").add("cnt", 8L).getRecord();

        List<BulletRecord> results = asList(expectedA, expectedB);
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(results).asJSON(), COMPLETED);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        }
        bolt.execute(tick);

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testQueryCountingMetrics() {
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        bolt = new DonableJoinBolt(config, 2, true);
        setup(bolt);

        Query filterQuery = makeGroupAllFieldFilterQuery("timestamp", Arrays.asList("1", "2"), EQUALS_ANY, singletonList(new GroupOperation(COUNT, null, "cnt")));
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(filterQuery), EMPTY);

        Assert.assertEquals(context.getLongMetric(TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.IMPROPER_QUERIES_METRIC), Long.valueOf(0));

        bolt.execute(query);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));

        sendRawByteTuplesTo(bolt, "42", singletonList(getGroupDataWithCount("cnt", 21)));
        sendRawByteTuplesTo(bolt, "42", singletonList(getGroupDataWithCount("cnt", 21)));

        List<BulletRecord> result = singletonList(RecordBox.get().add("cnt", 42L).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(result).asJSON(), COMPLETED);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        // Should cause an expiry and starts buffering the query for the query tickout
        bolt.execute(tick);

        // We need to tick the default query tickout to make the query emit
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS - 1; ++i) {
            bolt.execute(tick);
        }
        Assert.assertFalse(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));
        bolt.execute(tick);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);

        // create query again
        bolt.execute(query);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(2));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.IMPROPER_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.DUPLICATED_QUERIES_METRIC), Long.valueOf(0));

        // query still exists so treated as duplicate
        bolt.execute(query);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(2));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.IMPROPER_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.DUPLICATED_QUERIES_METRIC), Long.valueOf(1));
    }

    @Test
    public void testImproperQueryCountingMetrics() {
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        setup(new JoinBolt(config));

        Assert.assertEquals(context.getLongMetric(TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.IMPROPER_QUERIES_METRIC), Long.valueOf(0));

        Tuple badQuery = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", new byte[0], EMPTY);
        bolt.execute(badQuery);
        bolt.execute(badQuery);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.IMPROPER_QUERIES_METRIC), Long.valueOf(2));
    }

    @Test
    public void testCustomMetricEmitInterval() {
        Map<String, Number> mapping = new HashMap<>();
        mapping.put(TopologyConstants.ACTIVE_QUERIES_METRIC, 1);
        mapping.put(BulletStormConfig.DEFAULT_BUILT_IN_METRICS_INTERVAL_KEY, 10);
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING, mapping);
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        bolt = new DonableJoinBolt(config, BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS, true);
        setup(bolt);

        Query filterQuery = makeGroupAllFieldFilterQuery("timestamp", Arrays.asList("1", "2"), EQUALS_ANY, singletonList(new GroupOperation(COUNT, null, "cnt")));
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(filterQuery), EMPTY);

        Assert.assertEquals(context.getLongMetric(10, TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(1, TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(10, TopologyConstants.IMPROPER_QUERIES_METRIC), Long.valueOf(0));

        bolt.execute(query);
        Assert.assertEquals(context.getLongMetric(10, TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(1, TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        // First half to make it be seen as done through the DonableJoinBolt, the last half to actually finish buffering.
        int ticksNeeded = BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_POST_FINISH_BUFFER_TICKS * 2;
        for (int i = 0; i <= ticksNeeded; ++i) {
            Assert.assertEquals(context.getLongMetric(1, TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));
            bolt.execute(tick);
        }

        List<BulletRecord> result = singletonList(RecordBox.get().add("cnt", 0L).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(result).asJSON(), COMPLETED);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));

        Assert.assertEquals(context.getLongMetric(10, TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(1, TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(10, TopologyConstants.IMPROPER_QUERIES_METRIC), Long.valueOf(0));
    }

    @Test
    public void testKillSignal() {
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        setup(bolt);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(RAW_MAX_SIZE)), EMPTY);
        bolt.execute(query);

        sendRawRecordTuplesTo(bolt, "42", RAW_MAX_SIZE - 1);

        Assert.assertEquals(collector.getEmittedCount(), 0);

        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));

        Tuple kill = makeIDTuple(TupleClassifier.Type.METADATA_TUPLE, "42",
                                            new Metadata(Metadata.Signal.KILL, null));
        bolt.execute(kill);

        Assert.assertEquals(collector.getEmittedCount(), 0);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
    }

    @Test
    public void testCompleteSignal() {
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        setup(bolt);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                             SerializerDeserializer.toBytes(makeRawQuery(RAW_MAX_SIZE)),
                                             EMPTY);
        bolt.execute(query);

        sendRawRecordTuplesTo(bolt, "42", RAW_MAX_SIZE - 1);

        Assert.assertEquals(collector.getEmittedCount(), 0);

        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));

        Tuple complete = makeIDTuple(TupleClassifier.Type.METADATA_TUPLE, "42",
                                                new Metadata(Metadata.Signal.COMPLETE, null));
        bolt.execute(complete);

        Assert.assertEquals(collector.getEmittedCount(), 0);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
    }

    @Test
    public void testRateLimitErrorFromUpstream() {
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        setup(bolt);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(RAW_MAX_SIZE)), EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", RAW_MAX_SIZE - 1);
        Assert.assertEquals(collector.getEmittedCount(), 0);

        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));

        RateLimitError rateLimitError = new RateLimitError(2000.0, 1000.0);
        Tuple error = makeIDTuple(TupleClassifier.Type.ERROR_TUPLE, "42", rateLimitError);

        bolt.execute(error);
        Assert.assertEquals(collector.getEmittedCount(), 2);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42",
                                              Clip.of(sent).add(rateLimitError.makeMeta()).asJSON(),
                                              new Metadata(Metadata.Signal.FAIL, null));

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.KILL, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testRateLimitErrorFromUpstreamWithoutQuery() {
        RateLimitError rateLimitError = new RateLimitError(2000.0, 1000.0);
        Tuple error = makeIDTuple(TupleClassifier.Type.ERROR_TUPLE, "42", rateLimitError);

        bolt.execute(error);
        Assert.assertEquals(collector.getEmittedCount(), 0);
    }

    @Test
    public void testRateLimitingWithTicks() {
        RateLimitError rateLimitError = new RateLimitError(42.0, 5.0);
        bolt = new RateLimitedJoinBolt(2, rateLimitError, config);
        setup(bolt);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(10)), EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", 2);
        Assert.assertEquals(collector.getEmittedCount(), 0);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);

        Assert.assertEquals(collector.getEmittedCount(), 2);
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42",
                                              Clip.of(sent).add(rateLimitError.makeMeta()).asJSON(),
                                              new Metadata(Metadata.Signal.FAIL, null));
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.KILL, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
    }

    @Test
    public void testRateLimitingOnCombine() {
        RateLimitError rateLimitError = new RateLimitError(42.0, 5.0);
        bolt = new RateLimitedJoinBolt(2, rateLimitError, config);
        setup(bolt);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(10)), EMPTY);
        bolt.execute(query);

        // After consuming the 3rd one, it is rate limited and the fourth is not consumed
        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", 4);

        Assert.assertEquals(collector.getEmittedCount(), 2);
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42",
                                              Clip.of(sent.subList(0, 3)).add(rateLimitError.makeMeta()).asJSON(),
                                              new Metadata(Metadata.Signal.FAIL, null));
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.KILL, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
    }

    @Test
    public void testDataWithoutQuery() {
        sendRawRecordTuplesTo(bolt, "42", RAW_MAX_SIZE - 2);
        Assert.assertEquals(collector.getEmittedCount(), 0);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(RAW_MAX_SIZE)), EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", RAW_MAX_SIZE);
        Assert.assertEquals(collector.getEmittedCount(), 2);
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), COMPLETED);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
    }

    @Test
    public void testMissingMetadataIsEmitted() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(RAW_MAX_SIZE)), EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", RAW_MAX_SIZE);
        Assert.assertEquals(collector.getEmittedCount(), 2);
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), COMPLETED);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
    }

    @Test
    public void testMetadataIsNotReplaced() {
        Metadata actualMetadata = new Metadata(null, "foo");
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(RAW_MAX_SIZE)), actualMetadata);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", RAW_MAX_SIZE);

        Metadata expectedMetadata = new Metadata(Metadata.Signal.COMPLETE, "foo");

        Assert.assertEquals(collector.getEmittedCount(), 2);
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), expectedMetadata);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));

        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, "foo"));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));
    }

    @Test
    public void testQueryBeingDelayed() {
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        bolt = new DonableJoinBolt(config, 5, false);
        setup(bolt);

        Query queryObject = makeSimpleAggregationQuery(5, Window.Unit.RECORD, 1, Window.Unit.RECORD, 1);

        // This kind of query actually returns true for shouldBuffer but making it false using DonableJoinBolt to test
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                             SerializerDeserializer.toBytes(queryObject),
                                             EMPTY);
        bolt.execute(query);

        // Not active yet
        Assert.assertEquals(context.getLongMetric(TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_PRE_START_DELAY_TICKS - 1; ++i) {
            bolt.execute(tick);
            Assert.assertEquals(collector.getEmittedCount(), 0);
            Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
        }
        bolt.execute(tick);
        // Now active
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));

        Tuple expected;
        List<BulletRecord> sentFirst = sendSlidingWindowWithRawRecordTuplesTo(bolt, "42", 1);
        Assert.assertEquals(collector.getEmittedCount(), 1);
        expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sentFirst).asJSON(), EMPTY);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));

        // Two events can't really happen but pretending it does to distinguish the results
        List<BulletRecord> sentSecond = sendSlidingWindowWithRawRecordTuplesTo(bolt, "42", 2);
        Assert.assertEquals(collector.getEmittedCount(), 2);
        expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sentSecond).asJSON(), EMPTY);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));

        // Same with three
        List<BulletRecord> sentThird = sendSlidingWindowWithRawRecordTuplesTo(bolt, "42", 3);
        Assert.assertEquals(collector.getEmittedCount(), 3);
        expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sentThird).asJSON(), EMPTY);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));

        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));
    }

    @Test
    public void testQueryClosedWhileFinishedTerminatesTheQuery() {
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        bolt = new DonableJoinBolt(config, 2, true);
        setup(bolt);

        Query queryObject = makeSimpleAggregationQuery(5, Window.Unit.RECORD, 1, Window.Unit.RECORD, 1);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                             SerializerDeserializer.toBytes(queryObject),
                                             EMPTY);
        bolt.execute(query);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        // Third tick marks it done and start buffering
        bolt.execute(tick);
        bolt.execute(tick);
        // Begins buffering in query tickout buffer after here
        bolt.execute(tick);
        Assert.assertEquals(collector.getEmittedCount(), 0);

        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));

        List<BulletRecord> sent = sendSlidingWindowWithRawRecordTuplesTo(bolt, "42", 2);

        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(),
                                              new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));

        Tuple metadata = TupleUtils.makeTuple(TupleClassifier.Type.FEEDBACK_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        Assert.assertTrue(wasMetadataEmittedTo(TopologyConstants.FEEDBACK_STREAM, metadata));

        Assert.assertEquals(collector.getEmittedCount(), 2);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testWindowClosedOnTickIsImmediatelyEmitted() {
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        bolt = new ClosableJoinBolt(config, 3, false);
        setup(bolt);

        Query queryObject = makeSimpleAggregationQuery(5, Window.Unit.TIME, 100000, Window.Unit.TIME, 100000);

        // Using a time window here makes sure the query is not done but will closed after 3 calls
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                             SerializerDeserializer.toBytes(queryObject),
                                             EMPTY);
        bolt.execute(query);

        // Not active because it needs to be delayed
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_PRE_START_DELAY_TICKS - 1; ++i) {
            bolt.execute(tick);
            Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
        }
        bolt.execute(tick);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));

        List<BulletRecord> sentFirst = sendRawRecordTuplesTo(bolt, "42", 1);
        List<BulletRecord> sentSecond = sendRawRecordTuplesTo(bolt, "42", 1);

        // This will make it closed and emitted
        bolt.execute(tick);
        Assert.assertEquals(collector.getEmittedCount(), 1);
        List<BulletRecord> sent = new ArrayList<>(sentFirst);
        sent.addAll(sentSecond);

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), EMPTY);

        bolt.execute(tick);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));

    }

    @Test
    public void testQueryFinishedWhileBeingDelayed() {
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        bolt = new DonableJoinBolt(config, 2, false);
        setup(bolt);

        // This kind of query actually does not delay but making it true using DonableJoinBolt to test
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(5)), EMPTY);
        bolt.execute(query);

        // Not active yet
        Assert.assertEquals(context.getLongMetric(TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_PRE_START_DELAY_TICKS - 1; ++i) {
            bolt.execute(tick);
            Assert.assertEquals(collector.getEmittedCount(), 0);
            Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
        }

        // Still inactive
        List<BulletRecord> sentFirst = sendRawRecordTuplesTo(bolt, "42", 1);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));

        // After this, query is done while still in the pre-start delay state
        List<BulletRecord> sentSecond = sendRawRecordTuplesTo(bolt, "42", 1);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));

        // This will still be combined but it is now checked as being done and is emitted
        List<BulletRecord> sentThird = sendRawRecordTuplesTo(bolt, "42", 1);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));

        Assert.assertEquals(collector.getEmittedCount(), 2);
        List<BulletRecord> sent = new ArrayList<>(sentFirst);
        sent.addAll(sentSecond);
        sent.addAll(sentThird);

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), COMPLETED);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
    }

    @Test
    public void testQueryGettingDataWhileBeingDelayed() {
        config.set(BulletStormConfig.JOIN_BOLT_WINDOW_PRE_START_DELAY_TICKS, 3);
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        bolt = new DonableJoinBolt(config, 2, false);
        setup(bolt);

        // This kind of query actually does not delay but making it true using DonableJoinBolt to test
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", SerializerDeserializer.toBytes(makeRawQuery(5)), EMPTY);
        bolt.execute(query);

        // Not active yet
        Assert.assertEquals(context.getLongMetric(TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);

        bolt.execute(tick);

        // Still inactive. First call to isDone
        List<BulletRecord> sentFirst = sendRawRecordTuplesTo(bolt, "42", 1);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));

        // 2nd rotation (into 3rd level). isDone not called
        bolt.execute(tick);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));

        // This makes it rotate out pre-start and into queries. isDone is called since the queries are categorized
        bolt.execute(tick);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));
        Assert.assertEquals(collector.getEmittedCount(), 0);

        // One more tick categorizes the queries and sees it is done (because of the 3rd call to isDone)
        bolt.execute(tick);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(collector.getEmittedCount(), 2);

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sentFirst).asJSON(), COMPLETED);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
    }

    @Test
    public void testBatchTuple() {
        bolt = ComponentUtils.prepare(new JoinBolt(new BulletStormConfig("src/test/resources/test_config.yaml")), collector);
        bolt.replayCompleted = true;

        Tuple tuple = makeIDTuple(TupleClassifier.Type.BATCH_TUPLE, "JoinBolt-18");
        bolt.execute(tuple);

        Assert.assertEquals(collector.getAckedCount(), 1);
    }
}
