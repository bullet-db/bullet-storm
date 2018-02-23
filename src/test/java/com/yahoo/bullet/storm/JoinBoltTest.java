/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.yahoo.bullet.aggregations.CountDistinct;
import com.yahoo.bullet.aggregations.CountDistinctTest;
import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.aggregations.DistributionTest;
import com.yahoo.bullet.aggregations.GroupBy;
import com.yahoo.bullet.aggregations.GroupByTest;
import com.yahoo.bullet.aggregations.TopK;
import com.yahoo.bullet.aggregations.TopKTest;
import com.yahoo.bullet.aggregations.grouping.GroupData;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.AggregationUtils;
import com.yahoo.bullet.parsing.ParsingError;
import com.yahoo.bullet.parsing.QueryUtils;
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.querying.Querier;
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
import com.yahoo.sketches.frequencies.ErrorType;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.SUM;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.COUNT_FIELD;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.END_EXCLUSIVE;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.NEGATIVE_INFINITY_START;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.POSITIVE_INFINITY_END;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.PROBABILITY_FIELD;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.RANGE_FIELD;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.SEPARATOR;
import static com.yahoo.bullet.aggregations.sketches.QuantileSketch.START_INCLUSIVE;
import static com.yahoo.bullet.parsing.Aggregation.Type.COUNT_DISTINCT;
import static com.yahoo.bullet.parsing.Aggregation.Type.DISTRIBUTION;
import static com.yahoo.bullet.parsing.Aggregation.Type.GROUP;
import static com.yahoo.bullet.parsing.Aggregation.Type.RAW;
import static com.yahoo.bullet.parsing.Aggregation.Type.TOP_K;
import static com.yahoo.bullet.parsing.AggregationUtils.makeAttributes;
import static com.yahoo.bullet.parsing.Clause.Operation.EQUALS;
import static com.yahoo.bullet.parsing.QueryUtils.makeAggregationQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeGroupFilterQuery;
import static com.yahoo.bullet.storm.testing.TestHelpers.assertJSONEquals;
import static com.yahoo.bullet.storm.testing.TestHelpers.getListBytes;
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

        public DonableJoinBolt(BulletStormConfig config) {
            this(config, 1, false);
        }

        public DonableJoinBolt(BulletStormConfig config, int doneAfter, boolean shouldBuffer) {
            super(config);
            this.doneAfter = doneAfter;
            this.shouldBuffer = shouldBuffer;
        }

        @Override
        protected Querier createQuerier(String id, String query, BulletConfig config) {
            // Each new querier will be done doneAfter more times than the last querier
            Querier spied = spy(super.createQuerier(id, query, config));
            List<Boolean> doneAnswers = IntStream.range(0, doneAfter).mapToObj(i -> false)
                                                 .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
            doneAnswers.add(true);
            doAnswer(returnsElementsOf(doneAnswers)).when(spied).isDone();
            doReturn(shouldBuffer).when(spied).shouldBuffer();
            return spied;
        }
    }

    // This sends ceil(n / batchSize) batches to the bolt
    private List<BulletRecord> sendRawRecordTuplesTo(IRichBolt bolt, String id, int n, int batchSize) {
        List<BulletRecord> sent = new ArrayList<>();
        for (int i = 0; i < n; i += batchSize) {
            BulletRecord[] batch = new BulletRecord[batchSize];
            for (int j = 0; j < batchSize; ++j) {
                batch[j] = RecordBox.get().add("field", String.valueOf(i + j)).getRecord();
            }
            Tuple tuple = TupleUtils.makeIDTuple(TupleClassifier.Type.DATA_TUPLE, id, getListBytes(batch));
            bolt.execute(tuple);
            sent.addAll(asList(batch));
        }
        return sent;
    }

    private List<BulletRecord> sendRawRecordTuplesTo(IRichBolt bolt, String id, int n) {
        return sendRawRecordTuplesTo(bolt, id, n, 1);
    }

    private List<BulletRecord> sendRawRecordTuplesTo(IRichBolt bolt, String id) {
        return sendRawRecordTuplesTo(bolt, id, RAW_MAX_SIZE);
    }

    private void sendRawByteTuplesTo(IRichBolt bolt, String id, List<byte[]> data) {
        for (byte[] b : data) {
            Tuple tuple = TupleUtils.makeIDTuple(TupleClassifier.Type.DATA_TUPLE, id, b);
            bolt.execute(tuple);
        }
    }

    private byte[] getGroupDataWithCount(String countField, int count) {
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

    private boolean isSameTuple(List<Object> actual, List<Object> expected) {
        boolean result;
        result = actual.size() == 3;
        result &= actual.size() == expected.size();
        result &= actual.get(0).equals(expected.get(0));
        result &= actual.get(1).equals(expected.get(1));
        return result;
    }

    private boolean tupleEquals(List<Object> actual, Tuple expectedTuple) {
        List<Object> expected = expectedTuple.getValues();
        boolean result = isSameTuple(actual, expected);
        if (!result) {
            return false;
        }
        Metadata actualMetadata = (Metadata) actual.get(2);
        Metadata expectedMetadata = (Metadata) expected.get(2);
        if (actualMetadata.getSignal() != expectedMetadata.getSignal()) {
            return false;
        }
        Serializable actualContent = actualMetadata.getContent();
        Serializable expectedContent = expectedMetadata.getContent();
        return actualContent == expectedContent || actualContent.equals(expectedContent);
    }

    private boolean wasResultEmittedTo(String stream, Tuple expectedTuple) {
        return collector.getAllEmittedTo(stream).anyMatch(t -> tupleEquals(t.getTuple(), expectedTuple));
    }

    private boolean wasResultEmitted(Tuple expectedTuple) {
        return collector.getTuplesEmitted().anyMatch(t -> tupleEquals(t, expectedTuple));
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
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{}", EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42");

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), COMPLETED);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testFailJoiningForNoQuery() {
        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42");

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), COMPLETED);
        Assert.assertFalse(wasResultEmitted(expected));
        Assert.assertEquals(collector.getAllEmitted().count(), 0);
    }

    @Test
    public void testQueryNotDoneButIsTimeBased() {
        // This bolt will be done after combining RAW_MAX_SIZE - 1 times and is a shouldBuffer, so it is buffered.
        bolt = new DonableJoinBolt(config, RAW_MAX_SIZE - 1, true);
        setup(bolt);

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{}", EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", RAW_MAX_SIZE - 1);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        // Should make isDone true but query is a time based window so it gets buffered.
        bolt.execute(tick);

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), COMPLETED);

        // We need to tick the default query tickout to make the query emit
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_TICK_TIMEOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        }
        // Should emit on the last tick
        bolt.execute(tick);

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testJoiningAfterLateArrivalBeforeTickout() {
        bolt = new DonableJoinBolt(config, 2, true);
        setup(bolt);

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeAggregationQuery(RAW, 3), EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", 2);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), COMPLETED);

        // We now tick a few times to get the query rotated but not discarded
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_TICK_TIMEOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        }
        // Now we satisfy the aggregation and see if it causes an emission
        List<BulletRecord> sentLate = sendRawRecordTuplesTo(bolt, "42", 1);
        sent.addAll(sentLate);

        // The expected record now should contain the sentLate ones too
        expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), COMPLETED);

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testMultiJoining() {
        DonableJoinBolt donableJoinBolt = new DonableJoinBolt(config, 2, true);
        bolt = donableJoinBolt;
        setup(bolt);

        Tuple queryA = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeAggregationQuery(RAW, 3), EMPTY);
        bolt.execute(queryA);

        donableJoinBolt.shouldBuffer = false;
        Tuple queryB = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "43", makeAggregationQuery(RAW, 3), EMPTY);
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
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_TICK_TIMEOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(wasResultEmitted(emittedSecond));
        }
        // This will cause the emission
        bolt.execute(tick);
        Assert.assertTrue(wasResultEmitted(emittedSecond));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 2);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 2);
    }

    @Test
    public void testErrorEmittedProperly() {
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "garbage", EMPTY);
        bolt.execute(query);

        Assert.assertEquals(collector.getAllEmitted().count(), 1);

        String error = ParsingError.GENERIC_JSON_ERROR + ":\ngarbage\n" +
                       "IllegalStateException: Expected BEGIN_OBJECT but was STRING at line 1 column 1 path $";
        BulletError expectedError = ParsingError.makeError(error, ParsingError.GENERIC_JSON_RESOLUTION);
        Meta expectedMetadata = Meta.of(expectedError);
        List<Object> expected = TupleUtils.makeTuple("42", Clip.of(expectedMetadata).asJSON(), FAILED).getValues();
        List<Object> actual = collector.getNthTupleEmittedTo(TopologyConstants.RESULT_STREAM, 1).get();

        Assert.assertTrue(isSameTuple(actual, expected));
    }

    @Test
    public void testQueryIdentifierMetadata() {
        config = configWithRawMaxAndEmptyMeta();
        enableMetadataInConfig(config, Concept.QUERY_ID.getName(), "id");
        setup(new JoinBolt(config));

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{}", EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42");

        Meta meta = new Meta();
        meta.add("id", "42");
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).add(meta).asJSON(), COMPLETED);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testUnknownConceptMetadata() {
        config = configWithRawMaxAndEmptyMeta();
        enableMetadataInConfig(config, Concept.QUERY_ID.getName(), "id");
        enableMetadataInConfig(config, "foo", "bar");
        setup(new JoinBolt(config));

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{}", EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42");

        Meta meta = new Meta();
        meta.add("id", "42");
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).add(meta).asJSON(), COMPLETED);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testMultipleMeta() {
        config = configWithRawMaxAndEmptyMeta();
        enableMetadataInConfig(config, Concept.QUERY_ID.getName(), "id");
        enableMetadataInConfig(config, Concept.QUERY_BODY.getName(), "query");
        enableMetadataInConfig(config, Concept.QUERY_RECEIVE_TIME.getName(), "created");
        enableMetadataInConfig(config, Concept.QUERY_FINISH_TIME.getName(), "finished");
        setup(new JoinBolt(config));

        long startTime = System.currentTimeMillis();

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{}", COMPLETED);
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
        String actualID = meta.get("id").getAsString();
        String queryBody = meta.get("query").getAsString();
        long createdTime = meta.get("created").getAsLong();
        long finishedTime = meta.get("finished").getAsLong();

        Assert.assertEquals(actualID, "42");
        Assert.assertEquals(queryBody, "{}");
        Assert.assertTrue(createdTime <= finishedTime);
        Assert.assertTrue(createdTime >= startTime && createdTime <= endTime);
    }

    @Test
    public void testUnsupportedAggregation() {
        // "TOP_K" aggregation type not currently supported - error should be emitted
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeAggregationQuery(TOP_K, 5), EMPTY);
        bolt.execute(query);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 0);
    }

    @Test
    public void testUnknownAggregation() {
        // Lowercase "top" is not valid and will not be parsed since there is no enum for it
        // In this case aggregation type should be set to null and an error should be emitted
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{\"aggregation\": {\"type\": \"garbage\"}}", COMPLETED);
        bolt.execute(query);

        Assert.assertEquals(collector.getAllEmitted().count(), 1);
        BulletError expectedError = Aggregation.TYPE_NOT_SUPPORTED_ERROR;
        Meta expectedMetadata = Meta.of(expectedError);
        List<Object> expected = TupleUtils.makeTuple("42", Clip.of(expectedMetadata).asJSON(), COMPLETED).getValues();
        List<Object> actual = collector.getNthTupleEmittedTo(TopologyConstants.RESULT_STREAM, 1).get();
        Assert.assertTrue(isSameTuple(actual, expected));
    }

    @Test
    public void testUnhandledExceptionErrorEmitted() {
        // An empty query should throw an null-pointer exception which should be caught in JoinBolt
        // and an error should be emitted
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "", EMPTY);
        bolt.execute(query);

        sendRawRecordTuplesTo(bolt, "42");

        Assert.assertEquals(collector.getAllEmitted().count(), 1);

        String error = ParsingError.GENERIC_JSON_ERROR + ":\n\nNullPointerException: ";
        BulletError expectedError = ParsingError.makeError(error, ParsingError.GENERIC_JSON_RESOLUTION);
        Meta expectedMetadata = Meta.of(expectedError);
        List<Object> expected = TupleUtils.makeTuple("42", Clip.of(expectedMetadata).asJSON(), COMPLETED).getValues();
        List<Object> actual = collector.getNthTupleEmittedTo(TopologyConstants.RESULT_STREAM, 1).get();
        Assert.assertTrue(isSameTuple(actual, expected));
    }

    @Test
    public void testCounting() {
        bolt = new DonableJoinBolt(config, 5, true);
        setup(bolt);

        String filterQuery = makeGroupFilterQuery("timestamp", asList("1", "2"), EQUALS, GROUP, 1,
                                                  singletonList(new GroupOperation(COUNT, null, "cnt")));
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", filterQuery, EMPTY);
        bolt.execute(query);

        // Send 5 GroupData with counts 1, 2, 3, 4, 5 to the JoinBolt
        IntStream.range(1, 6).forEach(i -> sendRawByteTuplesTo(bolt, "42", singletonList(getGroupDataWithCount("cnt", i))));

        // 1 + 2 + 3 + 4 + 5
        List<BulletRecord> result = singletonList(RecordBox.get().add("cnt", 15L).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(result).asJSON(), COMPLETED);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        // Should starts buffering the query for the query tickout
        bolt.execute(tick);
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_TICK_TIMEOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        }
        bolt.execute(tick);

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testRawQueryDoneButNotTimedOutWithExcessRecords() {
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeAggregationQuery(RAW, 5), EMPTY);
        bolt.execute(query);

        // This will send 2 batches of 3 records each (total of 6 records).
        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", 5, 3);

        List<BulletRecord> actualSent = sent.subList(0, 5);

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(actualSent).asJSON(), COMPLETED);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testCountDistinct() {
        BulletConfig bulletConfig = CountDistinctTest.makeConfiguration(8, 512);

        CountDistinct distinct = CountDistinctTest.makeCountDistinct(bulletConfig, singletonList("field"));

        IntStream.range(0, 256).mapToObj(i -> RecordBox.get().add("field", i).getRecord()).forEach(distinct::consume);
        byte[] first = distinct.getData();

        distinct = CountDistinctTest.makeCountDistinct(bulletConfig, singletonList("field"));

        IntStream.range(128, 256).mapToObj(i -> RecordBox.get().add("field", i).getRecord()).forEach(distinct::consume);
        byte[] second = distinct.getData();

        // Send generated data to JoinBolt
        bolt = new DonableJoinBolt(config, 2, true);
        setup(bolt);

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                             makeAggregationQuery(COUNT_DISTINCT, 1, null, Pair.of("field", "field")),
                                             EMPTY);
        bolt.execute(query);

        sendRawByteTuplesTo(bolt, "42", asList(first, second));

        List<BulletRecord> result = singletonList(RecordBox.get().add(CountDistinct.DEFAULT_NEW_NAME, 256.0).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(result).asJSON(), COMPLETED);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_TICK_TIMEOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        }
        bolt.execute(tick);

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testGroupBy() {
        final int entries = 16;
        BulletConfig bulletConfig = GroupByTest.makeConfiguration(entries);
        GroupBy groupBy = GroupByTest.makeGroupBy(bulletConfig, singletonMap("fieldA", "A"), entries,
                                                  AggregationUtils.makeGroupOperation(COUNT, null, "cnt"),
                                                  AggregationUtils.makeGroupOperation(SUM, "fieldB", "sumB"));

        IntStream.range(0, 256).mapToObj(i -> RecordBox.get().add("fieldA", i % 16).add("fieldB", i / 16).getRecord())
                               .forEach(groupBy::consume);
        byte[] first = groupBy.getData();

        groupBy = GroupByTest.makeGroupBy(bulletConfig, singletonMap("fieldA", "A"), entries,
                                          AggregationUtils.makeGroupOperation(COUNT, null, "cnt"),
                                          AggregationUtils.makeGroupOperation(SUM, "fieldB", "sumB"));

        IntStream.range(256, 1024).mapToObj(i -> RecordBox.get().add("fieldA", i % 16).add("fieldB", i / 16).getRecord())
                                  .forEach(groupBy::consume);

        byte[] second = groupBy.getData();

        // Send generated data to JoinBolt
        bolt = new DonableJoinBolt(config, 2, true);
        setup(bolt);

        List<GroupOperation> operations = asList(new GroupOperation(COUNT, null, "cnt"),
                                                 new GroupOperation(SUM, "fieldB", "sumB"));
        String queryString = makeGroupFilterQuery("ts", singletonList("1"), EQUALS, GROUP, entries, operations,
                                                  Pair.of("fieldA", "A"));

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", queryString, EMPTY);
        bolt.execute(query);

        sendRawByteTuplesTo(bolt, "42", asList(first, second));

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_TICK_TIMEOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertEquals(collector.getAllEmitted().count(), 0);
        }
        bolt.execute(tick);

        Assert.assertEquals(collector.getAllEmitted().count(), 2);
        String response = (String) collector.getMthElementFromNthTupleEmittedTo(TopologyConstants.RESULT_STREAM, 1, 1).get();
        JsonParser parser = new JsonParser();
        JsonObject actual = parser.parse(response).getAsJsonObject();
        JsonArray actualRecords = actual.get(Clip.RECORDS_KEY).getAsJsonArray();
        Assert.assertEquals(actualRecords.size(), 16);
    }

    @Test
    public void testQueryCountingMetrics() {
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        bolt = new DonableJoinBolt(config, 2, true);
        setup(bolt);

        String filterQuery = makeGroupFilterQuery("timestamp", asList("1", "2"), EQUALS, GROUP, 1,
                                                  singletonList(new GroupOperation(COUNT, null, "cnt")));
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", filterQuery, EMPTY);

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
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_TICK_TIMEOUT - 1; ++i) {
            bolt.execute(tick);
        }
        Assert.assertFalse(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));
        bolt.execute(tick);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);

        bolt.execute(query);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(2));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.IMPROPER_QUERIES_METRIC), Long.valueOf(0));
    }

    @Test
    public void testImproperQueryCountingMetrics() {
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        setup(new JoinBolt(config));

        Assert.assertEquals(context.getLongMetric(TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(TopologyConstants.IMPROPER_QUERIES_METRIC), Long.valueOf(0));

        Tuple badQuery = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", EMPTY);
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
        bolt = new DonableJoinBolt(config, BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_TICK_TIMEOUT, false);
        setup(bolt);

        String filterQuery = makeGroupFilterQuery("timestamp", asList("1", "2"), EQUALS, GROUP, 1,
                                                  singletonList(new GroupOperation(COUNT, null, "cnt")));
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", filterQuery, EMPTY);

        Assert.assertEquals(context.getLongMetric(10, TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(1, TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(10, TopologyConstants.IMPROPER_QUERIES_METRIC), Long.valueOf(0));

        bolt.execute(query);
        Assert.assertEquals(context.getLongMetric(10, TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(1, TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        for (int i = 0; i <= BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_TICK_TIMEOUT; ++i) {
            bolt.execute(tick);
        }

        List<BulletRecord> result = singletonList(RecordBox.get().add("cnt", 0L).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(result).asJSON(), COMPLETED);
        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));

        Assert.assertEquals(context.getLongMetric(10, TopologyConstants.CREATED_QUERIES_METRIC), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(1, TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(10, TopologyConstants.IMPROPER_QUERIES_METRIC), Long.valueOf(0));
    }

    @Test
    public void testDistribution() {
        BulletConfig bulletConfig = DistributionTest.makeConfiguration(10, 128);

        Distribution distribution = DistributionTest.makeDistribution(bulletConfig, makeAttributes(Distribution.Type.PMF, 3),
                                                                      "field", 10, null);

        IntStream.range(0, 50).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                              .forEach(distribution::consume);

        byte[] first = distribution.getData();

        distribution = DistributionTest.makeDistribution(bulletConfig, makeAttributes(Distribution.Type.PMF, 3),
                                                         "field", 10, null);

        IntStream.range(50, 101).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                                .forEach(distribution::consume);

        byte[] second = distribution.getData();

        bolt = new DonableJoinBolt(config, 2, true);
        setup(bolt);

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                             makeAggregationQuery(DISTRIBUTION, 10, Distribution.Type.PMF, "field",
                                                                  null, null, null, null, 3),
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
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_TICK_TIMEOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        }
        bolt.execute(tick);

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testTopK() {
        BulletConfig bulletConfig = TopKTest.makeConfiguration(ErrorType.NO_FALSE_NEGATIVES, 16);

        Map<String, String> fields = new HashMap<>();
        fields.put("A", "");
        fields.put("B", "foo");
        TopK topK = TopKTest.makeTopK(bulletConfig, makeAttributes(null, 5L), fields, 2, null);

        IntStream.range(0, 32).mapToObj(i -> RecordBox.get().add("A", i % 8).getRecord()).forEach(topK::consume);

        byte[] first = topK.getData();

        topK = TopKTest.makeTopK(bulletConfig, makeAttributes(null, 5L), fields, 2, null);

        IntStream.range(0, 8).mapToObj(i -> RecordBox.get().add("A", i % 2).getRecord()).forEach(topK::consume);

        byte[] second = topK.getData();

        bolt = new DonableJoinBolt(config, 2, true);
        setup(bolt);

        String aggregationQuery = makeAggregationQuery(TOP_K, 2, 5L, "cnt", Pair.of("A", ""), Pair.of("B", "foo"));
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", aggregationQuery, EMPTY);
        bolt.execute(query);

        sendRawByteTuplesTo(bolt, "42", asList(first, second));

        BulletRecord expectedA = RecordBox.get().add("A", "0").add("foo", "null").add("cnt", 8L).getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "1").add("foo", "null").add("cnt", 8L).getRecord();

        List<BulletRecord> results = asList(expectedA, expectedB);
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(results).asJSON(), COMPLETED);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        for (int i = 0; i < BulletStormConfig.DEFAULT_JOIN_BOLT_QUERY_TICK_TIMEOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        }
        bolt.execute(tick);

        Assert.assertTrue(wasResultEmittedTo(TopologyConstants.RESULT_STREAM, expected));
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.RESULT_STREAM).count(), 1);
        Assert.assertEquals(collector.getAllEmittedTo(TopologyConstants.FEEDBACK_STREAM).count(), 1);
    }

    @Test
    public void testKillSignal() {
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        setup(bolt);

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{}", EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", RAW_MAX_SIZE - 1);

        Assert.assertEquals(collector.getAllEmitted().count(), 0);

        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));

        Tuple kill = TupleUtils.makeIDTuple(TupleClassifier.Type.METADATA_TUPLE, "42",
                                            new Metadata(Metadata.Signal.KILL, null));
        bolt.execute(kill);

        Assert.assertEquals(collector.getAllEmitted().count(), 0);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
    }

    @Test
    public void testCompleteSignal() {
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        setup(bolt);

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{}", EMPTY);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", RAW_MAX_SIZE - 1);

        Assert.assertEquals(collector.getAllEmitted().count(), 0);

        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(1));

        Tuple complete = TupleUtils.makeIDTuple(TupleClassifier.Type.METADATA_TUPLE, "42",
                                                new Metadata(Metadata.Signal.COMPLETE, null));
        bolt.execute(complete);

        Assert.assertEquals(collector.getAllEmitted().count(), 0);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_QUERIES_METRIC), Long.valueOf(0));
    }
}
