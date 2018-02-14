/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
/*
package com.yahoo.bullet.storm;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.yahoo.bullet.operations.AggregationOperations.DistributionType;
import com.yahoo.bullet.operations.SerializerDeserializer;
import com.yahoo.bullet.operations.aggregations.CountDistinct;
import com.yahoo.bullet.operations.aggregations.CountDistinctTest;
import com.yahoo.bullet.operations.aggregations.Distribution;
import com.yahoo.bullet.operations.aggregations.DistributionTest;
import com.yahoo.bullet.operations.aggregations.GroupBy;
import com.yahoo.bullet.operations.aggregations.GroupByTest;
import com.yahoo.bullet.operations.aggregations.TopK;
import com.yahoo.bullet.operations.aggregations.TopKTest;
import com.yahoo.bullet.operations.aggregations.grouping.GroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.AggregationUtils;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.querying.AggregationQuery;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.result.Metadata.Concept;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.sketches.frequencies.ErrorType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.yahoo.bullet.storm.testing.TestHelpers.assertJSONEquals;
import static com.yahoo.bullet.storm.testing.TestHelpers.getListBytes;
import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.COUNT_DISTINCT;
import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.DISTRIBUTION;
import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.GROUP;
import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.RAW;
import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.TOP_K;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.COUNT;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.SUM;
import static com.yahoo.bullet.operations.FilterOperations.FilterType.EQUALS;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.COUNT_FIELD;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.END_EXCLUSIVE;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.NEGATIVE_INFINITY_START;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.POSITIVE_INFINITY_END;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.PROBABILITY_FIELD;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.RANGE_FIELD;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.SEPARATOR;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.START_INCLUSIVE;
import static com.yahoo.bullet.parsing.AggregationUtils.makeAttributes;
import static com.yahoo.bullet.parsing.QueryUtils.getAggregationQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeAggregationQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeGroupFilterQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class JoinBoltTest {
    private static final com.yahoo.bullet.pubsub.Metadata METADATA = new com.yahoo.bullet.pubsub.Metadata();

    private CustomCollector collector;
    private CustomTopologyContext context;
    private JoinBolt bolt;

    private class ExpiringJoinBolt extends JoinBolt {
        private Map config = emptyMap();

        public ExpiringJoinBolt(Map config) {
            this.config = config;
        }

        public ExpiringJoinBolt() {
        }

        @Override
        protected AggregationQuery createQuery(Tuple queryTuple) {
            String id = queryTuple.getString(TopologyConstants.ID_POSITION);
            String queryString = queryTuple.getString(TopologyConstants.QUERY_POSITION);
            AggregationQuery spied = spy(getAggregationQuery(queryString, config));
            when(spied.isExpired()).thenReturn(false).thenReturn(true);
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
        return sendRawRecordTuplesTo(bolt, id, Aggregation.DEFAULT_SIZE);
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

    public static void enableMetadataInConfig(Map<String, Object> config, String metaConcept, String key) {
        List<Map<String, String>> metadataConfig =
                (List<Map<String, String>>) config.getOrDefault(BulletStormConfig.RESULT_METADATA_METRICS, new ArrayList<>());
        Map<String, String> conceptConfig = new HashMap<>();
        conceptConfig.put(BulletStormConfig.RESULT_METADATA_METRICS_CONCEPT_KEY, metaConcept);
        conceptConfig.put(BulletStormConfig.RESULT_METADATA_METRICS_NAME_KEY, key);
        metadataConfig.add(conceptConfig);

        config.put(BulletStormConfig.RESULT_METADATA_ENABLE, true);
        config.putIfAbsent(BulletStormConfig.RESULT_METADATA_METRICS, metadataConfig);
    }

    @BeforeMethod
    public void setup() {
        collector = new CustomCollector();
        bolt = ComponentUtils.prepare(new JoinBolt(), collector);
    }

    public void setup(Map<String, Object> config, JoinBolt bolt) {
        collector = new CustomCollector();
        context = new CustomTopologyContext();
        this.bolt = ComponentUtils.prepare(config, bolt, context, collector);
    }

    @Test
    public void testOutputFields() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        bolt.declareOutputFields(declarer);
        Fields expected = new Fields(TopologyConstants.ID_FIELD, TopologyConstants.RESULT_FIELD, TopologyConstants.METADATA_FIELD);
        Assert.assertTrue(declarer.areFieldsPresent(JoinBolt.JOIN_STREAM, false, expected));
    }

    @Test
    public void testUnknownTuple() {
        Tuple query = TupleUtils.makeTuple(TupleClassifier.Type.RECORD_TUPLE, RecordBox.get().add("a", "b").getRecord());
        bolt.execute(query);
        Assert.assertFalse(collector.wasAcked(query));
    }

    @Test
    public void testJoining() {
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{}", METADATA);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42");

        // We'd have <JSON, metadataTuple> as the expected tuple
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), METADATA);
        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testQueryExpiry() {
        bolt = ComponentUtils.prepare(new ExpiringJoinBolt(), collector);

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{}", METADATA);
        bolt.execute(query);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", Aggregation.DEFAULT_SIZE - 1);

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), METADATA);

        // Should cause an expiry and starts buffering the query for the query tickout
        bolt.execute(tick);
        // We need to tick the default query tickout to make the query emit
        for (int i = 0; i < JoinBolt.DEFAULT_QUERY_TICKOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(collector.wasTupleEmitted(expected));
        }
        bolt.execute(tick);

        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testEmit() {
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{}", METADATA);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42");

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON());
        Assert.assertFalse(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testFailJoiningForNoQuery() {
        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42");

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), METADATA);
        Assert.assertFalse(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 0);
    }

    @Test
    public void testJoiningAfterTickout() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        setup(config, new ExpiringJoinBolt());

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeAggregationQuery(RAW, 3), METADATA);
        bolt.execute(query);

        Assert.assertEquals(context.getLongMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", 2);

        // If we tick twice, the Query will be expired due to the ExpiringJoinBolt and put into bufferedQueries
        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        // We'd have <JSON, metadataTuple> as the expected tuple
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), METADATA);

        // We need to tick the default query tickout to make the query emit
        for (int i = 0; i < JoinBolt.DEFAULT_QUERY_TICKOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(collector.wasTupleEmitted(expected));
            Assert.assertEquals(context.getLongMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
            Assert.assertEquals(context.getLongMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));
        }
        // This will cause the emission
        bolt.execute(tick);
        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
        Assert.assertEquals(context.getLongMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
    }

    @Test
    public void testJoiningAfterLateArrivalBeforeTickout() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        setup(config, new ExpiringJoinBolt());

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeAggregationQuery(RAW, 3), METADATA);
        bolt.execute(query);

        Assert.assertEquals(context.getLongMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", 2);

        // If we tick twice, the Query will be expired due to the ExpiringJoinBolt and put into bufferedQueries
        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), METADATA);

        // We now tick a few times to get the query rotated but not discarded
        for (int i = 0; i < JoinBolt.DEFAULT_QUERY_TICKOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(collector.wasTupleEmitted(expected));
            Assert.assertEquals(context.getLongMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
            Assert.assertEquals(context.getLongMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));
        }
        // Now we satisfy the aggregation and see if it causes an emission
        List<BulletRecord> sentLate = sendRawRecordTuplesTo(bolt, "42", 1);
        sent.addAll(sentLate);

        // The expected record now should contain the sentLate ones too
        expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).asJSON(), METADATA);

        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
        Assert.assertEquals(context.getLongMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
    }

    @Test
    public void testMultiJoining() {
        bolt = ComponentUtils.prepare(new ExpiringJoinBolt(), collector);

        Tuple queryA = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{}", METADATA);
        bolt.execute(queryA);

        Tuple queryB = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "43", "{}", METADATA);
        bolt.execute(queryB);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);

        // This will not satisfy the query and will be emitted second
        List<BulletRecord> sentFirst = sendRawRecordTuplesTo(bolt, "42", Aggregation.DEFAULT_SIZE - 1);
        List<BulletRecord> sentSecond = sendRawRecordTuplesTo(bolt, "43");
        bolt.execute(tick);
        bolt.execute(tick);

        Tuple emittedFirst = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "43", Clip.of(sentSecond).asJSON(), METADATA);
        Assert.assertTrue(collector.wasNthEmitted(emittedFirst, 1));

        Tuple emittedSecond = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sentFirst).asJSON(), METADATA);
        // We need to tick the default query tickout to make the query emit
        for (int i = 0; i < JoinBolt.DEFAULT_QUERY_TICKOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(collector.wasTupleEmitted(emittedSecond));
        }
        // This will cause the emission
        bolt.execute(tick);
        Assert.assertTrue(collector.wasNthEmitted(emittedSecond, 2));
        Assert.assertEquals(collector.getAllEmitted().count(), 2);
    }

    @Test
    public void testErrorImmediate() {
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "garbage", METADATA);
        bolt.execute(query);

        Assert.assertEquals(collector.getAllEmitted().count(), 1);

        String error = Error.GENERIC_JSON_ERROR + ":\ngarbage\n" +
                       "IllegalStateException: Expected BEGIN_OBJECT but was STRING at line 1 column 1 path $";
        Error expectedError = Error.of(error, singletonList(Error.GENERIC_JSON_RESOLUTION));
        Metadata expectedMetadata = Metadata.of(expectedError);
        List<Object> expected = TupleUtils.makeTuple("42", Clip.of(expectedMetadata).asJSON(), METADATA).getValues();
        List<Object> actual = collector.getNthTupleEmittedTo(JoinBolt.JOIN_STREAM, 1).get();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testErrorEmittedProperly() {
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "garbage", METADATA);
        bolt.execute(query);

        Assert.assertEquals(collector.getAllEmitted().count(), 1);

        String error = Error.GENERIC_JSON_ERROR + ":\ngarbage\n" +
                       "IllegalStateException: Expected BEGIN_OBJECT but was STRING at line 1 column 1 path $";
        Error expectedError = Error.of(error, singletonList(Error.GENERIC_JSON_RESOLUTION));
        Metadata expectedMetadata = Metadata.of(expectedError);
        List<Object> expected = TupleUtils.makeTuple("42", Clip.of(expectedMetadata).asJSON(), METADATA).getValues();
        List<Object> actual = collector.getNthTupleEmittedTo(JoinBolt.JOIN_STREAM, 1).get();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testQueryIdentifierMetadata() {
        Map<String, Object> config = new HashMap<>();
        enableMetadataInConfig(config, Concept.QUERY_ID.getName(), "id");
        setup(config, new JoinBolt());

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{}", METADATA);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42");

        Metadata meta = new Metadata();
        meta.add("id", "42");
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).add(meta).asJSON(), METADATA);
        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testUnknownConceptMetadata() {
        Map<String, Object> config = new HashMap<>();
        enableMetadataInConfig(config, Concept.QUERY_ID.getName(), "id");
        enableMetadataInConfig(config, "foo", "bar");
        setup(config, new JoinBolt());

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{}", METADATA);
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42");

        Metadata meta = new Metadata();
        meta.add("id", "42");
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(sent).add(meta).asJSON(), METADATA);
        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testMultipleMetadata() {
        Map<String, Object> config = new HashMap<>();
        enableMetadataInConfig(config, Concept.QUERY_ID.getName(), "id");
        enableMetadataInConfig(config, Concept.QUERY_BODY.getName(), "query");
        enableMetadataInConfig(config, Concept.QUERY_CREATION_TIME.getName(), "created");
        enableMetadataInConfig(config, Concept.QUERY_TERMINATION_TIME.getName(), "finished");
        setup(config, new JoinBolt());

        long startTime = System.currentTimeMillis();

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{}", METADATA);
        bolt.execute(query);

        sendRawRecordTuplesTo(bolt, "42");

        long endTime = System.currentTimeMillis();

        Assert.assertEquals(collector.getAllEmitted().count(), 1);

        String response = (String) collector.getTuplesEmitted().findFirst().get().get(1);
        JsonParser parser = new JsonParser();
        JsonObject object = parser.parse(response).getAsJsonObject();

        String records = object.get(Clip.RECORDS_KEY).toString();
        assertJSONEquals(records, "[{\"field\":\"0\"}]");

        JsonObject meta = object.get(Clip.META_KEY).getAsJsonObject();
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
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeAggregationQuery(TOP_K, 5), METADATA);
        bolt.execute(query);
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testUnknownAggregation() {
        // Lowercase "top" is not valid and will not be parsed since there is no enum for it
        // In this case aggregation type should be set to null and an error should be emitted
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{\"aggregation\": {\"type\": \"garbage\"}}", METADATA);
        bolt.execute(query);

        Assert.assertEquals(collector.getAllEmitted().count(), 1);
        Error expectedError = Aggregation.TYPE_NOT_SUPPORTED_ERROR;
        Metadata expectedMetadata = Metadata.of(expectedError);
        List<Object> expected = TupleUtils.makeTuple("42", Clip.of(expectedMetadata).asJSON(), METADATA).getValues();
        List<Object> actual = collector.getNthTupleEmittedTo(JoinBolt.JOIN_STREAM, 1).get();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testUnhandledExceptionErrorEmitted() {
        // An empty query should throw an null-pointer exception which should be caught in JoinBolt
        // and an error should be emitted
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "", METADATA);
        bolt.execute(query);

        sendRawRecordTuplesTo(bolt, "42");

        Assert.assertEquals(collector.getAllEmitted().count(), 1);

        String error = Error.GENERIC_JSON_ERROR + ":\n\nNullPointerException: ";
        Error expectedError = Error.of(error, singletonList(Error.GENERIC_JSON_RESOLUTION));
        Metadata expectedMetadata = Metadata.of(expectedError);
        List<Object> expected = TupleUtils.makeTuple("42", Clip.of(expectedMetadata).asJSON(), METADATA).getValues();
        List<Object> actual = collector.getNthTupleEmittedTo(JoinBolt.JOIN_STREAM, 1).get();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testCounting() {
        bolt = ComponentUtils.prepare(new ExpiringJoinBolt(), collector);

        String filterQuery = makeGroupFilterQuery("timestamp", asList("1", "2"), EQUALS, GROUP, 1,
                                                  singletonList(new GroupOperation(COUNT, null, "cnt")));
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", filterQuery, METADATA);
        bolt.execute(query);

        // Send 5 GroupData with counts 1, 2, 3, 4, 5 to the JoinBolt
        IntStream.range(1, 6).forEach(i -> sendRawByteTuplesTo(bolt, "42", singletonList(getGroupDataWithCount("cnt", i))));

        // 1 + 2 + 3 + 4 + 5
        List<BulletRecord> result = singletonList(RecordBox.get().add("cnt", 15L).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(result).asJSON(), METADATA);


        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        // Should cause an expiry and starts buffering the query for the query tickout
        bolt.execute(tick);
        // We need to tick the default query tickout to make the query emit
        for (int i = 0; i < JoinBolt.DEFAULT_QUERY_TICKOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(collector.wasTupleEmitted(expected));
        }
        bolt.execute(tick);

        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testRawMicroBatchSizeGreaterThanOne() {
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeAggregationQuery(RAW, 5), METADATA);
        bolt.execute(query);

        // This will send 2 batches of 3 records each (total of 6 records).
        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, "42", 5, 3);

        List<BulletRecord> actualSent = sent.subList(0, 5);

        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(actualSent).asJSON(), METADATA);
        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testCountDistinct() {
        Map<Object, Object> config = CountDistinctTest.makeConfiguration(8, 512);

        CountDistinct distinct = CountDistinctTest.makeCountDistinct(config, singletonList("field"));

        IntStream.range(0, 256).mapToObj(i -> RecordBox.get().add("field", i).getRecord()).forEach(distinct::consume);
        byte[] first = distinct.getSerializedAggregation();

        distinct = CountDistinctTest.makeCountDistinct(config, singletonList("field"));

        IntStream.range(128, 256).mapToObj(i -> RecordBox.get().add("field", i).getRecord()).forEach(distinct::consume);
        byte[] second = distinct.getSerializedAggregation();

        // Send generated data to JoinBolt
        bolt = ComponentUtils.prepare(config, new ExpiringJoinBolt(config), collector);

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                             makeAggregationQuery(COUNT_DISTINCT, 1, null, Pair.of("field", "field")),
                                             METADATA);
        bolt.execute(query);

        sendRawByteTuplesTo(bolt, "42", asList(first, second));

        List<BulletRecord> result = singletonList(RecordBox.get().add(CountDistinct.DEFAULT_NEW_NAME, 256.0).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(result).asJSON(), METADATA);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);
        for (int i = 0; i < JoinBolt.DEFAULT_QUERY_TICKOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(collector.wasTupleEmitted(expected));
        }
        bolt.execute(tick);

        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testGroupBy() {
        final int entries = 16;
        Map<Object, Object> config = GroupByTest.makeConfiguration(entries);

        GroupBy groupBy = GroupByTest.makeGroupBy(config, singletonMap("fieldA", "A"), entries,
                                                  AggregationUtils.makeGroupOperation(COUNT, null, "cnt"),
                                                  AggregationUtils.makeGroupOperation(SUM, "fieldB", "sumB"));

        IntStream.range(0, 256).mapToObj(i -> RecordBox.get().add("fieldA", i % 16).add("fieldB", i / 16).getRecord())
                               .forEach(groupBy::consume);
        byte[] first = groupBy.getSerializedAggregation();

        groupBy = GroupByTest.makeGroupBy(config, singletonMap("fieldA", "A"), entries,
                                          AggregationUtils.makeGroupOperation(COUNT, null, "cnt"),
                                          AggregationUtils.makeGroupOperation(SUM, "fieldB", "sumB"));

        IntStream.range(256, 1024).mapToObj(i -> RecordBox.get().add("fieldA", i % 16).add("fieldB", i / 16).getRecord())
                                  .forEach(groupBy::consume);

        byte[] second = groupBy.getSerializedAggregation();

        // Send generated data to JoinBolt
        bolt = ComponentUtils.prepare(config, new ExpiringJoinBolt(config), collector);

        List<GroupOperation> operations = asList(new GroupOperation(COUNT, null, "cnt"),
                                                 new GroupOperation(SUM, "fieldB", "sumB"));
        String queryString = makeGroupFilterQuery("ts", singletonList("1"), EQUALS, GROUP, entries, operations,
                                                  Pair.of("fieldA", "A"));

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", queryString, METADATA);
        bolt.execute(query);

        sendRawByteTuplesTo(bolt, "42", asList(first, second));

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);
        for (int i = 0; i < JoinBolt.DEFAULT_QUERY_TICKOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertEquals(collector.getAllEmitted().count(), 0);
        }
        bolt.execute(tick);

        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testQueryCountingMetrics() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        setup(config, new ExpiringJoinBolt());

        String filterQuery = makeGroupFilterQuery("timestamp", asList("1", "2"), EQUALS, GROUP, 1,
                                                  singletonList(new GroupOperation(COUNT, null, "cnt")));
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", filterQuery, METADATA);

        Assert.assertEquals(context.getLongMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(JoinBolt.IMPROPER_QUERIES), Long.valueOf(0));

        bolt.execute(query);
        Assert.assertEquals(context.getLongMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));

        sendRawByteTuplesTo(bolt, "42", singletonList(getGroupDataWithCount("cnt", 21)));
        sendRawByteTuplesTo(bolt, "42", singletonList(getGroupDataWithCount("cnt", 21)));

        List<BulletRecord> result = singletonList(RecordBox.get().add("cnt", 42L).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(result).asJSON(), METADATA);


        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);

        // Should cause an expiry and starts buffering the query for the query tickout
        bolt.execute(tick);
        // We need to tick the default query tickout to make the query emit
        for (int i = 0; i < JoinBolt.DEFAULT_QUERY_TICKOUT - 1; ++i) {
            bolt.execute(tick);
        }
        Assert.assertFalse(collector.wasTupleEmitted(expected));
        Assert.assertEquals(context.getLongMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));

        bolt.execute(tick);
        Assert.assertEquals(context.getLongMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));

        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);

        bolt.execute(query);
        Assert.assertEquals(context.getLongMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(2));
        Assert.assertEquals(context.getLongMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(JoinBolt.IMPROPER_QUERIES), Long.valueOf(0));
    }

    @Test
    public void testImproperQueryCountingMetrics() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        setup(config, new JoinBolt());

        Assert.assertEquals(context.getLongMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(JoinBolt.IMPROPER_QUERIES), Long.valueOf(0));

        Tuple badQuery = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", METADATA);
        bolt.execute(badQuery);
        bolt.execute(badQuery);
        Assert.assertEquals(context.getLongMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(JoinBolt.IMPROPER_QUERIES), Long.valueOf(2));
    }

    @Test
    public void testCustomMetricEmitInterval() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Number> mapping = new HashMap<>();
        config.put(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);

        mapping.put(JoinBolt.ACTIVE_QUERIES, 1);
        mapping.put(JoinBolt.DEFAULT_BUILT_IN_METRICS_INTERVAL_KEY, 10);
        config.put(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING, mapping);
        setup(config, new ExpiringJoinBolt());

        String filterQuery = makeGroupFilterQuery("timestamp", asList("1", "2"), EQUALS, GROUP, 1,
                                                  singletonList(new GroupOperation(COUNT, null, "cnt")));
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", filterQuery, METADATA);

        Assert.assertEquals(context.getLongMetric(10, JoinBolt.CREATED_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(1, JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(10, JoinBolt.IMPROPER_QUERIES), Long.valueOf(0));

        bolt.execute(query);
        Assert.assertEquals(context.getLongMetric(10, JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(1, JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);
        for (int i = 0; i <= JoinBolt.DEFAULT_QUERY_TICKOUT; ++i) {
            bolt.execute(tick);
        }

        List<BulletRecord> result = singletonList(RecordBox.get().add("cnt", 42L).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(result).asJSON(), METADATA);
        Assert.assertFalse(collector.wasTupleEmitted(expected));

        Assert.assertEquals(context.getLongMetric(10, JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getLongMetric(1, JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getLongMetric(10, JoinBolt.IMPROPER_QUERIES), Long.valueOf(0));
    }

    @Test
    public void testDistribution() {
        Map<Object, Object> config = DistributionTest.makeConfiguration(10, 128);

        Distribution distribution = DistributionTest.makeDistribution(config, makeAttributes(DistributionType.PMF, 3),
                                                                      "field", 10, null);

        IntStream.range(0, 50).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                              .forEach(distribution::consume);

        byte[] first = distribution.getSerializedAggregation();

        distribution = DistributionTest.makeDistribution(config, makeAttributes(DistributionType.PMF, 3),
                                                         "field", 10, null);

        IntStream.range(50, 101).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                                .forEach(distribution::consume);

        byte[] second = distribution.getSerializedAggregation();

        bolt = ComponentUtils.prepare(config, new ExpiringJoinBolt(config), collector);

        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                             makeAggregationQuery(DISTRIBUTION, 10, DistributionType.PMF, "field",
                                                                  null, null, null, null, 3),
                                             METADATA);
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
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(results).asJSON(), METADATA);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);
        for (int i = 0; i < JoinBolt.DEFAULT_QUERY_TICKOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(collector.wasTupleEmitted(expected));
        }
        bolt.execute(tick);

        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testTopK() {
        Map<Object, Object> config = TopKTest.makeConfiguration(ErrorType.NO_FALSE_NEGATIVES, 16);

        Map<String, String> fields = new HashMap<>();
        fields.put("A", "");
        fields.put("B", "foo");
        TopK topK = TopKTest.makeTopK(config, makeAttributes(null, 5L), fields, 2, null);

        IntStream.range(0, 32).mapToObj(i -> RecordBox.get().add("A", i % 8).getRecord()).forEach(topK::consume);

        byte[] first = topK.getSerializedAggregation();

        topK = TopKTest.makeTopK(config, makeAttributes(null, 5L), fields, 2, null);

        IntStream.range(0, 8).mapToObj(i -> RecordBox.get().add("A", i % 2).getRecord()).forEach(topK::consume);

        byte[] second = topK.getSerializedAggregation();

        bolt = ComponentUtils.prepare(config, new ExpiringJoinBolt(config), collector);

        String aggregationQuery = makeAggregationQuery(TOP_K, 2, 5L, "cnt", Pair.of("A", ""), Pair.of("B", "foo"));
        Tuple query = TupleUtils.makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", aggregationQuery, METADATA);
        bolt.execute(query);

        sendRawByteTuplesTo(bolt, "42", asList(first, second));

        BulletRecord expectedA = RecordBox.get().add("A", "0").add("foo", "null").add("cnt", 8L).getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "1").add("foo", "null").add("cnt", 8L).getRecord();

        List<BulletRecord> results = asList(expectedA, expectedB);
        Tuple expected = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "42", Clip.of(results).asJSON(), METADATA);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);
        for (int i = 0; i < JoinBolt.DEFAULT_QUERY_TICKOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(collector.wasTupleEmitted(expected));
        }
        bolt.execute(tick);

        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }
}
*/
