/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.yahoo.bullet.BulletConfig;
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
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;
import static com.yahoo.bullet.TestHelpers.getListBytes;
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
        protected AggregationQuery getQuery(Long id, String queryString) {
            AggregationQuery spied = spy(getAggregationQuery(queryString, config));
            when(spied.isExpired()).thenReturn(false).thenReturn(true);
            return spied;
        }
    }

    // This sends ceil(n / batchSize) batches to the bolt
    private List<BulletRecord> sendRawRecordTuplesTo(IRichBolt bolt, Long id, int n, int batchSize) {
        List<BulletRecord> sent = new ArrayList<>();
        for (int i = 0; i < n; i += batchSize) {
            BulletRecord[] batch = new BulletRecord[batchSize];
            for (int j = 0; j < batchSize; ++j) {
                batch[j] = RecordBox.get().add("field", String.valueOf(i + j)).getRecord();
            }
            Tuple tuple = TupleUtils.makeIDTuple(TupleType.Type.FILTER_TUPLE, id, getListBytes(batch));
            bolt.execute(tuple);
            sent.addAll(asList(batch));
        }
        return sent;
    }

    private List<BulletRecord> sendRawRecordTuplesTo(IRichBolt bolt, Long id, int n) {
        return sendRawRecordTuplesTo(bolt, id, n, 1);
    }

    private List<BulletRecord> sendRawRecordTuplesTo(IRichBolt bolt, Long id) {
        return sendRawRecordTuplesTo(bolt, id, Aggregation.DEFAULT_SIZE);
    }

    private void sendRawByteTuplesTo(IRichBolt bolt, Long id, List<byte[]> data) {
        for (byte[] b : data) {
            Tuple tuple = TupleUtils.makeIDTuple(TupleType.Type.FILTER_TUPLE, id, b);
            bolt.execute(tuple);
        }
    }

    private byte[] getGroupDataWithCount(String countField, int count) {
        GroupData groupData = new GroupData(new HashSet<>(singletonList(new GroupOperation(COUNT,
                                                                        null, countField))));
        IntStream.range(0, count).forEach(i -> groupData.consume(RecordBox.get().getRecord()));
        return SerializerDeserializer.toBytes(groupData);
    }

    public static void enableMetadataInConfig(Map<String, Object> config, String metaConcept, String key) {
        List<Map<String, String>> metadataConfig =
                (List<Map<String, String>>) config.getOrDefault(BulletConfig.RESULT_METADATA_METRICS, new ArrayList<>());
        Map<String, String> conceptConfig = new HashMap<>();
        conceptConfig.put(BulletConfig.RESULT_METADATA_METRICS_CONCEPT_KEY, metaConcept);
        conceptConfig.put(BulletConfig.RESULT_METADATA_METRICS_NAME_KEY, key);
        metadataConfig.add(conceptConfig);

        config.put(BulletConfig.RESULT_METADATA_ENABLE, true);
        config.putIfAbsent(BulletConfig.RESULT_METADATA_METRICS, metadataConfig);
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
        Fields expected = new Fields(TopologyConstants.JOIN_FIELD, TopologyConstants.RETURN_FIELD);
        Assert.assertTrue(declarer.areFieldsPresent(JoinBolt.JOIN_STREAM, false, expected));
    }

    @Test
    public void testUnknownTuple() {
        Tuple query = TupleUtils.makeTuple(TupleType.Type.RECORD_TUPLE, RecordBox.get().add("a", "b").getRecord());
        bolt.execute(query);
        Assert.assertFalse(collector.wasAcked(query));
    }

    @Test
    public void testJoining() {
        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "{}");
        bolt.execute(query);

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, 42L);

        // We'd have <JSON, returnInfo> as the expected tuple
        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(sent).asJSON(), "");
        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testQueryExpiry() {
        bolt = ComponentUtils.prepare(new ExpiringJoinBolt(), collector);

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "{}");
        bolt.execute(query);

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, 42L, Aggregation.DEFAULT_SIZE - 1);

        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(sent).asJSON(), "");

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
    public void testFailJoiningForNoQuery() {
        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, 42L);

        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(sent).asJSON(), "");
        Assert.assertFalse(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 0);
    }

    @Test
    public void testFailJoiningForNoReturn() {
        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "{}");
        bolt.execute(query);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, 42L);

        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(sent).asJSON(), "");
        Assert.assertFalse(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 0);
    }

    @Test
    public void testFailJoiningForNoQueryOrReturn() {
        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, 42L);

        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(sent).asJSON(), "");
        Assert.assertFalse(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 0);
    }

    @Test
    public void testJoiningAfterTickout() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        setup(config, new ExpiringJoinBolt());

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                             makeAggregationQuery(RAW, 3));
        bolt.execute(query);

        Assert.assertEquals(context.getCountForMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, 42L, 2);

        // If we tick twice, the Query will be expired due to the ExpiringJoinBolt and put into bufferedQuerys
        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        // We'd have <JSON, returnInfo> as the expected tuple
        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(sent).asJSON(), "");

        // We need to tick the default query tickout to make the query emit
        for (int i = 0; i < JoinBolt.DEFAULT_QUERY_TICKOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(collector.wasTupleEmitted(expected));
            Assert.assertEquals(context.getCountForMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
            Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));
        }
        // This will cause the emission
        bolt.execute(tick);
        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
        Assert.assertEquals(context.getCountForMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
    }

    @Test
    public void testJoiningAfterLateArrivalBeforeTickout() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        setup(config, new ExpiringJoinBolt());

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                             makeAggregationQuery(RAW, 3));
        bolt.execute(query);

        Assert.assertEquals(context.getCountForMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, 42L, 2);


        // If we tick twice, the Query will be expired due to the ExpiringJoinBolt and put into bufferedQuerys
        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(sent).asJSON(), "");

        // We now tick a few times to get the query rotated but not discarded
        for (int i = 0; i < JoinBolt.DEFAULT_QUERY_TICKOUT - 1; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(collector.wasTupleEmitted(expected));
            Assert.assertEquals(context.getCountForMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
            Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));
        }
        // Now we satisfy the aggregation and see if it causes an emission
        List<BulletRecord> sentLate = sendRawRecordTuplesTo(bolt, 42L, 1);
        sent.addAll(sentLate);

        // The expected record now should contain the sentLate ones too
        expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(sent).asJSON(), "");

        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
        Assert.assertEquals(context.getCountForMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
    }

    @Test
    public void testFailJoiningAfterLateArrivalSinceNoReturn() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        setup(config, new ExpiringJoinBolt());

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                             makeAggregationQuery(RAW, 3));
        bolt.execute(query);

        Assert.assertEquals(context.getCountForMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));
        // No return information

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, 42L, 2);

        // If we tick twice, the Query will be expired due to the ExpiringJoinBolt and put into bufferedQuerys
        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);
        Assert.assertEquals(context.getCountForMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));

        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(sent).asJSON(), "");

        // Even if the aggregation has data, rotation does not cause its emission since we have no return information.
        for (int i = 0; i < JoinBolt.DEFAULT_QUERY_TICKOUT; ++i) {
            bolt.execute(tick);
            Assert.assertFalse(collector.wasTupleEmitted(expected));
        }
        Assert.assertEquals(context.getCountForMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
    }

    @Test
    public void testMultiJoining() {
        bolt = ComponentUtils.prepare(new ExpiringJoinBolt(), collector);

        Tuple queryA = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "{}");
        bolt.execute(queryA);

        Tuple queryB = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 43L, "{}");
        bolt.execute(queryB);

        Tuple returnInfoOne = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfoOne);

        Tuple returnInfoTwo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 43L, "");
        bolt.execute(returnInfoTwo);

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);

        // This will not satisfy the query and will be emitted second
        List<BulletRecord> sentFirst = sendRawRecordTuplesTo(bolt, 42L, Aggregation.DEFAULT_SIZE - 1);
        List<BulletRecord> sentSecond = sendRawRecordTuplesTo(bolt, 43L);
        bolt.execute(tick);
        bolt.execute(tick);

        Tuple emittedFirst = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(sentSecond).asJSON(), "");
        Assert.assertTrue(collector.wasNthEmitted(emittedFirst, 1));

        Tuple emittedSecond = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(sentFirst).asJSON(), "");
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
        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "garbage");
        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);
        bolt.execute(query);

        Assert.assertEquals(collector.getAllEmitted().count(), 1);

        Error expectedError = Error.of(Error.GENERIC_JSON_ERROR + ":\ngarbage\n" +
                        "IllegalStateException: Expected BEGIN_OBJECT but was STRING at line 1 column 1 path $",
                singletonList(Error.GENERIC_JSON_RESOLUTION));
        Metadata expectedMetadata = Metadata.of(expectedError);
        List<Object> expected = TupleUtils.makeTuple(Clip.of(expectedMetadata).asJSON(), "").getValues();
        List<Object> actual = collector.getNthTupleEmittedTo(JoinBolt.JOIN_STREAM, 1).get();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testErrorBuffering() {
        String queryString = "{filters : }";
        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, queryString);
        bolt.execute(query);

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        for (int i = 0; i < JoinBolt.DEFAULT_ERROR_TICKOUT - 1; ++i) {
            bolt.execute(tick);
        }

        Assert.assertEquals(collector.getAllEmitted().count(), 0);
        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        Assert.assertEquals(collector.getAllEmitted().count(), 1);

        Error expectedError = Error.of(Error.GENERIC_JSON_ERROR + ":\n" + queryString + "\n" +
                                       "MalformedJsonException: Expected value at line 1 column 12 path $.filters",
                                       singletonList(Error.GENERIC_JSON_RESOLUTION));
        Metadata expectedMetadata = Metadata.of(expectedError);
        List<Object> expected = TupleUtils.makeTuple(Clip.of(expectedMetadata).asJSON(), "").getValues();
        List<Object> actual = collector.getNthTupleEmittedTo(JoinBolt.JOIN_STREAM, 1).get();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testErrorBufferingTimeout() {
        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "garbage");
        bolt.execute(query);

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        for (int i = 0; i < JoinBolt.DEFAULT_ERROR_TICKOUT; ++i) {
            bolt.execute(tick);
        }

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        Assert.assertEquals(collector.getAllEmitted().count(), 0);
    }

    @Test
    public void testErrorBufferingCustomNoTimeout() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.JOIN_BOLT_ERROR_TICK_TIMEOUT, 10);
        setup(config, new JoinBolt());

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "garbage");
        bolt.execute(query);

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        for (int i = 0; i < 9; ++i) {
            bolt.execute(tick);
        }

        Assert.assertEquals(collector.getAllEmitted().count(), 0);

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        Assert.assertEquals(collector.getAllEmitted().count(), 1);
        Error expectedError = Error.of(Error.GENERIC_JSON_ERROR + ":\ngarbage\n" +
                                        "IllegalStateException: Expected BEGIN_OBJECT but was STRING at line 1 column 1 path $",
                                        singletonList(Error.GENERIC_JSON_RESOLUTION));
        Metadata expectedMetadata = Metadata.of(expectedError);
        List<Object> expected = TupleUtils.makeTuple(Clip.of(expectedMetadata).asJSON(), "").getValues();
        List<Object> actual = collector.getNthTupleEmittedTo(JoinBolt.JOIN_STREAM, 1).get();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testErrorBufferingCustomTimeout() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.JOIN_BOLT_ERROR_TICK_TIMEOUT, 10);
        setup(config, new JoinBolt());

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "garbage");
        bolt.execute(query);

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        for (int i = 0; i < 10; ++i) {
            bolt.execute(tick);
        }

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        Assert.assertEquals(collector.getAllEmitted().count(), 0);
    }

    @Test
    public void testQueryIdentifierMetadata() {
        Map<String, Object> config = new HashMap<>();
        enableMetadataInConfig(config, Concept.QUERY_ID.getName(), "id");
        setup(config, new JoinBolt());

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "{}");
        bolt.execute(query);

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, 42L);

        Metadata meta = new Metadata();
        meta.add("id", 42);
        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(sent).add(meta).asJSON(), "");
        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testUnknownConceptMetadata() {
        Map<String, Object> config = new HashMap<>();
        enableMetadataInConfig(config, Concept.QUERY_ID.getName(), "id");
        enableMetadataInConfig(config, "foo", "bar");
        setup(config, new JoinBolt());

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "{}");
        bolt.execute(query);

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, 42L);

        Metadata meta = new Metadata();
        meta.add("id", 42);
        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(sent).add(meta).asJSON(), "");
        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testMultipleMetadata() {
        Map<String, Object> config = new HashMap<>();
        enableMetadataInConfig(config, Concept.QUERY_ID.getName(), "id");
        enableMetadataInConfig(config, Concept.QUERY_BODY.getName(), "query");
        enableMetadataInConfig(config, Concept.CREATION_TIME.getName(), "created");
        enableMetadataInConfig(config, Concept.TERMINATION_TIME.getName(), "finished");
        setup(config, new JoinBolt());

        long startTime = System.currentTimeMillis();

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "{}");
        bolt.execute(query);

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        sendRawRecordTuplesTo(bolt, 42L);

        long endTime = System.currentTimeMillis();

        Assert.assertEquals(collector.getAllEmitted().count(), 1);

        String response = (String) collector.getTuplesEmitted().findFirst().get().get(0);
        JsonParser parser = new JsonParser();
        JsonObject object = parser.parse(response).getAsJsonObject();

        String records = object.get(Clip.RECORDS_KEY).toString();
        assertJSONEquals(records, "[{\"field\":\"0\"}]");

        JsonObject meta = object.get(Clip.META_KEY).getAsJsonObject();
        long actualID = meta.get("id").getAsLong();
        String queryBody = meta.get("query").getAsString();
        long createdTime = meta.get("created").getAsLong();
        long finishedTime = meta.get("finished").getAsLong();

        Assert.assertEquals(actualID, 42L);
        Assert.assertEquals(queryBody, "{}");
        Assert.assertTrue(createdTime <= finishedTime);
        Assert.assertTrue(createdTime >= startTime && createdTime <= endTime);
    }

    @Test
    public void testUnsupportedAggregation() {
        // "TOP_K" aggregation type not currently supported - error should be emitted
        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                             makeAggregationQuery(TOP_K, 5));
        bolt.execute(query);
        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);
        Assert.assertEquals(collector.getAllEmitted().count(), 1);
    }

    @Test
    public void testUnknownAggregation() {
        // Lowercase "top" is not valid and will not be parsed since there is no enum for it
        // In this case aggregation type should be set to null and an error should be emitted
        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "{\"aggregation\": {\"type\": \"garbage\"}}");
        bolt.execute(query);
        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        Assert.assertEquals(collector.getAllEmitted().count(), 1);
        Error expectedError = Aggregation.TYPE_NOT_SUPPORTED_ERROR;
        Metadata expectedMetadata = Metadata.of(expectedError);
        List<Object> expected = TupleUtils.makeTuple(Clip.of(expectedMetadata).asJSON(), "").getValues();
        List<Object> actual = collector.getNthTupleEmittedTo(JoinBolt.JOIN_STREAM, 1).get();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testUnhandledExceptionErrorEmitted() {
        // An empty query should throw an null-pointer exception which should be caught in JoinBolt
        // and an error should be emitted
        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "");
        bolt.execute(query);
        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        sendRawRecordTuplesTo(bolt, 42L);

        Assert.assertEquals(collector.getAllEmitted().count(), 1);
        Error expectedError = Error.of(Error.GENERIC_JSON_ERROR + ":\n\nNullPointerException: ",
                                       singletonList(Error.GENERIC_JSON_RESOLUTION));
        Metadata expectedMetadata = Metadata.of(expectedError);
        List<Object> expected = TupleUtils.makeTuple(Clip.of(expectedMetadata).asJSON(), "").getValues();
        List<Object> actual = collector.getNthTupleEmittedTo(JoinBolt.JOIN_STREAM, 1).get();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testCounting() {
        bolt = ComponentUtils.prepare(new ExpiringJoinBolt(), collector);

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                             makeGroupFilterQuery("timestamp", asList("1", "2"), EQUALS, GROUP, 1,
                                                                  singletonList(new GroupOperation(COUNT, null, "cnt"))));
        bolt.execute(query);

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        // Send 5 GroupData with counts 1, 2, 3, 4, 5 to the JoinBolt
        IntStream.range(1, 6)
                 .forEach(i -> sendRawByteTuplesTo(bolt, 42L, singletonList(getGroupDataWithCount("cnt", i))));

        // 1 + 2 + 3 + 4 + 5
        List<BulletRecord> result = singletonList(RecordBox.get().add("cnt", 15L).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(result).asJSON(), "");


        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
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
        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, makeAggregationQuery(RAW, 5));
        bolt.execute(query);

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        // This will send 2 batches of 3 records each (total of 6 records).
        List<BulletRecord> sent = sendRawRecordTuplesTo(bolt, 42L, 5, 3);

        List<BulletRecord> actualSent = sent.subList(0, 5);

        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(actualSent).asJSON(), "");
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

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                             makeAggregationQuery(COUNT_DISTINCT, 1, null, Pair.of("field", "field")));
        bolt.execute(query);
        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        sendRawByteTuplesTo(bolt, 42L, asList(first, second));

        List<BulletRecord> result = singletonList(RecordBox.get().add(CountDistinct.DEFAULT_NEW_NAME, 256.0).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(result).asJSON(), "");

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
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
        String queryString = makeGroupFilterQuery("ts", singletonList("1"), EQUALS, GROUP,
                entries, operations, Pair.of("fieldA", "A"));

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, queryString);
        bolt.execute(query);

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        sendRawByteTuplesTo(bolt, 42L, asList(first, second));

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
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
        config.put(BulletConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        setup(config, new ExpiringJoinBolt());

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                             makeGroupFilterQuery("timestamp", asList("1", "2"), EQUALS, GROUP, 1,
                                                                  singletonList(new GroupOperation(COUNT, null, "cnt"))));

        Assert.assertEquals(context.getCountForMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.IMPROPER_QUERIES), Long.valueOf(0));

        bolt.execute(query);
        Assert.assertEquals(context.getCountForMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");
        bolt.execute(returnInfo);

        sendRawByteTuplesTo(bolt, 42L, singletonList(getGroupDataWithCount("cnt", 21)));
        sendRawByteTuplesTo(bolt, 42L, singletonList(getGroupDataWithCount("cnt", 21)));

        List<BulletRecord> result = singletonList(RecordBox.get().add("cnt", 42L).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(result).asJSON(), "");


        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);

        // Should cause an expiry and starts buffering the query for the query tickout
        bolt.execute(tick);
        // We need to tick the default query tickout to make the query emit
        for (int i = 0; i < JoinBolt.DEFAULT_QUERY_TICKOUT - 1; ++i) {
            bolt.execute(tick);
        }
        Assert.assertFalse(collector.wasTupleEmitted(expected));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));

        bolt.execute(tick);
        Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));

        Assert.assertTrue(collector.wasNthEmitted(expected, 1));
        Assert.assertEquals(collector.getAllEmitted().count(), 1);

        bolt.execute(query);
        Assert.assertEquals(context.getCountForMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(2));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.IMPROPER_QUERIES), Long.valueOf(0));
    }

    @Test
    public void testImproperQueryCountingMetrics() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        setup(config, new JoinBolt());

        Assert.assertEquals(context.getCountForMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.IMPROPER_QUERIES), Long.valueOf(0));

        Tuple badQuery = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "");
        bolt.execute(badQuery);
        bolt.execute(badQuery);
        Assert.assertEquals(context.getCountForMetric(JoinBolt.CREATED_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getCountForMetric(JoinBolt.IMPROPER_QUERIES), Long.valueOf(2));
    }

    @Test
    public void testCustomMetricEmitInterval() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Number> mapping = new HashMap<>();
        config.put(BulletConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);

        mapping.put(JoinBolt.ACTIVE_QUERIES, 1);
        mapping.put(JoinBolt.DEFAULT_METRICS_INTERVAL_KEY, 10);
        config.put(BulletConfig.TOPOLOGY_METRICS_BUILT_IN_EMIT_INTERVAL_MAPPING, mapping);
        setup(config, new ExpiringJoinBolt());

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                             makeGroupFilterQuery("timestamp", asList("1", "2"), EQUALS, GROUP, 1,
                                                     singletonList(new GroupOperation(COUNT, null, "cnt"))));

        Assert.assertEquals(context.getCountForMetric(10, JoinBolt.CREATED_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getCountForMetric(1, JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getCountForMetric(10, JoinBolt.IMPROPER_QUERIES), Long.valueOf(0));

        bolt.execute(query);
        Assert.assertEquals(context.getCountForMetric(10, JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getCountForMetric(1, JoinBolt.ACTIVE_QUERIES), Long.valueOf(1));

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);
        for (int i = 0; i <= JoinBolt.DEFAULT_QUERY_TICKOUT; ++i) {
            bolt.execute(tick);
        }

        List<BulletRecord> result = singletonList(RecordBox.get().add("cnt", 42L).getRecord());
        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(result).asJSON(), "");
        Assert.assertFalse(collector.wasTupleEmitted(expected));

        Assert.assertEquals(context.getCountForMetric(10, JoinBolt.CREATED_QUERIES), Long.valueOf(1));
        Assert.assertEquals(context.getCountForMetric(1, JoinBolt.ACTIVE_QUERIES), Long.valueOf(0));
        Assert.assertEquals(context.getCountForMetric(10, JoinBolt.IMPROPER_QUERIES), Long.valueOf(0));
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

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                             makeAggregationQuery(DISTRIBUTION, 10, DistributionType.PMF, "field",
                                                                  null, null, null, null, 3));
        bolt.execute(query);

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");

        bolt.execute(returnInfo);

        sendRawByteTuplesTo(bolt, 42L, asList(first, second));

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
        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(results).asJSON(), "");

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
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

        Tuple query = TupleUtils.makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                             makeAggregationQuery(TOP_K, 2, 5L, "cnt", Pair.of("A", ""), Pair.of("B", "foo")));
        bolt.execute(query);

        Tuple returnInfo = TupleUtils.makeIDTuple(TupleType.Type.RETURN_TUPLE, 42L, "");

        bolt.execute(returnInfo);

        sendRawByteTuplesTo(bolt, 42L, asList(first, second));

        BulletRecord expectedA = RecordBox.get().add("A", "0").add("foo", "null").add("cnt", 8L).getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "1").add("foo", "null").add("cnt", 8L).getRecord();

        List<BulletRecord> results = asList(expectedA, expectedB);
        Tuple expected = TupleUtils.makeTuple(TupleType.Type.JOIN_TUPLE, Clip.of(results).asJSON(), "");

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
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
