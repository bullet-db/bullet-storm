/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.yahoo.bullet.aggregations.CountDistinct;
import com.yahoo.bullet.aggregations.CountDistinctTest;
import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.aggregations.DistributionTest;
import com.yahoo.bullet.aggregations.TopK;
import com.yahoo.bullet.aggregations.TopKTest;
import com.yahoo.bullet.aggregations.grouping.GroupData;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.querying.Querier;
import com.yahoo.bullet.querying.RateLimitError;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.storm.testing.ComponentUtils;
import com.yahoo.bullet.storm.testing.CustomCollector;
import com.yahoo.bullet.storm.testing.CustomOutputFieldsDeclarer;
import com.yahoo.bullet.storm.testing.CustomTopologyContext;
import com.yahoo.bullet.storm.testing.TestHelpers;
import com.yahoo.bullet.storm.testing.TupleUtils;
import com.yahoo.bullet.windowing.SlidingRecord;
import com.yahoo.sketches.frequencies.ErrorType;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
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
import static com.yahoo.bullet.parsing.Clause.Operation.AND;
import static com.yahoo.bullet.parsing.Clause.Operation.EQUALS;
import static com.yahoo.bullet.parsing.Clause.Operation.GREATER_THAN;
import static com.yahoo.bullet.parsing.Clause.Operation.NOT_EQUALS;
import static com.yahoo.bullet.parsing.Clause.Operation.OR;
import static com.yahoo.bullet.parsing.FilterUtils.getFieldFilter;
import static com.yahoo.bullet.parsing.FilterUtils.makeClause;
import static com.yahoo.bullet.parsing.QueryUtils.makeAggregationQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeFieldFilterQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeFilterQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeGroupFilterQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeProjectionFilterQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeProjectionQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeSimpleAggregationFilterQuery;
import static com.yahoo.bullet.storm.testing.TupleUtils.makeIDTuple;
import static com.yahoo.bullet.storm.testing.TupleUtils.makeRawTuple;
import static com.yahoo.bullet.storm.testing.TupleUtils.makeTuple;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.AdditionalAnswers.returnsElementsOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class FilterBoltTest {
    private CustomCollector collector;
    private FilterBolt bolt;
    private BulletStormConfig config;
    private static final Metadata METADATA = new Metadata();
    private static BulletRecordProvider bulletRecordProvider = new BulletStormConfig().getBulletRecordProvider();

    private static class NoQueryFilterBolt extends FilterBolt {
        NoQueryFilterBolt() {
            super(TopologyConstants.RECORD_COMPONENT, new BulletStormConfig());
        }

        @Override
        protected Querier createQuerier(Querier.Mode mode, String id, String query, BulletConfig config) {
            return null;
        }
    }

    // Spies calls to isDone and finishes (returns true) after a fixed number
    private static class DonableFilterBolt extends FilterBolt {
        private int doneAfter = 2;

        DonableFilterBolt() {
            // One record by default and done after 2nd tick
            this(1, 2, new BulletStormConfig());
        }

        DonableFilterBolt(int recordsConsumed, BulletStormConfig config) {
            this(recordsConsumed, 2, config);
        }

        private DonableFilterBolt(int recordsConsumed, int ticksConsumed, BulletStormConfig config) {
            super(TopologyConstants.RECORD_COMPONENT, config);
            // Last tick will need to make it done so subtract 1
            // FilterBolt calls isDone() and consume in Querier calls isDone(), so double it
            doneAfter = 2 * recordsConsumed + ticksConsumed - 1;
        }

        @Override
        protected Querier createQuerier(Querier.Mode mode, String id, String query, BulletConfig config) {
            Querier spied = spy(super.createQuerier(mode, id, query, config));
            List<Boolean> answers = IntStream.range(0, doneAfter).mapToObj(i -> false)
                                             .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
            answers.add(true);
            doAnswer(returnsElementsOf(answers)).when(spied).isDone();
            return spied;
        }
    }

    private static class RateLimitedFilterBolt extends FilterBolt {
        private final int limitedAfter;
        private final RateLimitError error;

        private RateLimitedFilterBolt(int recordsConsumed, RateLimitError error, BulletStormConfig config) {
            super(TopologyConstants.RECORD_COMPONENT, config);
            limitedAfter = recordsConsumed;
            this.error = error;
        }

        @Override
        protected Querier createQuerier(Querier.Mode mode, String id, String query, BulletConfig config) {
            Querier spied = spy(super.createQuerier(mode, id, query, config));
            List<Boolean> answers = IntStream.range(0, limitedAfter).mapToObj(i -> false)
                                             .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
            answers.add(true);
            doAnswer(returnsElementsOf(answers)).when(spied).isExceedingRateLimit();
            doReturn(error).when(spied).getRateLimitError();
            return spied;
        }
    }

    // Helper methods

    private static Tuple makeRecordTuple(BulletRecord record) {
        return makeRawTuple(TopologyConstants.RECORD_COMPONENT, Utils.DEFAULT_STREAM_ID, record);
    }

    private static Tuple makeRecordTuple(BulletRecord record, long timestamp) {
        return makeRawTuple(TopologyConstants.RECORD_COMPONENT, Utils.DEFAULT_STREAM_ID, record, timestamp);
    }

    private static Tuple makeDataTuple(TupleClassifier.Type type, String id, BulletRecord... records) {
        byte[] listBytes = TestHelpers.getListBytes(records);
        return makeTuple(type, id, listBytes);
    }

    private static Tuple makeSlidingTuple(TupleClassifier.Type type, String id, BulletRecord... records) {
        byte[] listBytes = TestHelpers.getListBytes(records);
        byte[] dataBytes = SerializerDeserializer.toBytes(new SlidingRecord.Data(records.length, listBytes));
        return makeTuple(type, id, dataBytes);
    }

    private boolean isSameTuple(List<Object> actual, List<Object> expected) {
        boolean result;
        result = actual.size() == 2;
        result &= actual.size() == expected.size();
        result &= actual.get(0).equals(expected.get(0));
        return result;
    }

    private boolean tupleEquals(List<Object> actual, Tuple expectedTuple) {
        List<Object> expected = expectedTuple.getValues();
        boolean result = isSameTuple(actual, expected);

        byte[] actualRecordList = (byte[]) actual.get(1);
        byte[] expectedRecordList = (byte[]) expected.get(1);
        return result && Arrays.equals(actualRecordList, expectedRecordList);
    }

    private boolean wasRawRecordEmittedTo(String stream, int times, Tuple expectedTuple) {
        return collector.getTuplesEmittedTo(stream).filter(t -> tupleEquals(t, expectedTuple)).count() == times;
    }

    private boolean wasRawRecordEmittedTo(String stream, Tuple expectedTuple) {
        return collector.getAllEmittedTo(stream).anyMatch(t -> tupleEquals(t.getTuple(), expectedTuple));
    }

    private boolean wasRawRecordEmitted(Tuple expectedTuple) {
        return collector.getTuplesEmitted().anyMatch(t -> tupleEquals(t, expectedTuple));
    }

    private byte[] getRawPayloadOfNthTuple(int tupleN) {
        // Position 1 is the raw data
        Optional<Object> data = collector.getMthElementFromNthTupleEmittedTo(TopologyConstants.DATA_STREAM, tupleN, 1);
        return (byte[]) data.orElse(null);
    }

    private boolean isEqual(GroupData actual, BulletRecord expected) {
        return actual.getMetricsAsBulletRecord(bulletRecordProvider).equals(expected);
    }

    private static BulletStormConfig oneRecordConfig() {
        BulletStormConfig config = new BulletStormConfig();
        // Set aggregation default size to 1 since most queries here are RAW with filtering and projections. This
        // makes them isClosedForPartition even if they are not done. immediately.
        config.set(BulletStormConfig.AGGREGATION_DEFAULT_SIZE, 1);
        config.validate();
        return config;
    }

    @BeforeMethod
    public void setup() {
        collector = new CustomCollector();
        config = oneRecordConfig();
        bolt = ComponentUtils.prepare(new FilterBolt(TopologyConstants.RECORD_COMPONENT, config), collector);
    }

    @Test
    public void testOutputFields() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        bolt.declareOutputFields(declarer);
        Fields expected = new Fields(TopologyConstants.ID_FIELD, TopologyConstants.DATA_FIELD);
        Assert.assertTrue(declarer.areFieldsPresent(TopologyConstants.DATA_STREAM, false, expected));
    }

    @Test
    public void testUnknownTuple() {
        Tuple query = TupleUtils.makeTuple(TupleClassifier.Type.RESULT_TUPLE, "", "");
        bolt.execute(query);
        Assert.assertFalse(collector.wasAcked(query));
    }

    @Test
    public void testProjection() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeProjectionQuery(Pair.of("field", "id"), Pair.of("map_field.id", "mid")),
                                  METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").add("timestamp", 92L)
                                             .addMap("map_field", Pair.of("id", "123"), Pair.of("bar", "foo"))
                                             .getRecord();

        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);

        BulletRecord expectedRecord = RecordBox.get().add("id", "b235gf23b").add("mid", "123").getRecord();
        Tuple expected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", expectedRecord);
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 1, expected));
    }

    @Test
    public void testBadJson() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "'filters' : [], ", METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);

        Tuple expected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", record);
        Assert.assertTrue(collector.wasAcked(query));
        Assert.assertTrue(collector.wasAcked(matching));
        Assert.assertFalse(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, expected));
    }

    @Test
    public void testFiltering() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeFieldFilterQuery("b235gf23b"), METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);

        BulletRecord anotherRecord = RecordBox.get().add("field", "wontmatch").getRecord();
        Tuple nonMatching = makeRecordTuple(anotherRecord);
        bolt.execute(nonMatching);

        Tuple expected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", record);
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 1, expected));

        Tuple anotherExpected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", anotherRecord);
        Assert.assertFalse(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, anotherExpected));
    }

    @Test
    public void testProjectionAndFiltering() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeProjectionFilterQuery("map_field.id", singletonList("123"), EQUALS,
                                                            Pair.of("field", "id"), Pair.of("map_field.id", "mid")),
                                  METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").add("timestamp", 92L)
                                             .addMap("map_field", Pair.of("id", "123"), Pair.of("bar", "foo"))
                                             .getRecord();

        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);

        BulletRecord expectedRecord = RecordBox.get().add("id", "b235gf23b").add("mid", "123").getRecord();
        Tuple expected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", expectedRecord);
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 1, expected));
    }

    @Test
    public void testFilteringUsingProjectedName() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeProjectionFilterQuery("mid", singletonList("123"), EQUALS,
                                                             Pair.of("field", "id"), Pair.of("map_field.id", "mid")),
                                  METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").add("timestamp", 92L)
                                             .addMap("map_field", Pair.of("id", "123"), Pair.of("bar", "foo"))
                                             .getRecord();

        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);

        BulletRecord expectedRecord = RecordBox.get().add("id", "b235gf23b").add("mid", "123").getRecord();
        Tuple expected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", expectedRecord);
        Assert.assertFalse(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, expected));
    }

    @Test
    public void testProjectionNotLosingFilterColumn() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeProjectionFilterQuery("timestamp", singletonList("92"), EQUALS,
                                                            Pair.of("field", "id"), Pair.of("map_field.id", "mid")),
                                  METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").add("timestamp", 92L)
                                             .addMap("map_field", Pair.of("id", "123"), Pair.of("bar", "foo"))
                                             .getRecord();

        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);

        BulletRecord expectedRecord = RecordBox.get().add("id", "b235gf23b").add("mid", "123").getRecord();
        Tuple expected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", expectedRecord);
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, expected));
    }

    @Test
    public void testFilteringSlidingWindow() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeSimpleAggregationFilterQuery("field", singletonList("b235gf23b"), EQUALS, RAW, 5,
                                                                   Window.Unit.RECORD, 1, Window.Unit.RECORD, 1),
                                  METADATA);
        bolt.execute(query);
        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeRecordTuple(record);

        bolt.execute(matching);
        bolt.execute(matching);
        bolt.execute(matching);
        bolt.execute(matching);

        Tuple expected = makeSlidingTuple(TupleClassifier.Type.DATA_TUPLE, "42", record);
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 4, expected));
    }

    @Test
    public void testDifferentQueryMatchingSameTuple() {
        Tuple queryA = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeFieldFilterQuery("b235gf23b"), METADATA);
        Tuple queryB = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "43",
                                   makeFilterQuery("timestamp", asList("1", "2", "3", "45"), EQUALS), METADATA);
        bolt.execute(queryA);
        bolt.execute(queryB);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").add("timestamp", 45L).getRecord();
        Tuple matching = makeRecordTuple(record);

        bolt.execute(matching);

        Tuple expectedA = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", record);
        Tuple expectedB = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "43", record);

        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 1, expectedA));
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 1, expectedB));
    }

    @Test
    public void testDifferentQueryMatchingDifferentTuple() {
        Tuple queryA = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeFieldFilterQuery("b235gf23b"), METADATA);
        Tuple queryB = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "43",
                                   makeFilterQuery("timestamp", asList("1", "2", "3", "45"), NOT_EQUALS), METADATA);
        bolt.execute(queryA);
        bolt.execute(queryB);

        BulletRecord recordA = RecordBox.get().add("field", "b235gf23b").add("timestamp", 45L).getRecord();
        BulletRecord recordB = RecordBox.get().add("field", "b235gf23b").add("timestamp", 42L).getRecord();
        Tuple matchingA = makeRecordTuple(recordA);
        Tuple matchingB = makeRecordTuple(recordB);

        bolt.execute(matchingA);
        bolt.execute(matchingB);

        Tuple expectedAA = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", recordA);
        Tuple expectedAB = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", recordB);
        Tuple expectedB = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "43", recordB);

        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 1, expectedAA));
        Assert.assertFalse(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, expectedAB));
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 1, expectedB));
    }

    @Test
    public void testFailQueryInitialization() {
        bolt = ComponentUtils.prepare(new NoQueryFilterBolt(), collector);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeFieldFilterQuery("b235gf23b"), METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);

        Tuple expected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", record);
        Assert.assertFalse(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, expected));
    }

    @Test
    public void testQueryNotDone() {
        bolt = ComponentUtils.prepare(new DonableFilterBolt(), collector);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeFieldFilterQuery("b235gf23b"), METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        Tuple expected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", record);
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 1, expected));
    }

    @Test
    public void testQueryDone() {
        bolt = ComponentUtils.prepare(new DonableFilterBolt(), collector);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeFieldFilterQuery("b235gf23b"), METADATA);
        bolt.execute(query);

        BulletRecord nonMatching = RecordBox.get().add("field", "foo").getRecord();
        Tuple notMatching = makeRecordTuple(nonMatching);
        bolt.execute(notMatching);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);

        Tuple expected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", record);
        Assert.assertFalse(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, expected));
    }

    @Test
    public void testQueryNotDoneAndThenDone() {
        bolt = ComponentUtils.prepare(new DonableFilterBolt(), collector);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeFieldFilterQuery("b235gf23b"), METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        Tuple expected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", record);
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 1, expected));

        BulletRecord anotherRecord = RecordBox.get().add("field", "b235gf23b").add("mid", "2342").getRecord();
        Tuple anotherExpected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", anotherRecord);
        Assert.assertFalse(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, anotherExpected));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testComplexFilterQuery() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeFilterQuery(OR,
                                                  makeClause(AND,
                                                             getFieldFilter("field", EQUALS, "abc"),
                                                             makeClause(OR,
                                                                        makeClause(AND,
                                                                                   getFieldFilter("experience", EQUALS, "app", "tv"),
                                                                                   getFieldFilter("pid", EQUALS, "1", "2")),
                                                                        getFieldFilter("mid", GREATER_THAN, "10"))),
                                                  makeClause(AND,
                                                             getFieldFilter("demographic_map.age", GREATER_THAN, "65"),
                                                             getFieldFilter("filter_map.is_fake_event", EQUALS, "true"))),
                                  METADATA);
        bolt.execute(query);

        // first clause is true : field == "abc", experience == "app" or "tv", mid < 10
        BulletRecord recordA = RecordBox.get().add("field", "abc")
                                              .add("experience", "tv")
                                              .add("mid", 11)
                                              .getRecord();
        // second clause is false: age > 65 and is_fake_event == null
        BulletRecord recordB = RecordBox.get().addMap("demographic_map", Pair.of("age", "67")).getRecord();

        Tuple nonMatching = makeRecordTuple(recordB);
        bolt.execute(nonMatching);
        bolt.execute(nonMatching);

        Tuple matching = makeRecordTuple(recordA);
        bolt.execute(matching);

        BulletRecord expectedRecord = RecordBox.get().add("field", "abc").add("experience", "tv")
                                                     .add("mid", 11).getRecord();
        BulletRecord notExpectedRecord = RecordBox.get().addMap("demographic_map", Pair.of("age", "67")).getRecord();

        Tuple expected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", expectedRecord);
        Tuple notExpected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", notExpectedRecord);

        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 1, expected));
        Assert.assertFalse(wasRawRecordEmitted(notExpected));
    }

    @Test
    public void testTuplesCustomSource() {
        bolt = ComponentUtils.prepare(new FilterBolt("CustomSource", oneRecordConfig()), collector);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeFieldFilterQuery("b235gf23b"), METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = TupleUtils.makeRawTuple("CustomSource", TopologyConstants.RECORD_STREAM, record);
        bolt.execute(matching);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);

        BulletRecord anotherRecord = RecordBox.get().add("field", "wontmatch").getRecord();
        Tuple nonMatching = TupleUtils.makeRawTuple("CustomSource", TopologyConstants.RECORD_STREAM, anotherRecord);
        bolt.execute(nonMatching);

        Tuple expected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", record);
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 1, expected));

        Tuple notExpected = makeDataTuple(TupleClassifier.Type.DATA_TUPLE, "42", anotherRecord);
        Assert.assertFalse(wasRawRecordEmitted(notExpected));

    }

    @Test
    public void testGroupAllCount() {
        // 15 Records will be consumed
        bolt = ComponentUtils.prepare(new DonableFilterBolt(15, new BulletStormConfig()), collector);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeGroupFilterQuery("timestamp", asList("1", "2"), EQUALS,
                                                       GROUP, 1, singletonList(new GroupOperation(COUNT, null, "cnt"))),
                                  METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("timestamp", "1").getRecord();
        Tuple matching = makeRecordTuple(record);
        IntStream.range(0, 10).forEach(i -> bolt.execute(matching));

        BulletRecord another = RecordBox.get().getRecord();

        Tuple nonMatching = makeRecordTuple(another);
        IntStream.range(0, 5).forEach(i -> bolt.execute(nonMatching));
        bolt.execute(nonMatching);

        // Two to flush bolt
        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        Assert.assertEquals(collector.getEmittedCount(), 1);
        GroupData actual = SerializerDeserializer.fromBytes(getRawPayloadOfNthTuple(1));
        BulletRecord expected = RecordBox.get().add("cnt", 10L).getRecord();

        Assert.assertTrue(isEqual(actual, expected));
    }

    @Test
    public void testCountDistinct() {
        // 256 Records will be consumed
        BulletStormConfig config = new BulletStormConfig(CountDistinctTest.makeConfiguration(8, 512));
        bolt = ComponentUtils.prepare(new DonableFilterBolt(256, config), collector);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeAggregationQuery(COUNT_DISTINCT, 1, null, Pair.of("field", "field")),
                                  METADATA);
        bolt.execute(query);

        IntStream.range(0, 256).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                               .map(FilterBoltTest::makeRecordTuple)
                               .forEach(bolt::execute);

        Assert.assertEquals(collector.getEmittedCount(), 0);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        Assert.assertEquals(collector.getEmittedCount(), 1);

        byte[] rawData = getRawPayloadOfNthTuple(1);
        Assert.assertNotNull(rawData);

        CountDistinct distinct = CountDistinctTest.makeCountDistinct(config, singletonList("field"));
        distinct.combine(rawData);

        BulletRecord actual = distinct.getRecords().get(0);
        BulletRecord expected = RecordBox.get().add(CountDistinct.DEFAULT_NEW_NAME, 256.0).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testNoConsumptionAfterDone() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeSimpleAggregationFilterQuery("field", singletonList("b235gf23b"), EQUALS, RAW, 5,
                                                                   Window.Unit.RECORD, 1, Window.Unit.RECORD, 1),
                                  METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);
        bolt.execute(matching);
        bolt.execute(matching);

        Tuple expected = makeSlidingTuple(TupleClassifier.Type.DATA_TUPLE, "42", record);
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 3, expected));

        collector = new CustomCollector();
        // Will be done after 2 consumes (no ticks)
        bolt = ComponentUtils.prepare(new DonableFilterBolt(2, 1, new BulletStormConfig()), collector);
        bolt.execute(query);
        bolt.execute(matching);
        bolt.execute(matching);
        // Now the query should be done, so it should not consume
        bolt.execute(matching);

        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 2, expected));
    }

    @Test
    public void testDistribution() {
        // 100 Records will be consumed
        BulletStormConfig config = new BulletStormConfig(DistributionTest.makeConfiguration(20, 128));
        bolt = ComponentUtils.prepare(new DonableFilterBolt(101, config), collector);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeAggregationQuery(DISTRIBUTION, 10, Distribution.Type.PMF, "field", null, null,
                                                       null, null, 3),
                                  METADATA);
        bolt.execute(query);

        IntStream.range(0, 101).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                               .map(FilterBoltTest::makeRecordTuple)
                               .forEach(bolt::execute);

        Assert.assertEquals(collector.getEmittedCount(), 0);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        Assert.assertEquals(collector.getEmittedCount(), 1);

        byte[] rawData = getRawPayloadOfNthTuple(1);
        Assert.assertNotNull(rawData);

        Distribution distribution = DistributionTest.makeDistribution(config, makeAttributes(Distribution.Type.PMF, 3),
                                                                      "field", 10, null);
        distribution.combine(rawData);

        List<BulletRecord> records = distribution.getRecords();

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
        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);
        Assert.assertEquals(records.get(2), expectedC);
        Assert.assertEquals(records.get(3), expectedD);
    }

    @Test
    public void testTopK() {
        // 16 records
        BulletStormConfig config = new BulletStormConfig(TopKTest.makeConfiguration(ErrorType.NO_FALSE_NEGATIVES, 32));
        bolt = ComponentUtils.prepare(new DonableFilterBolt(16, config), collector);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeAggregationQuery(TOP_K, 5, null, "cnt", Pair.of("A", ""), Pair.of("B", "foo")),
                                  METADATA);
        bolt.execute(query);

        IntStream.range(0, 8).mapToObj(i -> RecordBox.get().add("A", i).getRecord())
                             .map(FilterBoltTest::makeRecordTuple)
                             .forEach(bolt::execute);
        IntStream.range(0, 6).mapToObj(i -> RecordBox.get().add("A", 0).getRecord())
                             .map(FilterBoltTest::makeRecordTuple)
                             .forEach(bolt::execute);
        IntStream.range(0, 2).mapToObj(i -> RecordBox.get().add("A", 3).getRecord())
                             .map(FilterBoltTest::makeRecordTuple)
                             .forEach(bolt::execute);

        Tuple tick = TupleUtils.makeTuple(TupleClassifier.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        Assert.assertEquals(collector.getEmittedCount(), 1);

        byte[] rawData = getRawPayloadOfNthTuple(1);
        Assert.assertNotNull(rawData);

        Map<String, String> fields = new HashMap<>();
        fields.put("A", "");
        fields.put("B", "foo");
        TopK topK = TopKTest.makeTopK(config, makeAttributes("cnt", null), fields, 2, null);
        topK.combine(rawData);

        List<BulletRecord> records = topK.getRecords();
        Assert.assertEquals(records.size(), 2);

        BulletRecord expectedA = RecordBox.get().add("A", "0").add("foo", "null").add("cnt", 7L).getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "3").add("foo", "null").add("cnt", 3L).getRecord();

        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);
    }

    @Test
    public void testFilteringLatency() {
        config = new BulletStormConfig();
        // Don't use the overridden aggregation default size but turn on built in metrics
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        collector = new CustomCollector();
        CustomTopologyContext context = new CustomTopologyContext();
        bolt = new FilterBolt(TopologyConstants.RECORD_COMPONENT, config);
        ComponentUtils.prepare(new HashMap<>(), bolt, context, collector);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", makeFieldFilterQuery("bar"), METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "foo").getRecord();
        long start = System.currentTimeMillis();
        IntStream.range(0, 10).mapToObj(i -> makeRecordTuple(record, System.currentTimeMillis()))
                              .forEach(bolt::execute);
        long end = System.currentTimeMillis();
        double actualLatecy = context.getDoubleMetric(TopologyConstants.LATENCY_METRIC);
        Assert.assertTrue(actualLatecy <= end - start);
    }

    @Test
    public void testRateLimiting() {
        config = new BulletStormConfig();
        RateLimitError rateLimitError = new RateLimitError(42.0, config);
        bolt = new RateLimitedFilterBolt(2, rateLimitError, config);
        bolt = ComponentUtils.prepare(new HashMap<>(), bolt, collector);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeSimpleAggregationFilterQuery("field", singletonList("b235gf23b"), EQUALS, RAW, 100,
                                                                    Window.Unit.RECORD, 1, Window.Unit.RECORD, 1),
                                  METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);
        bolt.execute(matching);

        Tuple expected = makeSlidingTuple(TupleClassifier.Type.DATA_TUPLE, "42", record);
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 2, expected));

        bolt.execute(matching);
        Tuple error = TupleUtils.makeIDTuple(TupleClassifier.Type.ERROR_TUPLE, "42", rateLimitError);
        Assert.assertTrue(collector.wasNthEmitted(error, 3));
    }

    @Test
    public void testMissingRateLimit() {
        config = new BulletStormConfig();
        bolt = new RateLimitedFilterBolt(2, null, config);
        bolt = ComponentUtils.prepare(new HashMap<>(), bolt, collector);

        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeSimpleAggregationFilterQuery("field", singletonList("b235gf23b"), EQUALS, RAW, 100,
                                                                   Window.Unit.RECORD, 1, Window.Unit.RECORD, 1),
                                  METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);
        bolt.execute(matching);

        Tuple expected = makeSlidingTuple(TupleClassifier.Type.DATA_TUPLE, "42", record);
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 2, expected));
        Assert.assertEquals(collector.getEmittedCount(), 2);

        bolt.execute(matching);
        Assert.assertEquals(collector.getEmittedCount(), 2);
    }

    @Test
    public void testKillSignal() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeSimpleAggregationFilterQuery("field", singletonList("b235gf23b"), EQUALS, RAW, 5,
                                                                   Window.Unit.RECORD, 1, Window.Unit.RECORD, 1),
                                  METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);
        bolt.execute(matching);

        Tuple expected = makeSlidingTuple(TupleClassifier.Type.DATA_TUPLE, "42", record);
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 2, expected));
        Assert.assertEquals(collector.getEmittedCount(), 2);

        Tuple kill = makeIDTuple(TupleClassifier.Type.METADATA_TUPLE, "42", new Metadata(Metadata.Signal.KILL, null));
        bolt.execute(kill);

        bolt.execute(matching);
        bolt.execute(matching);

        Assert.assertEquals(collector.getEmittedCount(), 2);
    }

    @Test
    public void testCompleteSignal() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42",
                                  makeSimpleAggregationFilterQuery("field", singletonList("b235gf23b"), EQUALS, RAW, 5,
                                                                   Window.Unit.RECORD, 1, Window.Unit.RECORD, 1),
                                  METADATA);
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeRecordTuple(record);
        bolt.execute(matching);
        bolt.execute(matching);

        Tuple expected = makeSlidingTuple(TupleClassifier.Type.DATA_TUPLE, "42", record);
        Assert.assertTrue(wasRawRecordEmittedTo(TopologyConstants.DATA_STREAM, 2, expected));
        Assert.assertEquals(collector.getEmittedCount(), 2);

        Tuple complete = makeIDTuple(TupleClassifier.Type.METADATA_TUPLE, "42", new Metadata(Metadata.Signal.COMPLETE, null));
        bolt.execute(complete);

        bolt.execute(matching);
        bolt.execute(matching);

        Assert.assertEquals(collector.getEmittedCount(), 2);
    }

    @Test
    public void testQueryErrorsAreSilentlyIgnored() {
        Tuple query = makeIDTuple(TupleClassifier.Type.QUERY_TUPLE, "42", "{'aggregation': { 'type': null }}");
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple someTuple = makeRecordTuple(record);
        bolt.execute(someTuple);
        bolt.execute(someTuple);

        Assert.assertEquals(collector.getEmittedCount(), 0);
    }
}
