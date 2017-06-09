/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.TestHelpers;
import com.yahoo.bullet.operations.AggregationOperations.DistributionType;
import com.yahoo.bullet.operations.SerializerDeserializer;
import com.yahoo.bullet.operations.aggregations.CountDistinct;
import com.yahoo.bullet.operations.aggregations.CountDistinctTest;
import com.yahoo.bullet.operations.aggregations.Distribution;
import com.yahoo.bullet.operations.aggregations.DistributionTest;
import com.yahoo.bullet.operations.aggregations.TopK;
import com.yahoo.bullet.operations.aggregations.TopKTest;
import com.yahoo.bullet.operations.aggregations.grouping.GroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.querying.FilterQuery;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.sketches.frequencies.ErrorType;
import org.apache.commons.lang3.tuple.Pair;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
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

import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.COUNT_DISTINCT;
import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.DISTRIBUTION;
import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.GROUP;
import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.RAW;
import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.TOP_K;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.COUNT;
import static com.yahoo.bullet.operations.FilterOperations.FilterType.AND;
import static com.yahoo.bullet.operations.FilterOperations.FilterType.EQUALS;
import static com.yahoo.bullet.operations.FilterOperations.FilterType.GREATER_THAN;
import static com.yahoo.bullet.operations.FilterOperations.FilterType.NOT_EQUALS;
import static com.yahoo.bullet.operations.FilterOperations.FilterType.OR;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.COUNT_FIELD;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.END_EXCLUSIVE;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.NEGATIVE_INFINITY_START;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.POSITIVE_INFINITY_END;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.PROBABILITY_FIELD;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.RANGE_FIELD;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.SEPARATOR;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.START_INCLUSIVE;
import static com.yahoo.bullet.parsing.AggregationUtils.makeAttributes;
import static com.yahoo.bullet.parsing.LogicalClauseTest.clause;
import static com.yahoo.bullet.parsing.QueryUtils.getFilterQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeAggregationQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeFieldFilterQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeFilterQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeGroupFilterQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeProjectionFilterQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeProjectionQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeSimpleAggregationFilterQuery;
import static com.yahoo.bullet.storm.TupleUtils.makeIDTuple;
import static com.yahoo.bullet.storm.TupleUtils.makeTuple;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.AdditionalAnswers.returnsElementsOf;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class FilterBoltTest {
    private CustomCollector collector;
    private CustomTopologyContext context;
    private FilterBolt bolt;

    private class NoQueryFilterBolt extends FilterBolt {
        @Override
        protected FilterQuery getQuery(Long id, String queryString) {
            return null;
        }
    }

    private class NeverExpiringFilterBolt extends FilterBolt {
        @Override
        protected FilterQuery getQuery(Long id, String queryString) {
            FilterQuery original = super.getQuery(id, queryString);
            if (original != null) {
                original = spy(original);
                when(original.isExpired()).thenReturn(false);
            }
            return original;
        }
    }

    // Spies calls to isExpired and expires (returns true) after a fixed number
    private class ExpiringFilterBolt extends FilterBolt {
        private int expireAfter = 2;

        public ExpiringFilterBolt() {
            // One record by default and expire on 2nd tick
            this(1, 2);
        }

        public ExpiringFilterBolt(int recordsConsumed) {
            this(recordsConsumed, 2);
        }

        public ExpiringFilterBolt(int recordsConsumed, int ticksConsumed) {
            // Last tick will need to expire so subtract 1
            expireAfter = recordsConsumed + ticksConsumed - 1;
        }

        @Override
        protected FilterQuery getQuery(Long id, String queryString) {
            FilterQuery spied = spy(getFilterQuery(queryString, configuration));
            List<Boolean> answers = IntStream.range(0, expireAfter).mapToObj(i -> false)
                                             .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
            answers.add(true);
            when(spied.isExpired()).thenAnswer(returnsElementsOf(answers));
            return spied;
        }
    }

    public static Tuple makeRecordTuple(TupleType.Type type, Long id, BulletRecord... records) {
        byte[] listBytes = TestHelpers.getListBytes(records);
        return makeTuple(type, id, listBytes);
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
        Optional<Object> data = collector.getMthElementFromNthTupleEmittedTo(FilterBolt.FILTER_STREAM, tupleN, 1);
        if (data.isPresent()) {
            return (byte[]) data.get();
        }
        return null;
    }

    private boolean isEqual(GroupData actual, BulletRecord expected) {
        return actual.getMetricsAsBulletRecord().equals(expected);
    }

    @BeforeMethod
    public void setup() {
        collector = new CustomCollector();
        bolt = ComponentUtils.prepare(new NeverExpiringFilterBolt(), collector);
    }

    public void setup(Map<String, Object> config, FilterBolt bolt) {
        collector = new CustomCollector();
        context = new CustomTopologyContext();
        this.bolt = ComponentUtils.prepare(config, bolt, context, collector);
    }

    @Test
    public void testOutputFields() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        bolt.declareOutputFields(declarer);
        Fields expected = new Fields(TopologyConstants.ID_FIELD, TopologyConstants.RECORD_FIELD);
        Assert.assertTrue(declarer.areFieldsPresent(FilterBolt.FILTER_STREAM, false, expected));
    }

    @Test
    public void testUnknownTuple() {
        Tuple query = TupleUtils.makeTuple(TupleType.Type.RETURN_TUPLE, "", "");
        bolt.execute(query);
        Assert.assertFalse(collector.wasAcked(query));
    }

    @Test
    public void testProjection() {
        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                  makeProjectionQuery(Pair.of("field", "id"), Pair.of("map_field.id", "mid")));
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").add("timestamp", 92L)
                                             .addMap("map_field", Pair.of("id", "123"), Pair.of("bar", "foo"))
                                             .getRecord();

        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);

        BulletRecord expectedRecord = RecordBox.get().add("id", "b235gf23b").add("mid", "123").getRecord();
        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, expectedRecord);
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expected));
    }

    @Test
    public void testBadJson() {
        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, "'filters' : [], ");
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);

        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record);
        Assert.assertTrue(collector.wasAcked(query));
        Assert.assertTrue(collector.wasAcked(matching));
        Assert.assertFalse(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, expected));
    }

    @Test
    public void testFiltering() {
        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, makeFieldFilterQuery("b235gf23b"));
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);

        BulletRecord anotherRecord = RecordBox.get().add("field", "wontmatch").getRecord();
        Tuple nonMatching = makeTuple(TupleType.Type.RECORD_TUPLE, anotherRecord);
        bolt.execute(nonMatching);

        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record);
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expected));

        Tuple anotherExpected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, anotherRecord);
        Assert.assertFalse(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, anotherExpected));
    }

    @Test
    public void testProjectionAndFiltering() {
        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                makeProjectionFilterQuery("map_field.id", singletonList("123"), EQUALS,
                        Pair.of("field", "id"), Pair.of("map_field.id", "mid")));
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").add("timestamp", 92L)
                                             .addMap("map_field", Pair.of("id", "123"), Pair.of("bar", "foo"))
                                             .getRecord();

        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);

        BulletRecord expectedRecord = RecordBox.get().add("id", "b235gf23b").add("mid", "123").getRecord();
        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, expectedRecord);
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expected));
    }

    @Test
    public void testFilteringUsingProjectedName() {
        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                  makeProjectionFilterQuery("mid", singletonList("123"), EQUALS,
                                          Pair.of("field", "id"), Pair.of("map_field.id", "mid")));
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").add("timestamp", 92L)
                                             .addMap("map_field", Pair.of("id", "123"), Pair.of("bar", "foo"))
                                             .getRecord();

        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);

        BulletRecord expectedRecord = RecordBox.get().add("id", "b235gf23b").add("mid", "123").getRecord();
        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, expectedRecord);
        Assert.assertFalse(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, expected));
    }

    @Test
    public void testProjectionNotLosingFilterColumn() {
        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                  makeProjectionFilterQuery("timestamp", singletonList("92"), EQUALS,
                                          Pair.of("field", "id"), Pair.of("map_field.id", "mid")));
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").add("timestamp", 92L)
                                             .addMap("map_field", Pair.of("id", "123"), Pair.of("bar", "foo"))
                                             .getRecord();

        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);

        BulletRecord expectedRecord = RecordBox.get().add("id", "b235gf23b").add("mid", "123").getRecord();
        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, expectedRecord);
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, expected));
    }

    @Test
    public void testMultiFiltering() {
        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                makeSimpleAggregationFilterQuery("field", singletonList("b235gf23b"), EQUALS, RAW, 5));
        bolt.execute(query);
        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);

        bolt.execute(matching);
        bolt.execute(matching);
        bolt.execute(matching);
        bolt.execute(matching);

        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record);
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 4, expected));
    }

    @Test
    public void testDifferentQueryMatchingSameTuple() {
        Tuple queryA = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, makeFieldFilterQuery("b235gf23b"));
        Tuple queryB = makeIDTuple(TupleType.Type.QUERY_TUPLE, 43L,
                makeFilterQuery("timestamp", asList("1", "2", "3", "45"), EQUALS));
        bolt.execute(queryA);
        bolt.execute(queryB);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").add("timestamp", 45L).getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);

        bolt.execute(matching);

        Tuple expectedA = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record);
        Tuple expectedB = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 43L, record);

        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expectedA));
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expectedB));
    }

    @Test
    public void testDifferentQueryMatchingDifferentTuple() {
        Tuple queryA = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, makeFieldFilterQuery("b235gf23b"));
        Tuple queryB = makeIDTuple(TupleType.Type.QUERY_TUPLE, 43L,
                makeFilterQuery("timestamp", asList("1", "2", "3", "45"), NOT_EQUALS));
        bolt.execute(queryA);
        bolt.execute(queryB);

        BulletRecord recordA = RecordBox.get().add("field", "b235gf23b").add("timestamp", 45L).getRecord();
        BulletRecord recordB = RecordBox.get().add("field", "b235gf23b").add("timestamp", 42L).getRecord();
        Tuple matchingA = makeTuple(TupleType.Type.RECORD_TUPLE, recordA);
        Tuple matchingB = makeTuple(TupleType.Type.RECORD_TUPLE, recordB);

        bolt.execute(matchingA);
        bolt.execute(matchingB);

        Tuple expectedAA = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, recordA);
        Tuple expectedAB = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, recordB);
        Tuple expectedB = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 43L, recordB);

        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expectedAA));
        Assert.assertFalse(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, expectedAB));
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expectedB));
    }

    @Test
    public void testFailQueryInitialization() {
        bolt = ComponentUtils.prepare(new NoQueryFilterBolt(), collector);

        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, makeFieldFilterQuery("b235gf23b"));
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);

        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record);
        Assert.assertFalse(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, expected));
    }

    @Test
    public void testQueryNonExpiry() {
        bolt = ComponentUtils.prepare(new ExpiringFilterBolt(), collector);

        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, makeFieldFilterQuery("b235gf23b"));
        bolt.execute(query);

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);

        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record);
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expected));
    }

    @Test
    public void testQueryExpiry() {
        bolt = ComponentUtils.prepare(new ExpiringFilterBolt(), collector);

        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, makeFieldFilterQuery("b235gf23b"));
        bolt.execute(query);

        // Two to flush bolt
        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);

        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record);
        Assert.assertFalse(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, expected));
    }

    @Test
    public void testQueryNonExpiryAndThenExpiry() {
        bolt = ComponentUtils.prepare(new ExpiringFilterBolt(), collector);

        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, makeFieldFilterQuery("b235gf23b"));
        bolt.execute(query);

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);

        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record);
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expected));

        bolt.execute(tick);

        BulletRecord anotherRecord = RecordBox.get().add("field", "b235gf23b").add("mid", "2342").getRecord();
        Tuple anotherExpected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, anotherRecord);
        Assert.assertFalse(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, anotherExpected));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testComplexFilterQuery() {
        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                makeFilterQuery(OR,
                        clause(AND,
                                clause("field", EQUALS, "abc"),
                                clause(OR,
                                        clause(AND,
                                                clause("experience", EQUALS, "app", "tv"),
                                                clause("pid", EQUALS, "1", "2")),
                                        clause("mid", GREATER_THAN, "10"))),
                        clause(AND,
                                clause("demographic_map.age", GREATER_THAN, "65"),
                                clause("filter_map.is_fake_event", EQUALS, "true"))));
        bolt.execute(query);

        // first clause is true : field == "abc", experience == "app" or "tv", mid < 10
        BulletRecord recordA = RecordBox.get().add("field", "abc")
                                              .add("experience", "tv")
                                              .add("mid", 11)
                .getRecord();
        // second clause is false: age > 65 and is_fake_event == null
        BulletRecord recordB = RecordBox.get().addMap("demographic_map", Pair.of("age", "67")).getRecord();

        Tuple nonMatching = makeTuple(TupleType.Type.RECORD_TUPLE, recordB);
        bolt.execute(nonMatching);
        bolt.execute(nonMatching);

        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, recordA);
        bolt.execute(matching);

        BulletRecord expectedRecord = RecordBox.get().add("field", "abc").add("experience", "tv")
                                                     .add("mid", 11).getRecord();
        BulletRecord notExpectedRecord = RecordBox.get().addMap("demographic_map", Pair.of("age", "67")).getRecord();

        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, expectedRecord);
        Tuple notExpected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, notExpectedRecord);

        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expected));
        Assert.assertFalse(wasRawRecordEmitted(notExpected));
    }

    @Test
    public void testTuplesCustomSource() {
        FilterBolt bolt = ComponentUtils.prepare(new FilterBolt("CustomSource"), collector);

        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, makeFieldFilterQuery("b235gf23b"));
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = TupleUtils.makeRawTuple("CustomSource", TopologyConstants.RECORD_STREAM, record);
        bolt.execute(matching);

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);

        BulletRecord anotherRecord = RecordBox.get().add("field", "wontmatch").getRecord();
        Tuple nonMatching = TupleUtils.makeRawTuple("CustomSource", TopologyConstants.RECORD_STREAM, anotherRecord);
        bolt.execute(nonMatching);

        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record);
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expected));

        Tuple notExpected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, anotherRecord);
        Assert.assertFalse(wasRawRecordEmitted(notExpected));

    }

    @Test
    public void testGroupAllCount() {
        // 15 Records will be consumed
        bolt = ComponentUtils.prepare(new ExpiringFilterBolt(15), collector);

        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                makeGroupFilterQuery("timestamp", asList("1", "2"), EQUALS,
                        GROUP, 1, singletonList(new GroupOperation(COUNT, null, "cnt"))));
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("timestamp", "1").getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        IntStream.range(0, 10).forEach(i -> bolt.execute(matching));

        BulletRecord another = RecordBox.get().getRecord();

        Tuple nonMatching = makeTuple(TupleType.Type.RECORD_TUPLE, another);
        IntStream.range(0, 5).forEach(i -> bolt.execute(nonMatching));
        bolt.execute(nonMatching);

        // Two to flush bolt
        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        Assert.assertEquals(collector.getEmittedCount(), 1);
        GroupData actual = SerializerDeserializer.fromBytes(getRawPayloadOfNthTuple(1));
        BulletRecord expected = RecordBox.get().add("cnt", 10).getRecord();

        Assert.assertTrue(isEqual(actual, expected));
    }

    @Test
    public void testMicroBatching() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.RAW_AGGREGATION_MICRO_BATCH_SIZE, 3);
        // 5 Records will be consumed
        bolt = ComponentUtils.prepare(config, new ExpiringFilterBolt(5), collector);

        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                  makeSimpleAggregationFilterQuery("field", singletonList("b235gf23b"), EQUALS, RAW, 7));
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);
        bolt.execute(matching);
        bolt.execute(matching);
        bolt.execute(matching);
        bolt.execute(matching);

        // We should have a single micro-batch of size 3
        Assert.assertEquals(collector.getEmittedCount(), 1);

        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record, record, record);
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expected));

        // Two to flush bolt. When the query expires, the last two get emitted.
        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record, record);
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expected));
    }

    @Test
    public void testCountDistinct() {
        Map<Object, Object> config = CountDistinctTest.makeConfiguration(8, 512);
        // 256 Records will be consumed
        bolt = ComponentUtils.prepare(config, new ExpiringFilterBolt(256), collector);

        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                  makeAggregationQuery(COUNT_DISTINCT, 1, null, Pair.of("field", "field")));
        bolt.execute(query);

        IntStream.range(0, 256).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                               .map(r -> makeTuple(TupleType.Type.RECORD_TUPLE, r))
                .forEach(bolt::execute);

        Assert.assertEquals(collector.getEmittedCount(), 0);

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        Assert.assertEquals(collector.getEmittedCount(), 1);

        byte[] rawData = getRawPayloadOfNthTuple(1);
        Assert.assertNotNull(rawData);

        CountDistinct distinct = CountDistinctTest.makeCountDistinct(config, singletonList("field"));
        distinct.combine(rawData);

        BulletRecord actual = distinct.getAggregation().getRecords().get(0);
        BulletRecord expected = RecordBox.get().add(CountDistinct.DEFAULT_NEW_NAME, 256.0).getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testNoConsumptionAfterExpiry() {
        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                makeSimpleAggregationFilterQuery("field", singletonList("b235gf23b"), EQUALS, RAW, 5));
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);
        bolt.execute(matching);
        bolt.execute(matching);

        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record);
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 3, expected));

        collector = new CustomCollector();
        // Will expire after 2 consumes (no ticks)
        bolt = ComponentUtils.prepare(new ExpiringFilterBolt(2, 1), collector);
        bolt.execute(query);
        bolt.execute(matching);
        bolt.execute(matching);
        // Now the query should be expired, so it should not consume
        bolt.execute(matching);

        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 2, expected));
    }

    @Test
    public void testDistribution() {
        Map<Object, Object> config = DistributionTest.makeConfiguration(20, 128);
        // 100 Records will be consumed
        bolt = ComponentUtils.prepare(config, new ExpiringFilterBolt(101), collector);

        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                  makeAggregationQuery(DISTRIBUTION, 10, DistributionType.PMF, "field", null, null,
                                                       null, null, 3));
        bolt.execute(query);

        IntStream.range(0, 101).mapToObj(i -> RecordBox.get().add("field", i).getRecord())
                               .map(r -> makeTuple(TupleType.Type.RECORD_TUPLE, r))
                               .forEach(bolt::execute);

        Assert.assertEquals(collector.getEmittedCount(), 0);

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        Assert.assertEquals(collector.getEmittedCount(), 1);

        byte[] rawData = getRawPayloadOfNthTuple(1);
        Assert.assertNotNull(rawData);

        Distribution distribution = DistributionTest.makeDistribution(config, makeAttributes(DistributionType.PMF, 3),
                "field", 10, null);
        distribution.combine(rawData);

        List<BulletRecord> records = distribution.getAggregation().getRecords();

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
        Map<Object, Object> config = TopKTest.makeConfiguration(ErrorType.NO_FALSE_NEGATIVES, 32);
        // 16 records
        bolt = ComponentUtils.prepare(config, new ExpiringFilterBolt(16), collector);

        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L,
                                  makeAggregationQuery(TOP_K, 5, null, "cnt", Pair.of("A", ""), Pair.of("B", "foo")));
        bolt.execute(query);

        IntStream.range(0, 8).mapToObj(i -> RecordBox.get().add("A", i).getRecord())
                .map(r -> makeTuple(TupleType.Type.RECORD_TUPLE, r))
                .forEach(bolt::execute);
        IntStream.range(0, 6).mapToObj(i -> RecordBox.get().add("A", 0).getRecord())
                             .map(r -> makeTuple(TupleType.Type.RECORD_TUPLE, r))
                .forEach(bolt::execute);
        IntStream.range(0, 2).mapToObj(i -> RecordBox.get().add("A", 3).getRecord())
                             .map(r -> makeTuple(TupleType.Type.RECORD_TUPLE, r))
                .forEach(bolt::execute);

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
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

        List<BulletRecord> records = topK.getAggregation().getRecords();
        Assert.assertEquals(records.size(), 2);

        BulletRecord expectedA = RecordBox.get().add("A", "0").add("foo", "null").add("cnt", 7L).getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "3").add("foo", "null").add("cnt", 3L).getRecord();

        Assert.assertEquals(records.get(0), expectedA);
        Assert.assertEquals(records.get(1), expectedB);
    }

    @Test
    public void testFilteringLatency() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        setup(config, new NeverExpiringFilterBolt());

        Tuple query = makeIDTuple(TupleType.Type.QUERY_TUPLE, 42L, makeFieldFilterQuery("bar"));
        bolt.execute(query);

        BulletRecord record = RecordBox.get().add("field", "foo").getRecord();
        long start = System.currentTimeMillis();
        IntStream.range(0, 10).mapToObj(i -> makeTuple(TupleType.Type.RECORD_TUPLE, record, System.currentTimeMillis()))
                              .forEach(bolt::execute);
        long end = System.currentTimeMillis();
        double actualLatecy = context.getDoubleMetric(FilterBolt.LATENCY_METRIC);
        Assert.assertTrue(actualLatecy <= end - start);
    }
}
