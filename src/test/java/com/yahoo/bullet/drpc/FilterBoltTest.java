/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.drpc;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.TestHelpers;
import com.yahoo.bullet.operations.AggregationOperations.AggregationType;
import com.yahoo.bullet.operations.FilterOperations;
import com.yahoo.bullet.operations.SerializerDeserializer;
import com.yahoo.bullet.operations.aggregations.CountDistinct;
import com.yahoo.bullet.operations.aggregations.CountDistinctTest;
import com.yahoo.bullet.operations.aggregations.grouping.GroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.tracing.FilterRule;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.yahoo.bullet.drpc.TupleUtils.makeIDTuple;
import static com.yahoo.bullet.drpc.TupleUtils.makeTuple;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.COUNT;
import static com.yahoo.bullet.operations.FilterOperations.FilterType.AND;
import static com.yahoo.bullet.operations.FilterOperations.FilterType.EQUALS;
import static com.yahoo.bullet.operations.FilterOperations.FilterType.GREATER_THAN;
import static com.yahoo.bullet.operations.FilterOperations.FilterType.OR;
import static com.yahoo.bullet.parsing.LogicalClauseTest.clause;
import static com.yahoo.bullet.parsing.RuleUtils.getFilterRule;
import static com.yahoo.bullet.parsing.RuleUtils.makeAggregationRule;
import static com.yahoo.bullet.parsing.RuleUtils.makeFieldFilterRule;
import static com.yahoo.bullet.parsing.RuleUtils.makeFilterRule;
import static com.yahoo.bullet.parsing.RuleUtils.makeGroupFilterRule;
import static com.yahoo.bullet.parsing.RuleUtils.makeProjectionFilterRule;
import static com.yahoo.bullet.parsing.RuleUtils.makeProjectionRule;
import static com.yahoo.bullet.parsing.RuleUtils.makeSimpleAggregationFilterRule;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class FilterBoltTest {
    private CustomCollector collector;
    private FilterBolt bolt;

    private class NoRuleFilterBolt extends FilterBolt {
        @Override
        protected FilterRule getRule(Long id, String ruleString) {
            return null;
        }
    }

    private class ExpiringFilterBolt extends FilterBolt {
        @Override
        protected FilterRule getRule(Long id, String ruleString) {
            FilterRule spied = spy(getFilterRule(ruleString, configuration));
            when(spied.isExpired()).thenReturn(false).thenReturn(true);
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
        return actual.getAsBulletRecord().equals(expected);
    }

    @BeforeMethod
    public void setup() {
        collector = new CustomCollector();
        bolt = ComponentUtils.prepare(new FilterBolt(), collector);
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
        Tuple rule = TupleUtils.makeTuple(TupleType.Type.RETURN_TUPLE, "", "");
        bolt.execute(rule);
        Assert.assertFalse(collector.wasAcked(rule));
    }

    @Test
    public void testProjection() {
        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L,
                                 makeProjectionRule(Pair.of("field", "id"), Pair.of("map_field.id", "mid")));
        bolt.execute(rule);

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
        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L, "'filters' : [], ");
        bolt.execute(rule);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);

        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record);
        Assert.assertTrue(collector.wasAcked(rule));
        Assert.assertTrue(collector.wasAcked(matching));
        Assert.assertFalse(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, expected));
    }

    @Test
    public void testFiltering() {
        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L, makeFieldFilterRule("b235gf23b"));
        bolt.execute(rule);

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
        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L,
                                 makeProjectionFilterRule("map_field.id", singletonList("123"),
                                                          EQUALS,
                                                          Pair.of("field", "id"),
                                                          Pair.of("map_field.id", "mid")));
        bolt.execute(rule);

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
        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L,
                                 makeProjectionFilterRule("mid", singletonList("123"),
                                                          EQUALS,
                                                          Pair.of("field", "id"),
                                                          Pair.of("map_field.id", "mid")));
        bolt.execute(rule);

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
        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L,
                                 makeProjectionFilterRule("timestamp", singletonList("92"),
                                                          EQUALS,
                                                          Pair.of("field", "id"),
                                                          Pair.of("map_field.id", "mid")));
        bolt.execute(rule);

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
        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L,
                                 makeSimpleAggregationFilterRule("field", singletonList("b235gf23b"),
                                                                 EQUALS, AggregationType.RAW, 5));
        bolt.execute(rule);
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
    public void testDifferentRuleMatchingSameTuple() {
        Tuple ruleA = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L, makeFieldFilterRule("b235gf23b"));
        Tuple ruleB = makeIDTuple(TupleType.Type.RULE_TUPLE, 43L,
                                  makeFilterRule("timestamp", asList("1", "2", "3", "45"),
                                                 EQUALS));
        bolt.execute(ruleA);
        bolt.execute(ruleB);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").add("timestamp", 45L).getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);

        bolt.execute(matching);

        Tuple expectedA = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record);
        Tuple expectedB = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 43L, record);

        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expectedA));
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expectedB));
    }

    @Test
    public void testDifferentRuleMatchingDifferentTuple() {
        Tuple ruleA = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L, makeFieldFilterRule("b235gf23b"));
        Tuple ruleB = makeIDTuple(TupleType.Type.RULE_TUPLE, 43L,
                                  makeFilterRule("timestamp", asList("1", "2", "3", "45"),
                                                 FilterOperations.FilterType.NOT_EQUALS));
        bolt.execute(ruleA);
        bolt.execute(ruleB);

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
    public void testFailRuleInitialization() {
        bolt = ComponentUtils.prepare(new NoRuleFilterBolt(), collector);

        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L, makeFieldFilterRule("b235gf23b"));
        bolt.execute(rule);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);

        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record);
        Assert.assertFalse(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, expected));
    }

    @Test
    public void testRuleNonExpiry() {
        bolt = ComponentUtils.prepare(new ExpiringFilterBolt(), collector);

        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L, makeFieldFilterRule("b235gf23b"));
        bolt.execute(rule);

        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);

        BulletRecord record = RecordBox.get().add("field", "b235gf23b").getRecord();
        Tuple matching = makeTuple(TupleType.Type.RECORD_TUPLE, record);
        bolt.execute(matching);

        Tuple expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record);
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expected));
    }

    @Test
    public void testRuleExpiry() {
        bolt = ComponentUtils.prepare(new ExpiringFilterBolt(), collector);

        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L, makeFieldFilterRule("b235gf23b"));
        bolt.execute(rule);

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
    public void testRuleNonExpiryAndThenExpiry() {
        bolt = ComponentUtils.prepare(new ExpiringFilterBolt(), collector);

        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L, makeFieldFilterRule("b235gf23b"));
        bolt.execute(rule);

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
    public void testComplexFilterRule() {
        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L,
                                 makeFilterRule(OR,
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
        bolt.execute(rule);

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

        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L, makeFieldFilterRule("b235gf23b"));
        bolt.execute(rule);

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
        bolt = ComponentUtils.prepare(new ExpiringFilterBolt(), collector);

        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L,
                                 makeGroupFilterRule("timestamp", asList("1", "2"), EQUALS,
                                                     AggregationType.GROUP, 1,
                                                     singletonList(new GroupOperation(COUNT, null, "cnt"))));
        bolt.execute(rule);

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
        bolt = ComponentUtils.prepare(config, new ExpiringFilterBolt(), collector);

        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L,
                                 makeSimpleAggregationFilterRule("field", singletonList("b235gf23b"),
                                                                 EQUALS, AggregationType.RAW, 7));
        bolt.execute(rule);

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

        // Two to flush bolt. When the rule expires, the last two get emitted.
        Tuple tick = TupleUtils.makeTuple(TupleType.Type.TICK_TUPLE);
        bolt.execute(tick);
        bolt.execute(tick);

        expected = makeRecordTuple(TupleType.Type.FILTER_TUPLE, 42L, record, record);
        Assert.assertTrue(wasRawRecordEmittedTo(FilterBolt.FILTER_STREAM, 1, expected));
    }

    @Test
    public void testCountDistinct() {
        Map<Object, Object> config = CountDistinctTest.makeConfiguration(8, 512);
        bolt = ComponentUtils.prepare(config, new ExpiringFilterBolt(), collector);

        Tuple rule = makeIDTuple(TupleType.Type.RULE_TUPLE, 42L,
                                 makeAggregationRule(AggregationType.COUNT_DISTINCT, 1, null,
                                                     Pair.of("field", "field")));
        bolt.execute(rule);

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
}
