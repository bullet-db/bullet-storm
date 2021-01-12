/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.storm.grouping.IDGrouping;
import com.yahoo.bullet.storm.grouping.TaskIndexCaptureGrouping;
import com.yahoo.bullet.storm.testing.CustomBoltDeclarer;
import com.yahoo.bullet.storm.testing.CustomIMetricsConsumer;
import com.yahoo.bullet.storm.testing.CustomIRichSpout;
import com.yahoo.bullet.storm.testing.CustomSpoutDeclarer;
import com.yahoo.bullet.storm.testing.CustomTopologyBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.yahoo.bullet.storm.TopologyConstants.CAPTURE_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.DATA_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.DATA_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.ERROR_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.FEEDBACK_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.FILTER_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.ID_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.JOIN_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.LOOP_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.METADATA_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.QUERY_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.QUERY_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.RECORD_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.RESULT_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.RESULT_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.TICK_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.TICK_STREAM;
import static java.util.Collections.singletonList;

public class StormUtilsTest {
    private static final int NUM_TESTS = 10000;
    private static final int HASH_COUNT = 10;

    private CustomTopologyBuilder builder;
    private BulletStormConfig config;

    private static class Unserializable {
    }

    private BulletStormConfig makeInvalidConfig(BulletStormConfig config) {
        config.set(BulletStormConfig.CUSTOM_STORM_SETTING_PREFIX + "unserializable", new Unserializable());
        return config;
    }

    private void submitWithTopology(String recordComponent) {
        try {
            StormUtils.submit(makeInvalidConfig(config), recordComponent, builder);
        } catch (Exception ignored) {
        }
    }

    private void submitWithConfig(BulletStormConfig config) {
        try {
            StormUtils.submit(makeInvalidConfig(config), builder);
        } catch (Exception ignored) {
        }
    }

    private void submitWithTopologyAndConfig(String recordComponent, BulletStormConfig config) {
        try {
            StormUtils.submit(makeInvalidConfig(config), recordComponent, builder);
        } catch (Exception ignored) {
        }
    }

    private CustomBoltDeclarer getBolt(String name)  {
        return builder.getCreatedBolts().stream().filter(b -> name.equals(b.getId())).findFirst().orElse(null);
    }

    private CustomSpoutDeclarer getSpout(String name)  {
        return builder.getCreatedSpouts().stream().filter(s -> name.equals(s.getId())).findFirst().orElse(null);
    }

    private void assertContains(List<Pair<String, String>> componentStreams, String component, String stream) {
        Pair<String, String> pair = Pair.of(component, stream);
        Assert.assertTrue(componentStreams.stream().anyMatch(pair::equals));
    }

    private void assertContains(Map<Pair<String, String>, List<Fields>> groupings, String component, String stream, Fields... fields) {
        List<Fields> actuals = groupings.get(Pair.of(component, stream));
        Assert.assertNotNull(actuals);
        List<Fields> expecteds = Arrays.asList(fields);
        Assert.assertEquals(actuals.size(), expecteds.size());
        for (int i = 0; i < actuals.size(); ++i) {
            Assert.assertEquals(actuals.get(i).toList(), expecteds.get(i).toList());
        }
    }

    private void assertContains(Map<Pair<String, String>, CustomStreamGrouping> groupings, String component, String stream,
                                Class<? extends CustomStreamGrouping> klazz) {
        Assert.assertTrue(klazz.isInstance(groupings.get(Pair.of(component, stream))));
    }

    @BeforeMethod
    public void setup() {
        builder = new CustomTopologyBuilder();
        builder.setThrowExceptionOnCreate(false);
        config = new BulletStormConfig();
    }

    @Test
    public void testHookingIntoExistingRecordSource() {
        builder.setSpout("source", new CustomIRichSpout(), 10);

        Assert.assertFalse(builder.isTopologyCreated());
        submitWithTopology("source");

        Assert.assertTrue(builder.isTopologyCreated());

        Assert.assertEquals(builder.getCreatedSpouts().size(), 3);
        Assert.assertEquals(builder.getCreatedBolts().size(), 4);

        CustomSpoutDeclarer sourceSpout = getSpout("source");
        Assert.assertNotNull(sourceSpout);
        Assert.assertEquals(sourceSpout.getSpout().getClass(), CustomIRichSpout.class);
        Assert.assertEquals(sourceSpout.getParallelism(), 10);
        Assert.assertNull(sourceSpout.getCpuLoad());
        Assert.assertNull(sourceSpout.getOnHeap());
        Assert.assertNull(sourceSpout.getOffHeap());

        CustomSpoutDeclarer tickSpout = getSpout(TICK_COMPONENT);
        Assert.assertNotNull(tickSpout);
        Assert.assertEquals(tickSpout.getSpout().getClass(), TickSpout.class);
        Assert.assertEquals(tickSpout.getParallelism(), BulletStormConfig.TICK_SPOUT_PARALLELISM);
        Assert.assertEquals(tickSpout.getCpuLoad(), BulletStormConfig.DEFAULT_TICK_SPOUT_CPU_LOAD);
        Assert.assertEquals(tickSpout.getOnHeap(), BulletStormConfig.DEFAULT_TICK_SPOUT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(tickSpout.getOffHeap(), BulletStormConfig.DEFAULT_TICK_SPOUT_MEMORY_OFF_HEAP_LOAD);

        CustomSpoutDeclarer querySpout = getSpout(QUERY_COMPONENT);
        Assert.assertNotNull(querySpout);
        Assert.assertEquals(querySpout.getSpout().getClass(), QuerySpout.class);
        Assert.assertEquals(querySpout.getParallelism(), BulletStormConfig.DEFAULT_QUERY_SPOUT_PARALLELISM);
        Assert.assertEquals(querySpout.getCpuLoad(), BulletStormConfig.DEFAULT_QUERY_SPOUT_CPU_LOAD);
        Assert.assertEquals(querySpout.getOnHeap(), BulletStormConfig.DEFAULT_QUERY_SPOUT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(querySpout.getOffHeap(), BulletStormConfig.DEFAULT_QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD);

        CustomBoltDeclarer filterBolt = getBolt(FILTER_COMPONENT);
        Assert.assertNotNull(filterBolt);
        Assert.assertEquals(filterBolt.getBolt().getClass(), FilterBolt.class);
        Assert.assertEquals(filterBolt.getParallelism(), BulletStormConfig.DEFAULT_FILTER_BOLT_PARALLELISM);
        Assert.assertEquals(filterBolt.getCpuLoad(), BulletStormConfig.DEFAULT_FILTER_BOLT_CPU_LOAD);
        Assert.assertEquals(filterBolt.getOnHeap(), BulletStormConfig.DEFAULT_FILTER_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(filterBolt.getOffHeap(), BulletStormConfig.DEFAULT_FILTER_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> filterAllGroupings = filterBolt.getAllGroupings();
        Assert.assertEquals(filterAllGroupings.size(), 3);
        assertContains(filterAllGroupings, TICK_COMPONENT, TICK_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, QUERY_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, METADATA_STREAM);
        List<Pair<String, String>> filterShuffleGroupings = filterBolt.getShuffleGroupings();
        Assert.assertEquals(filterShuffleGroupings.size(), 1);
        assertContains(filterShuffleGroupings, "source", Utils.DEFAULT_STREAM_ID);
        Assert.assertTrue(filterBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(filterBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(filterBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer joinBolt = getBolt(JOIN_COMPONENT);
        Assert.assertNotNull(joinBolt);
        Assert.assertEquals(joinBolt.getBolt().getClass(), JoinBolt.class);
        Assert.assertEquals(joinBolt.getParallelism(), BulletStormConfig.DEFAULT_JOIN_BOLT_PARALLELISM);
        Assert.assertEquals(joinBolt.getCpuLoad(), BulletStormConfig.DEFAULT_JOIN_BOLT_CPU_LOAD);
        Assert.assertEquals(joinBolt.getOnHeap(), BulletStormConfig.DEFAULT_JOIN_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(joinBolt.getOffHeap(), BulletStormConfig.DEFAULT_JOIN_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> joinAllGroupings = joinBolt.getAllGroupings();
        Assert.assertEquals(joinAllGroupings.size(), 1);
        assertContains(joinAllGroupings, TICK_COMPONENT, TICK_STREAM);
        Map<Pair<String, String>, List<Fields>> joinFieldGroupings = joinBolt.getFieldsGroupings();
        Assert.assertEquals(joinFieldGroupings.size(), 3);
        assertContains(joinFieldGroupings, QUERY_COMPONENT, QUERY_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, QUERY_COMPONENT, METADATA_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, FILTER_COMPONENT, DATA_STREAM, new Fields(ID_FIELD));
        Assert.assertTrue(joinBolt.getShuffleGroupings().isEmpty());
        Assert.assertTrue(joinBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(joinBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer resultBolt = getBolt(RESULT_COMPONENT);
        Assert.assertNotNull(resultBolt);
        Assert.assertEquals(resultBolt.getBolt().getClass(), ResultBolt.class);
        Assert.assertEquals(resultBolt.getParallelism(), BulletStormConfig.DEFAULT_RESULT_BOLT_PARALLELISM);
        Assert.assertEquals(resultBolt.getCpuLoad(), BulletStormConfig.DEFAULT_RESULT_BOLT_CPU_LOAD);
        Assert.assertEquals(resultBolt.getOnHeap(), BulletStormConfig.DEFAULT_RESULT_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(resultBolt.getOffHeap(), BulletStormConfig.DEFAULT_RESULT_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> resultShuffleGroupings = resultBolt.getShuffleGroupings();
        Assert.assertEquals(resultShuffleGroupings.size(), 1);
        assertContains(resultShuffleGroupings, JOIN_COMPONENT, RESULT_STREAM);
        Assert.assertTrue(resultBolt.getAllGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer loopBolt = getBolt(LOOP_COMPONENT);
        Assert.assertNotNull(loopBolt);
        Assert.assertEquals(loopBolt.getBolt().getClass(), LoopBolt.class);
        Assert.assertEquals(loopBolt.getParallelism(), BulletStormConfig.DEFAULT_LOOP_BOLT_PARALLELISM);
        Assert.assertEquals(loopBolt.getCpuLoad(), BulletStormConfig.DEFAULT_LOOP_BOLT_CPU_LOAD);
        Assert.assertEquals(loopBolt.getOnHeap(), BulletStormConfig.DEFAULT_LOOP_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(loopBolt.getOffHeap(), BulletStormConfig.DEFAULT_LOOP_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> loopShuffleGroupings = loopBolt.getShuffleGroupings();
        Assert.assertEquals(loopShuffleGroupings.size(), 2);
        assertContains(loopShuffleGroupings, FILTER_COMPONENT, FEEDBACK_STREAM);
        assertContains(loopShuffleGroupings, JOIN_COMPONENT, FEEDBACK_STREAM);
        Assert.assertTrue(loopBolt.getAllGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer replayBolt = getBolt(REPLAY_COMPONENT);
        Assert.assertNull(replayBolt);
    }

    @Test
    public void testHookingWithReplay() {
        config.set(BulletStormConfig.REPLAY_ENABLE, true);
        config.validate();

        builder.setSpout("source", new CustomIRichSpout(), 10);

        Assert.assertFalse(builder.isTopologyCreated());
        submitWithTopologyAndConfig("source", config);

        Assert.assertTrue(builder.isTopologyCreated());

        Assert.assertEquals(builder.getCreatedSpouts().size(), 3);
        Assert.assertEquals(builder.getCreatedBolts().size(), 5);

        CustomSpoutDeclarer sourceSpout = getSpout("source");
        Assert.assertNotNull(sourceSpout);
        Assert.assertEquals(sourceSpout.getSpout().getClass(), CustomIRichSpout.class);
        Assert.assertEquals(sourceSpout.getParallelism(), 10);
        Assert.assertNull(sourceSpout.getCpuLoad());
        Assert.assertNull(sourceSpout.getOnHeap());
        Assert.assertNull(sourceSpout.getOffHeap());

        CustomSpoutDeclarer tickSpout = getSpout(TICK_COMPONENT);
        Assert.assertNotNull(tickSpout);
        Assert.assertEquals(tickSpout.getSpout().getClass(), TickSpout.class);
        Assert.assertEquals(tickSpout.getParallelism(), BulletStormConfig.TICK_SPOUT_PARALLELISM);
        Assert.assertEquals(tickSpout.getCpuLoad(), BulletStormConfig.DEFAULT_TICK_SPOUT_CPU_LOAD);
        Assert.assertEquals(tickSpout.getOnHeap(), BulletStormConfig.DEFAULT_TICK_SPOUT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(tickSpout.getOffHeap(), BulletStormConfig.DEFAULT_TICK_SPOUT_MEMORY_OFF_HEAP_LOAD);

        CustomSpoutDeclarer querySpout = getSpout(QUERY_COMPONENT);
        Assert.assertNotNull(querySpout);
        Assert.assertEquals(querySpout.getSpout().getClass(), QuerySpout.class);
        Assert.assertEquals(querySpout.getParallelism(), BulletStormConfig.DEFAULT_QUERY_SPOUT_PARALLELISM);
        Assert.assertEquals(querySpout.getCpuLoad(), BulletStormConfig.DEFAULT_QUERY_SPOUT_CPU_LOAD);
        Assert.assertEquals(querySpout.getOnHeap(), BulletStormConfig.DEFAULT_QUERY_SPOUT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(querySpout.getOffHeap(), BulletStormConfig.DEFAULT_QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD);

        CustomBoltDeclarer filterBolt = getBolt(FILTER_COMPONENT);
        Assert.assertNotNull(filterBolt);
        Assert.assertEquals(filterBolt.getBolt().getClass(), FilterBolt.class);
        Assert.assertEquals(filterBolt.getParallelism(), BulletStormConfig.DEFAULT_FILTER_BOLT_PARALLELISM);
        Assert.assertEquals(filterBolt.getCpuLoad(), BulletStormConfig.DEFAULT_FILTER_BOLT_CPU_LOAD);
        Assert.assertEquals(filterBolt.getOnHeap(), BulletStormConfig.DEFAULT_FILTER_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(filterBolt.getOffHeap(), BulletStormConfig.DEFAULT_FILTER_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> filterAllGroupings = filterBolt.getAllGroupings();
        Assert.assertEquals(filterAllGroupings.size(), 3);
        assertContains(filterAllGroupings, TICK_COMPONENT, TICK_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, QUERY_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, METADATA_STREAM);
        List<Pair<String, String>> filterShuffleGroupings = filterBolt.getShuffleGroupings();
        Assert.assertEquals(filterShuffleGroupings.size(), 1);
        assertContains(filterShuffleGroupings, "source", Utils.DEFAULT_STREAM_ID);
        List<Pair<String, String>> filterDirectGroupings = filterBolt.getDirectGroupings();
        Assert.assertEquals(filterDirectGroupings.size(), 1);
        assertContains(filterDirectGroupings, REPLAY_COMPONENT, REPLAY_STREAM);
        Assert.assertTrue(filterBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(filterBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer joinBolt = getBolt(JOIN_COMPONENT);
        Assert.assertNotNull(joinBolt);
        Assert.assertEquals(joinBolt.getBolt().getClass(), JoinBolt.class);
        Assert.assertEquals(joinBolt.getParallelism(), BulletStormConfig.DEFAULT_JOIN_BOLT_PARALLELISM);
        Assert.assertEquals(joinBolt.getCpuLoad(), BulletStormConfig.DEFAULT_JOIN_BOLT_CPU_LOAD);
        Assert.assertEquals(joinBolt.getOnHeap(), BulletStormConfig.DEFAULT_JOIN_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(joinBolt.getOffHeap(), BulletStormConfig.DEFAULT_JOIN_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> joinAllGroupings = joinBolt.getAllGroupings();
        Assert.assertEquals(joinAllGroupings.size(), 2);
        assertContains(joinAllGroupings, TICK_COMPONENT, TICK_STREAM);
        assertContains(joinAllGroupings, QUERY_COMPONENT, METADATA_STREAM);
        List<Pair<String, String>> joinDirectGroupings = joinBolt.getDirectGroupings();
        Assert.assertEquals(joinDirectGroupings.size(), 1);
        assertContains(joinDirectGroupings, REPLAY_COMPONENT, REPLAY_STREAM);
        Map<Pair<String, String>, CustomStreamGrouping> joinCustomGroupings = joinBolt.getCustomGroupings();
        Assert.assertEquals(joinCustomGroupings.size(), 4);
        assertContains(joinCustomGroupings, QUERY_COMPONENT, QUERY_STREAM, IDGrouping.class);
        assertContains(joinCustomGroupings, FILTER_COMPONENT, DATA_STREAM, IDGrouping.class);
        assertContains(joinCustomGroupings, FILTER_COMPONENT, ERROR_STREAM, IDGrouping.class);
        assertContains(joinCustomGroupings, REPLAY_COMPONENT, CAPTURE_STREAM, TaskIndexCaptureGrouping.class);
        Assert.assertTrue(joinBolt.getShuffleGroupings().isEmpty());
        Assert.assertTrue(joinBolt.getFieldsGroupings().isEmpty());

        CustomBoltDeclarer resultBolt = getBolt(RESULT_COMPONENT);
        Assert.assertNotNull(resultBolt);
        Assert.assertEquals(resultBolt.getBolt().getClass(), ResultBolt.class);
        Assert.assertEquals(resultBolt.getParallelism(), BulletStormConfig.DEFAULT_RESULT_BOLT_PARALLELISM);
        Assert.assertEquals(resultBolt.getCpuLoad(), BulletStormConfig.DEFAULT_RESULT_BOLT_CPU_LOAD);
        Assert.assertEquals(resultBolt.getOnHeap(), BulletStormConfig.DEFAULT_RESULT_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(resultBolt.getOffHeap(), BulletStormConfig.DEFAULT_RESULT_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> resultShuffleGroupings = resultBolt.getShuffleGroupings();
        Assert.assertEquals(resultShuffleGroupings.size(), 1);
        assertContains(resultShuffleGroupings, JOIN_COMPONENT, RESULT_STREAM);
        Assert.assertTrue(resultBolt.getAllGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer loopBolt = getBolt(LOOP_COMPONENT);
        Assert.assertNotNull(loopBolt);
        Assert.assertEquals(loopBolt.getBolt().getClass(), LoopBolt.class);
        Assert.assertEquals(loopBolt.getParallelism(), BulletStormConfig.DEFAULT_LOOP_BOLT_PARALLELISM);
        Assert.assertEquals(loopBolt.getCpuLoad(), BulletStormConfig.DEFAULT_LOOP_BOLT_CPU_LOAD);
        Assert.assertEquals(loopBolt.getOnHeap(), BulletStormConfig.DEFAULT_LOOP_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(loopBolt.getOffHeap(), BulletStormConfig.DEFAULT_LOOP_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> loopShuffleGroupings = loopBolt.getShuffleGroupings();
        Assert.assertEquals(loopShuffleGroupings.size(), 2);
        assertContains(loopShuffleGroupings, FILTER_COMPONENT, FEEDBACK_STREAM);
        assertContains(loopShuffleGroupings, JOIN_COMPONENT, FEEDBACK_STREAM);
        Assert.assertTrue(loopBolt.getAllGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer replayBolt = getBolt(REPLAY_COMPONENT);
        Assert.assertNotNull(replayBolt);
        Assert.assertEquals(replayBolt.getBolt().getClass(), ReplayBolt.class);
        Assert.assertEquals(replayBolt.getParallelism(), BulletStormConfig.DEFAULT_REPLAY_BOLT_PARALLELISM);
        Assert.assertEquals(replayBolt.getCpuLoad(), BulletStormConfig.DEFAULT_REPLAY_BOLT_CPU_LOAD);
        Assert.assertEquals(replayBolt.getOnHeap(), BulletStormConfig.DEFAULT_REPLAY_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(replayBolt.getOffHeap(), BulletStormConfig.DEFAULT_REPLAY_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> replayAllGroupings = replayBolt.getAllGroupings();
        Assert.assertEquals(replayAllGroupings.size(), 2);
        assertContains(replayAllGroupings, QUERY_COMPONENT, QUERY_STREAM);
        assertContains(replayAllGroupings, QUERY_COMPONENT, METADATA_STREAM);
        Map<Pair<String, String>, List<Fields>> replayFieldGroupings = replayBolt.getFieldsGroupings();
        Assert.assertEquals(replayFieldGroupings.size(), 1);
        assertContains(replayFieldGroupings, QUERY_COMPONENT, REPLAY_STREAM, new Fields(ID_FIELD));
        Assert.assertTrue(replayBolt.getShuffleGroupings().isEmpty());
        Assert.assertTrue(replayBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(replayBolt.getCustomGroupings().isEmpty());
    }

    @Test
    public void testHookingInDSLSpout() {
        config = new BulletStormConfig("src/test/resources/test_dsl_config.yaml");
        config.set(BulletStormConfig.DSL_SPOUT_ENABLE, true);

        Assert.assertFalse(builder.isTopologyCreated());
        submitWithConfig(config);

        Assert.assertTrue(builder.isTopologyCreated());

        Assert.assertEquals(builder.getCreatedSpouts().size(), 3);
        Assert.assertEquals(builder.getCreatedBolts().size(), 4);

        CustomSpoutDeclarer source = getSpout(RECORD_COMPONENT);
        Assert.assertNotNull(source);
        Assert.assertEquals(source.getSpout().getClass(), DSLSpout.class);
        Assert.assertEquals(source.getParallelism(), BulletStormConfig.DEFAULT_DSL_SPOUT_PARALLELISM);
        Assert.assertEquals(source.getCpuLoad(), BulletStormConfig.DEFAULT_DSL_SPOUT_CPU_LOAD);
        Assert.assertEquals(source.getOnHeap(), BulletStormConfig.DEFAULT_DSL_SPOUT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(source.getOffHeap(), BulletStormConfig.DEFAULT_DSL_SPOUT_MEMORY_OFF_HEAP_LOAD);

        CustomSpoutDeclarer tickSpout = getSpout(TICK_COMPONENT);
        Assert.assertNotNull(tickSpout);
        Assert.assertEquals(tickSpout.getSpout().getClass(), TickSpout.class);
        Assert.assertEquals(tickSpout.getParallelism(), BulletStormConfig.TICK_SPOUT_PARALLELISM);
        Assert.assertEquals(tickSpout.getCpuLoad(), BulletStormConfig.DEFAULT_TICK_SPOUT_CPU_LOAD);
        Assert.assertEquals(tickSpout.getOnHeap(), BulletStormConfig.DEFAULT_TICK_SPOUT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(tickSpout.getOffHeap(), BulletStormConfig.DEFAULT_TICK_SPOUT_MEMORY_OFF_HEAP_LOAD);

        CustomSpoutDeclarer querySpout = getSpout(QUERY_COMPONENT);
        Assert.assertNotNull(querySpout);
        Assert.assertEquals(querySpout.getSpout().getClass(), QuerySpout.class);
        Assert.assertEquals(querySpout.getParallelism(), BulletStormConfig.DEFAULT_QUERY_SPOUT_PARALLELISM);
        Assert.assertEquals(querySpout.getCpuLoad(), BulletStormConfig.DEFAULT_QUERY_SPOUT_CPU_LOAD);
        Assert.assertEquals(querySpout.getOnHeap(), BulletStormConfig.DEFAULT_QUERY_SPOUT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(querySpout.getOffHeap(), BulletStormConfig.DEFAULT_QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD);

        CustomBoltDeclarer filterBolt = getBolt(FILTER_COMPONENT);
        Assert.assertNotNull(filterBolt);
        Assert.assertEquals(filterBolt.getBolt().getClass(), FilterBolt.class);
        Assert.assertEquals(filterBolt.getParallelism(), BulletStormConfig.DEFAULT_FILTER_BOLT_PARALLELISM);
        Assert.assertEquals(filterBolt.getCpuLoad(), BulletStormConfig.DEFAULT_FILTER_BOLT_CPU_LOAD);
        Assert.assertEquals(filterBolt.getOnHeap(), BulletStormConfig.DEFAULT_FILTER_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(filterBolt.getOffHeap(), BulletStormConfig.DEFAULT_FILTER_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> filterAllGroupings = filterBolt.getAllGroupings();
        Assert.assertEquals(filterAllGroupings.size(), 3);
        assertContains(filterAllGroupings, TICK_COMPONENT, TICK_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, QUERY_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, METADATA_STREAM);
        List<Pair<String, String>> filterShuffleGroupings = filterBolt.getShuffleGroupings();
        Assert.assertEquals(filterShuffleGroupings.size(), 1);
        assertContains(filterShuffleGroupings, RECORD_COMPONENT, Utils.DEFAULT_STREAM_ID);
        Assert.assertTrue(filterBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(filterBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(filterBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer joinBolt = getBolt(JOIN_COMPONENT);
        Assert.assertNotNull(joinBolt);
        Assert.assertEquals(joinBolt.getBolt().getClass(), JoinBolt.class);
        Assert.assertEquals(joinBolt.getParallelism(), BulletStormConfig.DEFAULT_JOIN_BOLT_PARALLELISM);
        Assert.assertEquals(joinBolt.getCpuLoad(), BulletStormConfig.DEFAULT_JOIN_BOLT_CPU_LOAD);
        Assert.assertEquals(joinBolt.getOnHeap(), BulletStormConfig.DEFAULT_JOIN_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(joinBolt.getOffHeap(), BulletStormConfig.DEFAULT_JOIN_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> joinAllGroupings = joinBolt.getAllGroupings();
        Assert.assertEquals(joinAllGroupings.size(), 1);
        assertContains(joinAllGroupings, TICK_COMPONENT, TICK_STREAM);
        Map<Pair<String, String>, List<Fields>> joinFieldGroupings = joinBolt.getFieldsGroupings();
        Assert.assertEquals(joinFieldGroupings.size(), 3);
        assertContains(joinFieldGroupings, QUERY_COMPONENT, QUERY_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, QUERY_COMPONENT, METADATA_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, FILTER_COMPONENT, DATA_STREAM, new Fields(ID_FIELD));
        Assert.assertTrue(joinBolt.getShuffleGroupings().isEmpty());
        Assert.assertTrue(joinBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(joinBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer resultBolt = getBolt(RESULT_COMPONENT);
        Assert.assertNotNull(resultBolt);
        Assert.assertEquals(resultBolt.getBolt().getClass(), ResultBolt.class);
        Assert.assertEquals(resultBolt.getParallelism(), BulletStormConfig.DEFAULT_RESULT_BOLT_PARALLELISM);
        Assert.assertEquals(resultBolt.getCpuLoad(), BulletStormConfig.DEFAULT_RESULT_BOLT_CPU_LOAD);
        Assert.assertEquals(resultBolt.getOnHeap(), BulletStormConfig.DEFAULT_RESULT_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(resultBolt.getOffHeap(), BulletStormConfig.DEFAULT_RESULT_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> resultShuffleGroupings = resultBolt.getShuffleGroupings();
        Assert.assertEquals(resultShuffleGroupings.size(), 1);
        assertContains(resultShuffleGroupings, JOIN_COMPONENT, RESULT_STREAM);
        Assert.assertTrue(resultBolt.getAllGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer loopBolt = getBolt(LOOP_COMPONENT);
        Assert.assertNotNull(loopBolt);
        Assert.assertEquals(loopBolt.getBolt().getClass(), LoopBolt.class);
        Assert.assertEquals(loopBolt.getParallelism(), BulletStormConfig.DEFAULT_LOOP_BOLT_PARALLELISM);
        Assert.assertEquals(loopBolt.getCpuLoad(), BulletStormConfig.DEFAULT_LOOP_BOLT_CPU_LOAD);
        Assert.assertEquals(loopBolt.getOnHeap(), BulletStormConfig.DEFAULT_LOOP_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(loopBolt.getOffHeap(), BulletStormConfig.DEFAULT_LOOP_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> loopShuffleGroupings = loopBolt.getShuffleGroupings();
        Assert.assertEquals(loopShuffleGroupings.size(), 2);
        assertContains(loopShuffleGroupings, FILTER_COMPONENT, FEEDBACK_STREAM);
        assertContains(loopShuffleGroupings, JOIN_COMPONENT, FEEDBACK_STREAM);
        Assert.assertTrue(loopBolt.getAllGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer replayBolt = getBolt(REPLAY_COMPONENT);
        Assert.assertNull(replayBolt);
    }

    @Test
    public void testHookingInDSLSpoutAndBolt() {
        config = new BulletStormConfig("src/test/resources/test_dsl_config.yaml");
        config.set(BulletStormConfig.DSL_SPOUT_ENABLE, true);
        config.set(BulletStormConfig.DSL_BOLT_ENABLE, true);

        Assert.assertFalse(builder.isTopologyCreated());
        submitWithConfig(config);

        Assert.assertTrue(builder.isTopologyCreated());

        Assert.assertEquals(builder.getCreatedSpouts().size(), 3);
        Assert.assertEquals(builder.getCreatedBolts().size(), 5);

        CustomSpoutDeclarer data = getSpout(DATA_COMPONENT);
        Assert.assertNotNull(data);
        Assert.assertEquals(data.getSpout().getClass(), DSLSpout.class);
        Assert.assertEquals(data.getParallelism(), BulletStormConfig.DEFAULT_DSL_SPOUT_PARALLELISM);
        Assert.assertEquals(data.getCpuLoad(), BulletStormConfig.DEFAULT_DSL_SPOUT_CPU_LOAD);
        Assert.assertEquals(data.getOnHeap(), BulletStormConfig.DEFAULT_DSL_SPOUT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(data.getOffHeap(), BulletStormConfig.DEFAULT_DSL_SPOUT_MEMORY_OFF_HEAP_LOAD);

        CustomBoltDeclarer source = getBolt(RECORD_COMPONENT);
        Assert.assertNotNull(source);
        Assert.assertEquals(source.getBolt().getClass(), DSLBolt.class);
        Assert.assertEquals(source.getParallelism(), BulletStormConfig.DEFAULT_DSL_BOLT_PARALLELISM);
        Assert.assertEquals(source.getCpuLoad(), BulletStormConfig.DEFAULT_DSL_BOLT_CPU_LOAD);
        Assert.assertEquals(source.getOnHeap(), BulletStormConfig.DEFAULT_DSL_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(source.getOffHeap(), BulletStormConfig.DEFAULT_DSL_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> sourceShuffleGroupings = source.getShuffleGroupings();
        Assert.assertEquals(sourceShuffleGroupings.size(), 1);
        assertContains(sourceShuffleGroupings, DATA_COMPONENT, Utils.DEFAULT_STREAM_ID);
        Assert.assertTrue(source.getAllGroupings().isEmpty());
        Assert.assertTrue(source.getDirectGroupings().isEmpty());
        Assert.assertTrue(source.getFieldsGroupings().isEmpty());
        Assert.assertTrue(source.getCustomGroupings().isEmpty());

        CustomSpoutDeclarer tickSpout = getSpout(TICK_COMPONENT);
        Assert.assertNotNull(tickSpout);
        Assert.assertEquals(tickSpout.getSpout().getClass(), TickSpout.class);
        Assert.assertEquals(tickSpout.getParallelism(), BulletStormConfig.TICK_SPOUT_PARALLELISM);
        Assert.assertEquals(tickSpout.getCpuLoad(), BulletStormConfig.DEFAULT_TICK_SPOUT_CPU_LOAD);
        Assert.assertEquals(tickSpout.getOnHeap(), BulletStormConfig.DEFAULT_TICK_SPOUT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(tickSpout.getOffHeap(), BulletStormConfig.DEFAULT_TICK_SPOUT_MEMORY_OFF_HEAP_LOAD);

        CustomSpoutDeclarer querySpout = getSpout(QUERY_COMPONENT);
        Assert.assertNotNull(querySpout);
        Assert.assertEquals(querySpout.getSpout().getClass(), QuerySpout.class);
        Assert.assertEquals(querySpout.getParallelism(), BulletStormConfig.DEFAULT_QUERY_SPOUT_PARALLELISM);
        Assert.assertEquals(querySpout.getCpuLoad(), BulletStormConfig.DEFAULT_QUERY_SPOUT_CPU_LOAD);
        Assert.assertEquals(querySpout.getOnHeap(), BulletStormConfig.DEFAULT_QUERY_SPOUT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(querySpout.getOffHeap(), BulletStormConfig.DEFAULT_QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD);

        CustomBoltDeclarer filterBolt = getBolt(FILTER_COMPONENT);
        Assert.assertNotNull(filterBolt);
        Assert.assertEquals(filterBolt.getBolt().getClass(), FilterBolt.class);
        Assert.assertEquals(filterBolt.getParallelism(), BulletStormConfig.DEFAULT_FILTER_BOLT_PARALLELISM);
        Assert.assertEquals(filterBolt.getCpuLoad(), BulletStormConfig.DEFAULT_FILTER_BOLT_CPU_LOAD);
        Assert.assertEquals(filterBolt.getOnHeap(), BulletStormConfig.DEFAULT_FILTER_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(filterBolt.getOffHeap(), BulletStormConfig.DEFAULT_FILTER_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> filterAllGroupings = filterBolt.getAllGroupings();
        Assert.assertEquals(filterAllGroupings.size(), 3);
        assertContains(filterAllGroupings, TICK_COMPONENT, TICK_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, QUERY_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, METADATA_STREAM);
        List<Pair<String, String>> filterShuffleGroupings = filterBolt.getShuffleGroupings();
        Assert.assertEquals(filterShuffleGroupings.size(), 1);
        assertContains(filterShuffleGroupings, RECORD_COMPONENT, Utils.DEFAULT_STREAM_ID);
        Assert.assertTrue(filterBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(filterBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(filterBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer joinBolt = getBolt(JOIN_COMPONENT);
        Assert.assertNotNull(joinBolt);
        Assert.assertEquals(joinBolt.getBolt().getClass(), JoinBolt.class);
        Assert.assertEquals(joinBolt.getParallelism(), BulletStormConfig.DEFAULT_JOIN_BOLT_PARALLELISM);
        Assert.assertEquals(joinBolt.getCpuLoad(), BulletStormConfig.DEFAULT_JOIN_BOLT_CPU_LOAD);
        Assert.assertEquals(joinBolt.getOnHeap(), BulletStormConfig.DEFAULT_JOIN_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(joinBolt.getOffHeap(), BulletStormConfig.DEFAULT_JOIN_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> joinAllGroupings = joinBolt.getAllGroupings();
        Assert.assertEquals(joinAllGroupings.size(), 1);
        assertContains(joinAllGroupings, TICK_COMPONENT, TICK_STREAM);
        Map<Pair<String, String>, List<Fields>> joinFieldGroupings = joinBolt.getFieldsGroupings();
        Assert.assertEquals(joinFieldGroupings.size(), 3);
        assertContains(joinFieldGroupings, QUERY_COMPONENT, QUERY_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, QUERY_COMPONENT, METADATA_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, FILTER_COMPONENT, DATA_STREAM, new Fields(ID_FIELD));
        Assert.assertTrue(joinBolt.getShuffleGroupings().isEmpty());
        Assert.assertTrue(joinBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(joinBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer resultBolt = getBolt(RESULT_COMPONENT);
        Assert.assertNotNull(resultBolt);
        Assert.assertEquals(resultBolt.getBolt().getClass(), ResultBolt.class);
        Assert.assertEquals(resultBolt.getParallelism(), BulletStormConfig.DEFAULT_RESULT_BOLT_PARALLELISM);
        Assert.assertEquals(resultBolt.getCpuLoad(), BulletStormConfig.DEFAULT_RESULT_BOLT_CPU_LOAD);
        Assert.assertEquals(resultBolt.getOnHeap(), BulletStormConfig.DEFAULT_RESULT_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(resultBolt.getOffHeap(), BulletStormConfig.DEFAULT_RESULT_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> resultShuffleGroupings = resultBolt.getShuffleGroupings();
        Assert.assertEquals(resultShuffleGroupings.size(), 1);
        assertContains(resultShuffleGroupings, JOIN_COMPONENT, RESULT_STREAM);
        Assert.assertTrue(resultBolt.getAllGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer loopBolt = getBolt(LOOP_COMPONENT);
        Assert.assertNotNull(loopBolt);
        Assert.assertEquals(loopBolt.getBolt().getClass(), LoopBolt.class);
        Assert.assertEquals(loopBolt.getParallelism(), BulletStormConfig.DEFAULT_LOOP_BOLT_PARALLELISM);
        Assert.assertEquals(loopBolt.getCpuLoad(), BulletStormConfig.DEFAULT_LOOP_BOLT_CPU_LOAD);
        Assert.assertEquals(loopBolt.getOnHeap(), BulletStormConfig.DEFAULT_LOOP_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(loopBolt.getOffHeap(), BulletStormConfig.DEFAULT_LOOP_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> loopShuffleGroupings = loopBolt.getShuffleGroupings();
        Assert.assertEquals(loopShuffleGroupings.size(), 2);
        assertContains(loopShuffleGroupings, FILTER_COMPONENT, FEEDBACK_STREAM);
        assertContains(loopShuffleGroupings, JOIN_COMPONENT, FEEDBACK_STREAM);
        Assert.assertTrue(loopBolt.getAllGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer replayBolt = getBolt(REPLAY_COMPONENT);
        Assert.assertNull(replayBolt);
    }

    @Test
    public void testHookingInBulletSpout() {
        config.set(BulletStormConfig.BULLET_SPOUT_CLASS_NAME, CustomIRichSpout.class.getName());

        Assert.assertFalse(builder.isTopologyCreated());
        submitWithConfig(config);

        Assert.assertTrue(builder.isTopologyCreated());

        Assert.assertEquals(builder.getCreatedSpouts().size(), 3);
        Assert.assertEquals(builder.getCreatedBolts().size(), 4);

        CustomSpoutDeclarer source = getSpout(RECORD_COMPONENT);
        Assert.assertNotNull(source);
        Assert.assertEquals(source.getSpout().getClass(), CustomIRichSpout.class);
        Assert.assertEquals(source.getParallelism(), BulletStormConfig.DEFAULT_BULLET_SPOUT_PARALLELISM);
        Assert.assertEquals(source.getCpuLoad(), BulletStormConfig.DEFAULT_BULLET_SPOUT_CPU_LOAD);
        Assert.assertEquals(source.getOnHeap(), BulletStormConfig.DEFAULT_BULLET_SPOUT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(source.getOffHeap(), BulletStormConfig.DEFAULT_BULLET_SPOUT_MEMORY_OFF_HEAP_LOAD);

        CustomSpoutDeclarer tickSpout = getSpout(TICK_COMPONENT);
        Assert.assertNotNull(tickSpout);
        Assert.assertEquals(tickSpout.getSpout().getClass(), TickSpout.class);
        Assert.assertEquals(tickSpout.getParallelism(), BulletStormConfig.TICK_SPOUT_PARALLELISM);
        Assert.assertEquals(tickSpout.getCpuLoad(), BulletStormConfig.DEFAULT_TICK_SPOUT_CPU_LOAD);
        Assert.assertEquals(tickSpout.getOnHeap(), BulletStormConfig.DEFAULT_TICK_SPOUT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(tickSpout.getOffHeap(), BulletStormConfig.DEFAULT_TICK_SPOUT_MEMORY_OFF_HEAP_LOAD);

        CustomSpoutDeclarer querySpout = getSpout(QUERY_COMPONENT);
        Assert.assertNotNull(querySpout);
        Assert.assertEquals(querySpout.getSpout().getClass(), QuerySpout.class);
        Assert.assertEquals(querySpout.getParallelism(), BulletStormConfig.DEFAULT_QUERY_SPOUT_PARALLELISM);
        Assert.assertEquals(querySpout.getCpuLoad(), BulletStormConfig.DEFAULT_QUERY_SPOUT_CPU_LOAD);
        Assert.assertEquals(querySpout.getOnHeap(), BulletStormConfig.DEFAULT_QUERY_SPOUT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(querySpout.getOffHeap(), BulletStormConfig.DEFAULT_QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD);

        CustomBoltDeclarer filterBolt = getBolt(FILTER_COMPONENT);
        Assert.assertNotNull(filterBolt);
        Assert.assertEquals(filterBolt.getBolt().getClass(), FilterBolt.class);
        Assert.assertEquals(filterBolt.getParallelism(), BulletStormConfig.DEFAULT_FILTER_BOLT_PARALLELISM);
        Assert.assertEquals(filterBolt.getCpuLoad(), BulletStormConfig.DEFAULT_FILTER_BOLT_CPU_LOAD);
        Assert.assertEquals(filterBolt.getOnHeap(), BulletStormConfig.DEFAULT_FILTER_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(filterBolt.getOffHeap(), BulletStormConfig.DEFAULT_FILTER_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> filterAllGroupings = filterBolt.getAllGroupings();
        Assert.assertEquals(filterAllGroupings.size(), 3);
        assertContains(filterAllGroupings, TICK_COMPONENT, TICK_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, QUERY_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, METADATA_STREAM);
        List<Pair<String, String>> filterShuffleGroupings = filterBolt.getShuffleGroupings();
        Assert.assertEquals(filterShuffleGroupings.size(), 1);
        assertContains(filterShuffleGroupings, RECORD_COMPONENT, Utils.DEFAULT_STREAM_ID);
        Assert.assertTrue(filterBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(filterBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(filterBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer joinBolt = getBolt(JOIN_COMPONENT);
        Assert.assertNotNull(joinBolt);
        Assert.assertEquals(joinBolt.getBolt().getClass(), JoinBolt.class);
        Assert.assertEquals(joinBolt.getParallelism(), BulletStormConfig.DEFAULT_JOIN_BOLT_PARALLELISM);
        Assert.assertEquals(joinBolt.getCpuLoad(), BulletStormConfig.DEFAULT_JOIN_BOLT_CPU_LOAD);
        Assert.assertEquals(joinBolt.getOnHeap(), BulletStormConfig.DEFAULT_JOIN_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(joinBolt.getOffHeap(), BulletStormConfig.DEFAULT_JOIN_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> joinAllGroupings = joinBolt.getAllGroupings();
        Assert.assertEquals(joinAllGroupings.size(), 1);
        assertContains(joinAllGroupings, TICK_COMPONENT, TICK_STREAM);
        Map<Pair<String, String>, List<Fields>> joinFieldGroupings = joinBolt.getFieldsGroupings();
        Assert.assertEquals(joinFieldGroupings.size(), 3);
        assertContains(joinFieldGroupings, QUERY_COMPONENT, QUERY_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, QUERY_COMPONENT, METADATA_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, FILTER_COMPONENT, DATA_STREAM, new Fields(ID_FIELD));
        Assert.assertTrue(joinBolt.getShuffleGroupings().isEmpty());
        Assert.assertTrue(joinBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(joinBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer resultBolt = getBolt(RESULT_COMPONENT);
        Assert.assertNotNull(resultBolt);
        Assert.assertEquals(resultBolt.getBolt().getClass(), ResultBolt.class);
        Assert.assertEquals(resultBolt.getParallelism(), BulletStormConfig.DEFAULT_RESULT_BOLT_PARALLELISM);
        Assert.assertEquals(resultBolt.getCpuLoad(), BulletStormConfig.DEFAULT_RESULT_BOLT_CPU_LOAD);
        Assert.assertEquals(resultBolt.getOnHeap(), BulletStormConfig.DEFAULT_RESULT_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(resultBolt.getOffHeap(), BulletStormConfig.DEFAULT_RESULT_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> resultShuffleGroupings = resultBolt.getShuffleGroupings();
        Assert.assertEquals(resultShuffleGroupings.size(), 1);
        assertContains(resultShuffleGroupings, JOIN_COMPONENT, RESULT_STREAM);
        Assert.assertTrue(resultBolt.getAllGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(resultBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer loopBolt = getBolt(LOOP_COMPONENT);
        Assert.assertNotNull(loopBolt);
        Assert.assertEquals(loopBolt.getBolt().getClass(), LoopBolt.class);
        Assert.assertEquals(loopBolt.getParallelism(), BulletStormConfig.DEFAULT_LOOP_BOLT_PARALLELISM);
        Assert.assertEquals(loopBolt.getCpuLoad(), BulletStormConfig.DEFAULT_LOOP_BOLT_CPU_LOAD);
        Assert.assertEquals(loopBolt.getOnHeap(), BulletStormConfig.DEFAULT_LOOP_BOLT_MEMORY_ON_HEAP_LOAD);
        Assert.assertEquals(loopBolt.getOffHeap(), BulletStormConfig.DEFAULT_LOOP_BOLT_MEMORY_OFF_HEAP_LOAD);
        List<Pair<String, String>> loopShuffleGroupings = loopBolt.getShuffleGroupings();
        Assert.assertEquals(loopShuffleGroupings.size(), 2);
        assertContains(loopShuffleGroupings, FILTER_COMPONENT, FEEDBACK_STREAM);
        assertContains(loopShuffleGroupings, JOIN_COMPONENT, FEEDBACK_STREAM);
        Assert.assertTrue(loopBolt.getAllGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getDirectGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getFieldsGroupings().isEmpty());
        Assert.assertTrue(loopBolt.getCustomGroupings().isEmpty());

        CustomBoltDeclarer replayBolt = getBolt(REPLAY_COMPONENT);
        Assert.assertNull(replayBolt);
    }

    @Test
    public void testDisabledLoopBoltOnNoWindowing() {
        builder.setSpout("source", new CustomIRichSpout(), 10);
        config.set(BulletStormConfig.WINDOW_DISABLE, true);

        Assert.assertFalse(builder.isTopologyCreated());
        submitWithTopology("source");

        Assert.assertTrue(builder.isTopologyCreated());

        Assert.assertEquals(builder.getCreatedSpouts().size(), 3);
        Assert.assertEquals(builder.getCreatedBolts().size(), 3);

        Assert.assertNotNull(getSpout("source"));
        Assert.assertNotNull(getSpout(TICK_COMPONENT));
        Assert.assertNotNull(getSpout(QUERY_COMPONENT));
        Assert.assertNotNull(getBolt(FILTER_COMPONENT));
        Assert.assertNotNull(getBolt(JOIN_COMPONENT));
        Assert.assertNotNull(getBolt(RESULT_COMPONENT));
        Assert.assertNull(getBolt(LOOP_COMPONENT));
    }

    @Test
    public void testEnabledLoopBoltOnNoWindowingButReplay() {
        builder.setSpout("source", new CustomIRichSpout(), 10);
        config.set(BulletStormConfig.WINDOW_DISABLE, true);
        config.set(BulletStormConfig.REPLAY_ENABLE, true);

        Assert.assertFalse(builder.isTopologyCreated());
        submitWithTopology("source");

        Assert.assertTrue(builder.isTopologyCreated());

        Assert.assertEquals(builder.getCreatedSpouts().size(), 3);
        Assert.assertEquals(builder.getCreatedBolts().size(), 5);

        Assert.assertNotNull(getSpout("source"));
        Assert.assertNotNull(getSpout(TICK_COMPONENT));
        Assert.assertNotNull(getSpout(QUERY_COMPONENT));
        Assert.assertNotNull(getBolt(FILTER_COMPONENT));
        Assert.assertNotNull(getBolt(JOIN_COMPONENT));
        Assert.assertNotNull(getBolt(RESULT_COMPONENT));
        Assert.assertNotNull(getBolt(LOOP_COMPONENT));
        Assert.assertNotNull(getBolt(REPLAY_COMPONENT));
    }

    @Test
    public void testHookingInCustomMetricsConsumer() {
        builder.setSpout("source", new CustomIRichSpout(), 10);
        config.set(BulletStormConfig.TOPOLOGY_METRICS_ENABLE, true);
        config.set(BulletStormConfig.TOPOLOGY_METRICS_CLASSES, singletonList(CustomIMetricsConsumer.class.getName()));

        Assert.assertNull(config.get(CustomIMetricsConsumer.CUSTOM_METRICS_REGISTERED));
        Assert.assertNull(config.get(CustomIMetricsConsumer.CUSTOM_METRICS_V2_ENABLED));
        Assert.assertFalse(builder.isTopologyCreated());

        submitWithTopology("source");

        Assert.assertTrue(builder.isTopologyCreated());
        // This value is the value of the v2 metrics tick setting in the storm config
        Assert.assertTrue((Boolean) config.get(CustomIMetricsConsumer.CUSTOM_METRICS_V2_ENABLED));
        Assert.assertTrue((Boolean) config.get(CustomIMetricsConsumer.CUSTOM_METRICS_REGISTERED));
    }

    @Test
    public void testGetHashIndex() {
        Random random = new Random();
        for (int i = 0; i < NUM_TESTS; i++) {
            int index = StormUtils.getHashIndex(random.nextInt(), HASH_COUNT);
            Assert.assertTrue(0 <= index && index < HASH_COUNT);
        }
    }
}
