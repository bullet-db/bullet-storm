/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.yahoo.bullet.storm.testing.CustomBoltDeclarer;
import com.yahoo.bullet.storm.testing.CustomIMetricsConsumer;
import com.yahoo.bullet.storm.testing.CustomIRichSpout;
import com.yahoo.bullet.storm.testing.CustomSpoutDeclarer;
import com.yahoo.bullet.storm.testing.CustomTopologyBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.yahoo.bullet.storm.TopologyConstants.DATA_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.FEEDBACK_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.FILTER_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.ID_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.JOIN_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.LOOP_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.METADATA_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.QUERY_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.QUERY_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.RESULT_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.RESULT_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.TICK_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.TICK_STREAM;
import static java.util.Collections.singletonList;

public class StormUtilsTest {
    private CustomTopologyBuilder builder;
    private BulletStormConfig config;

    private void submitWithTopology(String recordComponent) {
        try {
            StormUtils.submit(config, recordComponent, builder);
        } catch (Exception ignored) {
        }
    }

    private void submitWithConfig(BulletStormConfig config) {
        try {
            StormUtils.submit(config, builder);
        } catch (Exception ignored) {
        }
    }

    private CustomBoltDeclarer getBolt(String name)  {
        return builder.getCreatedBolts().stream().filter(b -> name.equals(b.getId())).findFirst().get();
    }

    private CustomSpoutDeclarer getSpout(String name)  {
        return builder.getCreatedSpouts().stream().filter(s -> name.equals(s.getId())).findFirst().get();
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

    @BeforeMethod
    public void setup() {
        builder = new CustomTopologyBuilder();
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

        CustomSpoutDeclarer tickSpout = getSpout(TopologyConstants.TICK_COMPONENT);
        Assert.assertNotNull(tickSpout);
        Assert.assertEquals(tickSpout.getSpout().getClass(), TickSpout.class);
        Assert.assertEquals(tickSpout.getParallelism(), BulletStormConfig.TICK_SPOUT_PARALLELISM);

        CustomSpoutDeclarer querySpout = getSpout(TopologyConstants.QUERY_COMPONENT);
        Assert.assertNotNull(querySpout);
        Assert.assertEquals(querySpout.getSpout().getClass(), QuerySpout.class);
        Assert.assertEquals(querySpout.getParallelism(), BulletStormConfig.DEFAULT_QUERY_SPOUT_PARALLELISM);

        CustomBoltDeclarer filterBolt = getBolt(FILTER_COMPONENT);
        Assert.assertNotNull(filterBolt);
        Assert.assertEquals(filterBolt.getBolt().getClass(), FilterBolt.class);
        Assert.assertEquals(filterBolt.getParallelism(), BulletStormConfig.DEFAULT_FILTER_BOLT_PARALLELISM);
        List<Pair<String, String>> filterAllGroupings = filterBolt.getAllGroupings();
        Assert.assertEquals(filterAllGroupings.size(), 3);
        assertContains(filterAllGroupings, TICK_COMPONENT, TICK_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, QUERY_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, METADATA_STREAM);
        List<Pair<String, String>> filterShuffleGroupings = filterBolt.getShuffleGroupings();
        assertContains(filterShuffleGroupings, "source", Utils.DEFAULT_STREAM_ID);
        Map<Pair<String, String>, List<Fields>> filterFieldGroupings = filterBolt.getFieldsGroupings();
        Assert.assertTrue(filterFieldGroupings.isEmpty());

        CustomBoltDeclarer joinBolt = getBolt(JOIN_COMPONENT);
        Assert.assertNotNull(joinBolt);
        Assert.assertEquals(joinBolt.getBolt().getClass(), JoinBolt.class);
        Assert.assertEquals(joinBolt.getParallelism(), BulletStormConfig.DEFAULT_JOIN_BOLT_PARALLELISM);
        List<Pair<String, String>> joinAllGroupings = joinBolt.getAllGroupings();
        Assert.assertEquals(joinAllGroupings.size(), 1);
        assertContains(joinAllGroupings, TICK_COMPONENT, TICK_STREAM);
        List<Pair<String, String>> joinShuffleGroupings = joinBolt.getShuffleGroupings();
        Assert.assertTrue(joinShuffleGroupings.isEmpty());
        Map<Pair<String, String>, List<Fields>> joinFieldGroupings = joinBolt.getFieldsGroupings();
        Assert.assertEquals(joinFieldGroupings.size(), 3);
        assertContains(joinFieldGroupings, QUERY_COMPONENT, QUERY_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, QUERY_COMPONENT, METADATA_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, FILTER_COMPONENT, DATA_STREAM, new Fields(ID_FIELD));

        CustomBoltDeclarer resultBolt = getBolt(RESULT_COMPONENT);
        Assert.assertNotNull(resultBolt);
        Assert.assertEquals(resultBolt.getBolt().getClass(), ResultBolt.class);
        Assert.assertEquals(resultBolt.getParallelism(), BulletStormConfig.DEFAULT_RESULT_BOLT_PARALLELISM);
        List<Pair<String, String>> resultAllGroupings = resultBolt.getAllGroupings();
        Assert.assertTrue(resultAllGroupings.isEmpty());
        List<Pair<String, String>> resultShuffleGroupings = resultBolt.getShuffleGroupings();
        Assert.assertEquals(resultShuffleGroupings.size(), 1);
        assertContains(resultShuffleGroupings, JOIN_COMPONENT, RESULT_STREAM);
        Map<Pair<String, String>, List<Fields>> resultFieldGroupings = resultBolt.getFieldsGroupings();
        Assert.assertTrue(resultFieldGroupings.isEmpty());

        CustomBoltDeclarer loopBolt = getBolt(LOOP_COMPONENT);
        Assert.assertNotNull(loopBolt);
        Assert.assertEquals(loopBolt.getBolt().getClass(), LoopBolt.class);
        Assert.assertEquals(loopBolt.getParallelism(), BulletStormConfig.DEFAULT_LOOP_BOLT_PARALLELISM);
        List<Pair<String, String>> loopAllGroupings = loopBolt.getAllGroupings();
        Assert.assertTrue(loopAllGroupings.isEmpty());
        List<Pair<String, String>> loopShuffleGroupings = loopBolt.getShuffleGroupings();
        Assert.assertEquals(loopShuffleGroupings.size(), 1);
        assertContains(loopShuffleGroupings, JOIN_COMPONENT, FEEDBACK_STREAM);
        Map<Pair<String, String>, List<Fields>> loopFieldGroupings = loopBolt.getFieldsGroupings();
        Assert.assertTrue(loopFieldGroupings.isEmpty());
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

        CustomSpoutDeclarer source = getSpout(TopologyConstants.RECORD_COMPONENT);
        Assert.assertNotNull(source);
        Assert.assertEquals(source.getSpout().getClass(), DSLSpout.class);
        Assert.assertEquals(source.getParallelism(), BulletStormConfig.DEFAULT_DSL_SPOUT_PARALLELISM);

        CustomSpoutDeclarer tickSpout = getSpout(TopologyConstants.TICK_COMPONENT);
        Assert.assertNotNull(tickSpout);
        Assert.assertEquals(tickSpout.getSpout().getClass(), TickSpout.class);
        Assert.assertEquals(tickSpout.getParallelism(), BulletStormConfig.TICK_SPOUT_PARALLELISM);

        CustomSpoutDeclarer querySpout = getSpout(TopologyConstants.QUERY_COMPONENT);
        Assert.assertNotNull(querySpout);
        Assert.assertEquals(querySpout.getSpout().getClass(), QuerySpout.class);
        Assert.assertEquals(querySpout.getParallelism(), BulletStormConfig.DEFAULT_QUERY_SPOUT_PARALLELISM);

        CustomBoltDeclarer filterBolt = getBolt(FILTER_COMPONENT);
        Assert.assertNotNull(filterBolt);
        Assert.assertEquals(filterBolt.getBolt().getClass(), FilterBolt.class);
        Assert.assertEquals(filterBolt.getParallelism(), BulletStormConfig.DEFAULT_FILTER_BOLT_PARALLELISM);
        List<Pair<String, String>> filterAllGroupings = filterBolt.getAllGroupings();
        Assert.assertEquals(filterAllGroupings.size(), 3);
        assertContains(filterAllGroupings, TICK_COMPONENT, TICK_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, QUERY_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, METADATA_STREAM);
        List<Pair<String, String>> filterShuffleGroupings = filterBolt.getShuffleGroupings();
        assertContains(filterShuffleGroupings, TopologyConstants.RECORD_COMPONENT, Utils.DEFAULT_STREAM_ID);
        Map<Pair<String, String>, List<Fields>> filterFieldGroupings = filterBolt.getFieldsGroupings();
        Assert.assertTrue(filterFieldGroupings.isEmpty());

        CustomBoltDeclarer joinBolt = getBolt(JOIN_COMPONENT);
        Assert.assertNotNull(joinBolt);
        Assert.assertEquals(joinBolt.getBolt().getClass(), JoinBolt.class);
        Assert.assertEquals(joinBolt.getParallelism(), BulletStormConfig.DEFAULT_JOIN_BOLT_PARALLELISM);
        List<Pair<String, String>> joinAllGroupings = joinBolt.getAllGroupings();
        Assert.assertEquals(joinAllGroupings.size(), 1);
        assertContains(joinAllGroupings, TICK_COMPONENT, TICK_STREAM);
        List<Pair<String, String>> joinShuffleGroupings = joinBolt.getShuffleGroupings();
        Assert.assertTrue(joinShuffleGroupings.isEmpty());
        Map<Pair<String, String>, List<Fields>> joinFieldGroupings = joinBolt.getFieldsGroupings();
        Assert.assertEquals(joinFieldGroupings.size(), 3);
        assertContains(joinFieldGroupings, QUERY_COMPONENT, QUERY_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, QUERY_COMPONENT, METADATA_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, FILTER_COMPONENT, DATA_STREAM, new Fields(ID_FIELD));

        CustomBoltDeclarer resultBolt = getBolt(RESULT_COMPONENT);
        Assert.assertNotNull(resultBolt);
        Assert.assertEquals(resultBolt.getBolt().getClass(), ResultBolt.class);
        Assert.assertEquals(resultBolt.getParallelism(), BulletStormConfig.DEFAULT_RESULT_BOLT_PARALLELISM);
        List<Pair<String, String>> resultAllGroupings = resultBolt.getAllGroupings();
        Assert.assertTrue(resultAllGroupings.isEmpty());
        List<Pair<String, String>> resultShuffleGroupings = resultBolt.getShuffleGroupings();
        Assert.assertEquals(resultShuffleGroupings.size(), 1);
        assertContains(resultShuffleGroupings, JOIN_COMPONENT, RESULT_STREAM);
        Map<Pair<String, String>, List<Fields>> resultFieldGroupings = resultBolt.getFieldsGroupings();
        Assert.assertTrue(resultFieldGroupings.isEmpty());

        CustomBoltDeclarer loopBolt = getBolt(LOOP_COMPONENT);
        Assert.assertNotNull(loopBolt);
        Assert.assertEquals(loopBolt.getBolt().getClass(), LoopBolt.class);
        Assert.assertEquals(loopBolt.getParallelism(), BulletStormConfig.DEFAULT_LOOP_BOLT_PARALLELISM);
        List<Pair<String, String>> loopAllGroupings = loopBolt.getAllGroupings();
        Assert.assertTrue(loopAllGroupings.isEmpty());
        List<Pair<String, String>> loopShuffleGroupings = loopBolt.getShuffleGroupings();
        Assert.assertEquals(loopShuffleGroupings.size(), 1);
        assertContains(loopShuffleGroupings, JOIN_COMPONENT, FEEDBACK_STREAM);
        Map<Pair<String, String>, List<Fields>> loopFieldGroupings = loopBolt.getFieldsGroupings();
        Assert.assertTrue(loopFieldGroupings.isEmpty());
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

        CustomSpoutDeclarer data = getSpout(TopologyConstants.DATA_COMPONENT);
        Assert.assertNotNull(data);
        Assert.assertEquals(data.getSpout().getClass(), DSLSpout.class);
        Assert.assertEquals(data.getParallelism(), BulletStormConfig.DEFAULT_DSL_SPOUT_PARALLELISM);

        CustomBoltDeclarer source = getBolt(TopologyConstants.RECORD_COMPONENT);
        Assert.assertNotNull(source);
        Assert.assertEquals(source.getBolt().getClass(), DSLBolt.class);
        Assert.assertEquals(source.getParallelism(), BulletStormConfig.DEFAULT_DSL_BOLT_PARALLELISM);
        List<Pair<String, String>> sourceShuffleGroupings = source.getShuffleGroupings();
        assertContains(sourceShuffleGroupings, TopologyConstants.DATA_COMPONENT, Utils.DEFAULT_STREAM_ID);

        CustomSpoutDeclarer tickSpout = getSpout(TopologyConstants.TICK_COMPONENT);
        Assert.assertNotNull(tickSpout);
        Assert.assertEquals(tickSpout.getSpout().getClass(), TickSpout.class);
        Assert.assertEquals(tickSpout.getParallelism(), BulletStormConfig.TICK_SPOUT_PARALLELISM);

        CustomSpoutDeclarer querySpout = getSpout(TopologyConstants.QUERY_COMPONENT);
        Assert.assertNotNull(querySpout);
        Assert.assertEquals(querySpout.getSpout().getClass(), QuerySpout.class);
        Assert.assertEquals(querySpout.getParallelism(), BulletStormConfig.DEFAULT_QUERY_SPOUT_PARALLELISM);

        CustomBoltDeclarer filterBolt = getBolt(FILTER_COMPONENT);
        Assert.assertNotNull(filterBolt);
        Assert.assertEquals(filterBolt.getBolt().getClass(), FilterBolt.class);
        Assert.assertEquals(filterBolt.getParallelism(), BulletStormConfig.DEFAULT_FILTER_BOLT_PARALLELISM);
        List<Pair<String, String>> filterAllGroupings = filterBolt.getAllGroupings();
        Assert.assertEquals(filterAllGroupings.size(), 3);
        assertContains(filterAllGroupings, TICK_COMPONENT, TICK_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, QUERY_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, METADATA_STREAM);
        List<Pair<String, String>> filterShuffleGroupings = filterBolt.getShuffleGroupings();
        assertContains(filterShuffleGroupings, TopologyConstants.RECORD_COMPONENT, Utils.DEFAULT_STREAM_ID);
        Map<Pair<String, String>, List<Fields>> filterFieldGroupings = filterBolt.getFieldsGroupings();
        Assert.assertTrue(filterFieldGroupings.isEmpty());

        CustomBoltDeclarer joinBolt = getBolt(JOIN_COMPONENT);
        Assert.assertNotNull(joinBolt);
        Assert.assertEquals(joinBolt.getBolt().getClass(), JoinBolt.class);
        Assert.assertEquals(joinBolt.getParallelism(), BulletStormConfig.DEFAULT_JOIN_BOLT_PARALLELISM);
        List<Pair<String, String>> joinAllGroupings = joinBolt.getAllGroupings();
        Assert.assertEquals(joinAllGroupings.size(), 1);
        assertContains(joinAllGroupings, TICK_COMPONENT, TICK_STREAM);
        List<Pair<String, String>> joinShuffleGroupings = joinBolt.getShuffleGroupings();
        Assert.assertTrue(joinShuffleGroupings.isEmpty());
        Map<Pair<String, String>, List<Fields>> joinFieldGroupings = joinBolt.getFieldsGroupings();
        Assert.assertEquals(joinFieldGroupings.size(), 3);
        assertContains(joinFieldGroupings, QUERY_COMPONENT, QUERY_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, QUERY_COMPONENT, METADATA_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, FILTER_COMPONENT, DATA_STREAM, new Fields(ID_FIELD));

        CustomBoltDeclarer resultBolt = getBolt(RESULT_COMPONENT);
        Assert.assertNotNull(resultBolt);
        Assert.assertEquals(resultBolt.getBolt().getClass(), ResultBolt.class);
        Assert.assertEquals(resultBolt.getParallelism(), BulletStormConfig.DEFAULT_RESULT_BOLT_PARALLELISM);
        List<Pair<String, String>> resultAllGroupings = resultBolt.getAllGroupings();
        Assert.assertTrue(resultAllGroupings.isEmpty());
        List<Pair<String, String>> resultShuffleGroupings = resultBolt.getShuffleGroupings();
        Assert.assertEquals(resultShuffleGroupings.size(), 1);
        assertContains(resultShuffleGroupings, JOIN_COMPONENT, RESULT_STREAM);
        Map<Pair<String, String>, List<Fields>> resultFieldGroupings = resultBolt.getFieldsGroupings();
        Assert.assertTrue(resultFieldGroupings.isEmpty());

        CustomBoltDeclarer loopBolt = getBolt(LOOP_COMPONENT);
        Assert.assertNotNull(loopBolt);
        Assert.assertEquals(loopBolt.getBolt().getClass(), LoopBolt.class);
        Assert.assertEquals(loopBolt.getParallelism(), BulletStormConfig.DEFAULT_LOOP_BOLT_PARALLELISM);
        List<Pair<String, String>> loopAllGroupings = loopBolt.getAllGroupings();
        Assert.assertTrue(loopAllGroupings.isEmpty());
        List<Pair<String, String>> loopShuffleGroupings = loopBolt.getShuffleGroupings();
        Assert.assertEquals(loopShuffleGroupings.size(), 1);
        assertContains(loopShuffleGroupings, JOIN_COMPONENT, FEEDBACK_STREAM);
        Map<Pair<String, String>, List<Fields>> loopFieldGroupings = loopBolt.getFieldsGroupings();
        Assert.assertTrue(loopFieldGroupings.isEmpty());
    }

    @Test
    public void testHookingInBulletSpout() {
        config.set(BulletStormConfig.BULLET_SPOUT_CLASS_NAME, CustomIRichSpout.class.getName());

        Assert.assertFalse(builder.isTopologyCreated());
        submitWithConfig(config);

        Assert.assertTrue(builder.isTopologyCreated());

        Assert.assertEquals(builder.getCreatedSpouts().size(), 3);
        Assert.assertEquals(builder.getCreatedBolts().size(), 4);

        CustomSpoutDeclarer source = getSpout(TopologyConstants.RECORD_COMPONENT);
        Assert.assertNotNull(source);
        Assert.assertEquals(source.getSpout().getClass(), CustomIRichSpout.class);
        Assert.assertEquals(source.getParallelism(), BulletStormConfig.DEFAULT_BULLET_SPOUT_PARALLELISM);

        CustomSpoutDeclarer tickSpout = getSpout(TopologyConstants.TICK_COMPONENT);
        Assert.assertNotNull(tickSpout);
        Assert.assertEquals(tickSpout.getSpout().getClass(), TickSpout.class);
        Assert.assertEquals(tickSpout.getParallelism(), BulletStormConfig.TICK_SPOUT_PARALLELISM);

        CustomSpoutDeclarer querySpout = getSpout(TopologyConstants.QUERY_COMPONENT);
        Assert.assertNotNull(querySpout);
        Assert.assertEquals(querySpout.getSpout().getClass(), QuerySpout.class);
        Assert.assertEquals(querySpout.getParallelism(), BulletStormConfig.DEFAULT_QUERY_SPOUT_PARALLELISM);

        CustomBoltDeclarer filterBolt = getBolt(FILTER_COMPONENT);
        Assert.assertNotNull(filterBolt);
        Assert.assertEquals(filterBolt.getBolt().getClass(), FilterBolt.class);
        Assert.assertEquals(filterBolt.getParallelism(), BulletStormConfig.DEFAULT_FILTER_BOLT_PARALLELISM);
        List<Pair<String, String>> filterAllGroupings = filterBolt.getAllGroupings();
        Assert.assertEquals(filterAllGroupings.size(), 3);
        assertContains(filterAllGroupings, TICK_COMPONENT, TICK_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, QUERY_STREAM);
        assertContains(filterAllGroupings, QUERY_COMPONENT, METADATA_STREAM);
        List<Pair<String, String>> filterShuffleGroupings = filterBolt.getShuffleGroupings();
        assertContains(filterShuffleGroupings, TopologyConstants.RECORD_COMPONENT, Utils.DEFAULT_STREAM_ID);
        Map<Pair<String, String>, List<Fields>> filterFieldGroupings = filterBolt.getFieldsGroupings();
        Assert.assertTrue(filterFieldGroupings.isEmpty());

        CustomBoltDeclarer joinBolt = getBolt(JOIN_COMPONENT);
        Assert.assertNotNull(joinBolt);
        Assert.assertEquals(joinBolt.getBolt().getClass(), JoinBolt.class);
        Assert.assertEquals(joinBolt.getParallelism(), BulletStormConfig.DEFAULT_JOIN_BOLT_PARALLELISM);
        List<Pair<String, String>> joinAllGroupings = joinBolt.getAllGroupings();
        Assert.assertEquals(joinAllGroupings.size(), 1);
        assertContains(joinAllGroupings, TICK_COMPONENT, TICK_STREAM);
        List<Pair<String, String>> joinShuffleGroupings = joinBolt.getShuffleGroupings();
        Assert.assertTrue(joinShuffleGroupings.isEmpty());
        Map<Pair<String, String>, List<Fields>> joinFieldGroupings = joinBolt.getFieldsGroupings();
        Assert.assertEquals(joinFieldGroupings.size(), 3);
        assertContains(joinFieldGroupings, QUERY_COMPONENT, QUERY_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, QUERY_COMPONENT, METADATA_STREAM, new Fields(ID_FIELD));
        assertContains(joinFieldGroupings, FILTER_COMPONENT, DATA_STREAM, new Fields(ID_FIELD));

        CustomBoltDeclarer resultBolt = getBolt(RESULT_COMPONENT);
        Assert.assertNotNull(resultBolt);
        Assert.assertEquals(resultBolt.getBolt().getClass(), ResultBolt.class);
        Assert.assertEquals(resultBolt.getParallelism(), BulletStormConfig.DEFAULT_RESULT_BOLT_PARALLELISM);
        List<Pair<String, String>> resultAllGroupings = resultBolt.getAllGroupings();
        Assert.assertTrue(resultAllGroupings.isEmpty());
        List<Pair<String, String>> resultShuffleGroupings = resultBolt.getShuffleGroupings();
        Assert.assertEquals(resultShuffleGroupings.size(), 1);
        assertContains(resultShuffleGroupings, JOIN_COMPONENT, RESULT_STREAM);
        Map<Pair<String, String>, List<Fields>> resultFieldGroupings = resultBolt.getFieldsGroupings();
        Assert.assertTrue(resultFieldGroupings.isEmpty());

        CustomBoltDeclarer loopBolt = getBolt(LOOP_COMPONENT);
        Assert.assertNotNull(loopBolt);
        Assert.assertEquals(loopBolt.getBolt().getClass(), LoopBolt.class);
        Assert.assertEquals(loopBolt.getParallelism(), BulletStormConfig.DEFAULT_LOOP_BOLT_PARALLELISM);
        List<Pair<String, String>> loopAllGroupings = loopBolt.getAllGroupings();
        Assert.assertTrue(loopAllGroupings.isEmpty());
        List<Pair<String, String>> loopShuffleGroupings = loopBolt.getShuffleGroupings();
        Assert.assertEquals(loopShuffleGroupings.size(), 1);
        assertContains(loopShuffleGroupings, JOIN_COMPONENT, FEEDBACK_STREAM);
        Map<Pair<String, String>, List<Fields>> loopFieldGroupings = loopBolt.getFieldsGroupings();
        Assert.assertTrue(loopFieldGroupings.isEmpty());
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
    }

    @Test
    public void testHookingInCustomMetricsConsumer() {
        builder.setSpout("source", new CustomIRichSpout(), 10);
        config.set(BulletStormConfig.TOPOLOGY_METRICS_ENABLE, true);
        config.set(BulletStormConfig.TOPOLOGY_METRICS_CLASSES, singletonList(CustomIMetricsConsumer.class.getName()));

        Assert.assertNull(config.get(CustomIMetricsConsumer.CUSTOM_METRICS_REGISTERED));
        Assert.assertFalse(builder.isTopologyCreated());
        submitWithTopology("source");

        Assert.assertTrue(builder.isTopologyCreated());
        Assert.assertTrue((Boolean) config.get(CustomIMetricsConsumer.CUSTOM_METRICS_REGISTERED));
    }
}
