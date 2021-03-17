/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.storm.grouping.IDGrouping;
import com.yahoo.bullet.storm.grouping.TaskIndexCaptureGrouping;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.List;
import java.util.Objects;

import static com.yahoo.bullet.storm.TopologyConstants.CAPTURE_STREAM;
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
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.REPLAY_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.RESULT_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.TICK_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.TICK_STREAM;

@Slf4j
@SuppressWarnings("unchecked")
public class StormUtils {
    private static final int POSITIVE_INT_MASK = 0x7FFFFFFF;
    public static final String HYPHEN = "-";

    /**
     * This function can be used to wire up the source of the records to Bullet. The name of the last component in your
     * topology and the {@link TopologyBuilder} used to create your topology should be provided. That topology
     * will be wired up with Bullet reading from your component that produces the {@link com.yahoo.bullet.record.BulletRecord}.
     *
     * @param bulletConfig The non-null, validated {@link BulletStormConfig} that contains the necessary configuration.
     * @param recordComponent The non-null name of the component used in your topology that is the source of records for Bullet.
     * @param builder The non-null {@link TopologyBuilder} that was used to create your topology.
     * @param stormConfig The non-null Storm {@link Config} to use to submit the topology with.
     * @throws Exception if there were issues creating the topology.
     */
    public static void submit(BulletStormConfig bulletConfig, String recordComponent, TopologyBuilder builder, Config stormConfig) throws Exception {
        Objects.requireNonNull(bulletConfig);
        Objects.requireNonNull(recordComponent);
        Objects.requireNonNull(builder);
        Objects.requireNonNull(stormConfig);

        String name = bulletConfig.getAs(BulletStormConfig.TOPOLOGY_NAME, String.class);

        Number querySpoutParallelism = bulletConfig.getAs(BulletStormConfig.QUERY_SPOUT_PARALLELISM, Number.class);
        Number querySpoutCPULoad = bulletConfig.getAs(BulletStormConfig.QUERY_SPOUT_CPU_LOAD, Number.class);
        Number querySpoutMemoryOnHeapLoad = bulletConfig.getAs(BulletStormConfig.QUERY_SPOUT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number querySpoutMemoryOffHeapLoad = bulletConfig.getAs(BulletStormConfig.QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD, Number.class);

        // Tick parallelism must be 1 otherwise multiple ticks will get delivered to a component
        Number tickSpoutParallelism = BulletStormConfig.TICK_SPOUT_PARALLELISM;
        Number tickSpoutCPULoad = bulletConfig.getAs(BulletStormConfig.TICK_SPOUT_CPU_LOAD, Number.class);
        Number tickSpoutMemoryOnheapLoad = bulletConfig.getAs(BulletStormConfig.TICK_SPOUT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number tickSpoutMemoryOffHeapLoad = bulletConfig.getAs(BulletStormConfig.TICK_SPOUT_MEMORY_OFF_HEAP_LOAD, Number.class);

        Number filterBoltParallelism = bulletConfig.getAs(BulletStormConfig.FILTER_BOLT_PARALLELISM, Number.class);
        Number filterBoltCPULoad = bulletConfig.getAs(BulletStormConfig.FILTER_BOLT_CPU_LOAD, Number.class);
        Number filterBoltMemoryOnheapLoad = bulletConfig.getAs(BulletStormConfig.FILTER_BOLT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number filterBoltMemoryOffHeapLoad = bulletConfig.getAs(BulletStormConfig.FILTER_BOLT_MEMORY_OFF_HEAP_LOAD, Number.class);

        Number joinBoltParallelism = bulletConfig.getAs(BulletStormConfig.JOIN_BOLT_PARALLELISM, Number.class);
        Number joinBoltCPULoad = bulletConfig.getAs(BulletStormConfig.JOIN_BOLT_CPU_LOAD, Number.class);
        Number joinBoltMemoryOnHeapLoad = bulletConfig.getAs(BulletStormConfig.JOIN_BOLT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number joinBoltMemoryOffHeapLoad = bulletConfig.getAs(BulletStormConfig.JOIN_BOLT_MEMORY_OFF_HEAP_LOAD, Number.class);

        Number resultBoltParallelism = bulletConfig.getAs(BulletStormConfig.RESULT_BOLT_PARALLELISM, Number.class);
        Number resultBoltCPULoad = bulletConfig.getAs(BulletStormConfig.RESULT_BOLT_CPU_LOAD, Number.class);
        Number resultBoltMemoryOnHeapLoad = bulletConfig.getAs(BulletStormConfig.RESULT_BOLT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number resultBoltMemoryOffHeapLoad = bulletConfig.getAs(BulletStormConfig.RESULT_BOLT_MEMORY_OFF_HEAP_LOAD, Number.class);

        Number loopBoltParallelism = bulletConfig.getAs(BulletStormConfig.LOOP_BOLT_PARALLELISM, Number.class);
        Number loopBoltCPULoad = bulletConfig.getAs(BulletStormConfig.LOOP_BOLT_CPU_LOAD, Number.class);
        Number loopBoltMemoryOnHeapLoad = bulletConfig.getAs(BulletStormConfig.LOOP_BOLT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number loopBoltMemoryOffHeapLoad = bulletConfig.getAs(BulletStormConfig.LOOP_BOLT_MEMORY_OFF_HEAP_LOAD, Number.class);

        Number replayBoltParallelism = bulletConfig.getAs(BulletStormConfig.REPLAY_BOLT_PARALLELISM, Number.class);
        Number replayBoltCPULoad = bulletConfig.getAs(BulletStormConfig.REPLAY_BOLT_CPU_LOAD, Number.class);
        Number replayBoltMemoryOnHeapLoad = bulletConfig.getAs(BulletStormConfig.REPLAY_BOLT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number replayBoltMemoryOffHeapLoad = bulletConfig.getAs(BulletStormConfig.REPLAY_BOLT_MEMORY_OFF_HEAP_LOAD, Number.class);

        boolean isWindowingDisabled = bulletConfig.getAs(BulletConfig.WINDOW_DISABLE, Boolean.class);
        boolean isReplayEnabled = bulletConfig.getAs(BulletStormConfig.REPLAY_ENABLE, Boolean.class);

        builder.setSpout(QUERY_COMPONENT, new QuerySpout(bulletConfig), querySpoutParallelism)
               .setCPULoad(querySpoutCPULoad).setMemoryLoad(querySpoutMemoryOnHeapLoad, querySpoutMemoryOffHeapLoad);

        builder.setSpout(TICK_COMPONENT, new TickSpout(bulletConfig), tickSpoutParallelism)
               .setCPULoad(tickSpoutCPULoad).setMemoryLoad(tickSpoutMemoryOnheapLoad, tickSpoutMemoryOffHeapLoad);

        // Hook in the source of the BulletRecords
        if (isReplayEnabled) {
            builder.setBolt(FILTER_COMPONENT, new FilterBolt(recordComponent, bulletConfig), filterBoltParallelism)
                   .shuffleGrouping(recordComponent)
                   .allGrouping(QUERY_COMPONENT, QUERY_STREAM)
                   .allGrouping(QUERY_COMPONENT, METADATA_STREAM)
                   .directGrouping(REPLAY_COMPONENT, REPLAY_STREAM)
                   .allGrouping(TICK_COMPONENT, TICK_STREAM)
                   .setCPULoad(filterBoltCPULoad).setMemoryLoad(filterBoltMemoryOnheapLoad, filterBoltMemoryOffHeapLoad);

            builder.setBolt(JOIN_COMPONENT, new JoinBolt(bulletConfig), joinBoltParallelism)
                   .customGrouping(QUERY_COMPONENT, QUERY_STREAM, new IDGrouping())
                   .allGrouping(QUERY_COMPONENT, METADATA_STREAM)
                   .customGrouping(FILTER_COMPONENT, DATA_STREAM, new IDGrouping())
                   .customGrouping(FILTER_COMPONENT, ERROR_STREAM, new IDGrouping())
                   .customGrouping(REPLAY_COMPONENT, CAPTURE_STREAM, new TaskIndexCaptureGrouping())
                   .directGrouping(REPLAY_COMPONENT, REPLAY_STREAM)
                   .allGrouping(TICK_COMPONENT, TICK_STREAM)
                   .setCPULoad(joinBoltCPULoad).setMemoryLoad(joinBoltMemoryOnHeapLoad, joinBoltMemoryOffHeapLoad);

            builder.setBolt(REPLAY_COMPONENT, new ReplayBolt(bulletConfig), replayBoltParallelism)
                   .allGrouping(QUERY_COMPONENT, QUERY_STREAM)
                   .allGrouping(QUERY_COMPONENT, METADATA_STREAM)
                   .fieldsGrouping(QUERY_COMPONENT, REPLAY_STREAM, new Fields(ID_FIELD))
                   .setCPULoad(replayBoltCPULoad).setMemoryLoad(replayBoltMemoryOnHeapLoad, replayBoltMemoryOffHeapLoad);
        } else {
            builder.setBolt(FILTER_COMPONENT, new FilterBolt(recordComponent, bulletConfig), filterBoltParallelism)
                   .shuffleGrouping(recordComponent)
                   .allGrouping(QUERY_COMPONENT, QUERY_STREAM)
                   .allGrouping(QUERY_COMPONENT, METADATA_STREAM)
                   .allGrouping(TICK_COMPONENT, TICK_STREAM)
                   .setCPULoad(filterBoltCPULoad).setMemoryLoad(filterBoltMemoryOnheapLoad, filterBoltMemoryOffHeapLoad);

            builder.setBolt(JOIN_COMPONENT, new JoinBolt(bulletConfig), joinBoltParallelism)
                   .fieldsGrouping(QUERY_COMPONENT, QUERY_STREAM, new Fields(ID_FIELD))
                   .fieldsGrouping(QUERY_COMPONENT, METADATA_STREAM, new Fields(ID_FIELD))
                   .fieldsGrouping(FILTER_COMPONENT, DATA_STREAM, new Fields(ID_FIELD))
                   .fieldsGrouping(FILTER_COMPONENT, ERROR_STREAM, new Fields(ID_FIELD))
                   .allGrouping(TICK_COMPONENT, TICK_STREAM)
                   .setCPULoad(joinBoltCPULoad).setMemoryLoad(joinBoltMemoryOnHeapLoad, joinBoltMemoryOffHeapLoad);
        }

        builder.setBolt(TopologyConstants.RESULT_COMPONENT, new ResultBolt(bulletConfig), resultBoltParallelism)
               .shuffleGrouping(JOIN_COMPONENT, RESULT_STREAM)
               .setCPULoad(resultBoltCPULoad).setMemoryLoad(resultBoltMemoryOnHeapLoad, resultBoltMemoryOffHeapLoad);

        // Hook in the Loop Bolt only if windowing or replay is enabled
        if (isWindowingDisabled && !isReplayEnabled) {
            log.info("Windowing and replay are disabled. Skipping hooking in the Loop Bolt...");
        } else {
            builder.setBolt(LOOP_COMPONENT, new LoopBolt(bulletConfig), loopBoltParallelism)
                   .shuffleGrouping(FILTER_COMPONENT, FEEDBACK_STREAM)
                   .shuffleGrouping(JOIN_COMPONENT, FEEDBACK_STREAM)
                   .setCPULoad(loopBoltCPULoad).setMemoryLoad(loopBoltMemoryOnHeapLoad, loopBoltMemoryOffHeapLoad);
        }

        // Metrics
        Boolean enableMetrics = (Boolean) bulletConfig.get(BulletStormConfig.TOPOLOGY_METRICS_ENABLE);
        if (enableMetrics) {
            stormConfig.put(Config.TOPOLOGY_ENABLE_V2_METRICS_TICK, true);
            List<String> classNames = bulletConfig.getAs(BulletStormConfig.TOPOLOGY_METRICS_CLASSES, List.class);
            classNames.forEach(className -> ReflectionUtils.registerMetricsConsumer(className, stormConfig, bulletConfig));
        }

        // Put the rest of the other possible custom Storm settings without checking their types
        stormConfig.putAll(bulletConfig.getCustomStormSettings());

        StormSubmitter.submitTopology(name, stormConfig, builder.createTopology());
    }
    
    /**
     * This function can be used to wire up the source of the records to Bullet. The name of the last component in your
     * topology and the {@link TopologyBuilder} used to create your topology should be provided. That topology
     * will be wired up with Bullet reading from your component that produces the {@link com.yahoo.bullet.record.BulletRecord}.
     *
     * @param config The non-null, validated {@link BulletStormConfig} that contains the necessary configuration.
     * @param recordComponent The non-null name of the component used in your topology that is the source of records for Bullet.
     * @param builder The non-null {@link TopologyBuilder} that was used to create your topology.
     * @throws Exception if there were issues creating the topology.
     */
    public static void submit(BulletStormConfig config, String recordComponent, TopologyBuilder builder) throws Exception {
        submit(config, recordComponent, builder, new Config());
    }

    private static void addDSLSpout(BulletStormConfig config, TopologyBuilder builder) {
        Number dslSpoutParallelism = config.getAs(BulletStormConfig.DSL_SPOUT_PARALLELISM, Number.class);
        Number dslSpoutCPULoad = config.getAs(BulletStormConfig.DSL_SPOUT_CPU_LOAD, Number.class);
        Number dslSpoutMemoryOnHeapLoad = config.getAs(BulletStormConfig.DSL_SPOUT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number dslSpoutMemoryOffHeapLoad = config.getAs(BulletStormConfig.DSL_SPOUT_MEMORY_OFF_HEAP_LOAD, Number.class);

        Boolean dslConnectorAsSpout = config.getAs(BulletStormConfig.DSL_SPOUT_CONNECTOR_SPOUT_ENABLE, Boolean.class);
        Boolean dslBoltEnable = config.getAs(BulletStormConfig.DSL_BOLT_ENABLE, Boolean.class);

        DSLSpout spout;
        if (dslConnectorAsSpout) {
            String dslConnectorSpoutClass = config.getAs(BulletStormConfig.DSL_SPOUT_CONNECTOR_CLASS_NAME, String.class);
            log.info("Using the SpoutConnector with the spout {} as the DSLSpout", dslConnectorSpoutClass);
            spout = new DSLConnectorSpout(config);
        } else {
            spout = new DSLSpout(config);
        }

        builder.setSpout(dslBoltEnable ? TopologyConstants.DATA_COMPONENT : TopologyConstants.RECORD_COMPONENT, spout, dslSpoutParallelism)
               .setCPULoad(dslSpoutCPULoad)
               .setMemoryLoad(dslSpoutMemoryOnHeapLoad, dslSpoutMemoryOffHeapLoad);

        log.info("Added DSLSpout with Parallelism {}, CPU load {}, On-heap memory {}, Off-heap memory {}",
                 dslSpoutParallelism, dslSpoutCPULoad, dslSpoutMemoryOnHeapLoad, dslSpoutMemoryOffHeapLoad);

        if (dslBoltEnable) {
            Number dslBoltParallelism = config.getAs(BulletStormConfig.DSL_BOLT_PARALLELISM, Number.class);
            Number dslBoltCPULoad = config.getAs(BulletStormConfig.DSL_BOLT_CPU_LOAD, Number.class);
            Number dslBoltMemoryOnHeapLoad = config.getAs(BulletStormConfig.DSL_BOLT_MEMORY_ON_HEAP_LOAD, Number.class);
            Number dslBoltMemoryOffHeapLoad = config.getAs(BulletStormConfig.DSL_BOLT_MEMORY_OFF_HEAP_LOAD, Number.class);

            builder.setBolt(TopologyConstants.RECORD_COMPONENT, new DSLBolt(config), dslBoltParallelism)
                   .shuffleGrouping(TopologyConstants.DATA_COMPONENT)
                   .setCPULoad(dslBoltCPULoad)
                   .setMemoryLoad(dslBoltMemoryOnHeapLoad, dslBoltMemoryOffHeapLoad);

            log.info("Added DSLBolt with Parallelism {}, CPU load {}, On-heap memory {}, Off-heap memory {}",
                     dslBoltParallelism, dslBoltCPULoad, dslBoltMemoryOnHeapLoad, dslBoltMemoryOffHeapLoad);
        }
    }

    private static void addBulletSpout(BulletStormConfig config, TopologyBuilder builder) throws Exception {
        String bulletSpoutClassName = config.getAs(BulletStormConfig.BULLET_SPOUT_CLASS_NAME, String.class);
        List<String> bulletSpoutArgs = config.getAs(BulletStormConfig.BULLET_SPOUT_ARGS, List.class);
        Number bulletSpoutParallelism = config.getAs(BulletStormConfig.BULLET_SPOUT_PARALLELISM, Number.class);
        Number bulletSpoutCPULoad = config.getAs(BulletStormConfig.BULLET_SPOUT_CPU_LOAD, Number.class);
        Number bulletSpoutMemoryOnHeapLoad = config.getAs(BulletStormConfig.BULLET_SPOUT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number bulletSpoutMemoryOffHeapLoad = config.getAs(BulletStormConfig.BULLET_SPOUT_MEMORY_OFF_HEAP_LOAD, Number.class);

        builder.setSpout(TopologyConstants.RECORD_COMPONENT, ReflectionUtils.getSpout(bulletSpoutClassName, bulletSpoutArgs), bulletSpoutParallelism)
               .setCPULoad(bulletSpoutCPULoad)
               .setMemoryLoad(bulletSpoutMemoryOnHeapLoad, bulletSpoutMemoryOffHeapLoad);

        log.info("Added spout with Parallelism {}, CPU load {}, On-heap memory {}, Off-heap memory {}",
                 bulletSpoutParallelism, bulletSpoutCPULoad, bulletSpoutMemoryOnHeapLoad, bulletSpoutMemoryOffHeapLoad);
    }

    /**
     * This submits a topology after loading the configured Spout (and optionally, Bolt), which is either the {@link DSLSpout}
     * and {@link DSLBolt} or a custom Spout and Bolt. The topology is submitted with the given configuration as the source
     * of {@link com.yahoo.bullet.record.BulletRecord} using the given {@link TopologyBuilder}. Takes a {@link Config}
     * if any custom settings were added.
     *
     * @param bulletConfig The Bullet settings for this Bullet topology.
     * @param builder The {@link TopologyBuilder} to use to add the topology to.
     * @param stormConfig The Storm {@link Config} to use for this topology.
     * @throws Exception if there were any issues submitting the topology.
     */
    public static void submit(BulletStormConfig bulletConfig, TopologyBuilder builder, Config stormConfig) throws Exception {
        Boolean dslSpoutEnable = bulletConfig.getAs(BulletStormConfig.DSL_SPOUT_ENABLE, Boolean.class);
        if (dslSpoutEnable) {
            addDSLSpout(bulletConfig, builder);
        } else {
            addBulletSpout(bulletConfig, builder);
        }
        submit(bulletConfig, TopologyConstants.RECORD_COMPONENT, builder, stormConfig);
    }

    /**
     * This submits a topology after loading the configured Spout (and optionally, Bolt), which is either the {@link DSLSpout}
     * and {@link DSLBolt} or a custom Spout and Bolt. The topology is submitted with the given configuration as the source
     * of {@link com.yahoo.bullet.record.BulletRecord} using the given {@link TopologyBuilder}
     *
     * @param config The Bullet settings for this topology.
     * @param builder The {@link TopologyBuilder} to use to add the topology to.
     * @throws Exception if there were any issues submitting the topology.
     */
    public static void submit(BulletStormConfig config, TopologyBuilder builder) throws Exception {
        submit(config, builder, new Config());
    }

    /**
     * Returns an index based on the hash of the key and the hash count.
     *
     * @param key The key that determines the hash.
     * @param count The hash count.
     * @return The index for the given key and hash count.
     */
    public static int getHashIndex(Object key, int count) {
        return (key.hashCode() & POSITIVE_INT_MASK) % count;
    }
}
