/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.common.BulletConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.List;
import java.util.Objects;

import static com.yahoo.bullet.storm.TopologyConstants.DATA_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.FEEDBACK_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.FILTER_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.ID_FIELD;
import static com.yahoo.bullet.storm.TopologyConstants.JOIN_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.LOOP_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.METADATA_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.QUERY_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.QUERY_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.RESULT_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.TICK_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.TICK_STREAM;

@Slf4j
@SuppressWarnings("unchecked")
public class StormUtils {
    /**
     * This function can be used to wire up the source of the records to Bullet. Your source may be as simple
     * as a Spout (in which case, just use the {@link Topology#main(String[])} method with the class name of your Spout.
     * This method is more for wiring up an arbitrary topology to Bullet. The name of last component in your
     * topology and the {@link TopologyBuilder} used to create your topology should be provided. That topology
     * will be wired up with Bullet reading from your component that produces the {@link com.yahoo.bullet.record.BulletRecord}.
     *
     * @param config The non-null, validated {@link BulletStormConfig} that contains the necessary configuration.
     * @param recordComponent The non-null name of the component used in your topology that is the source of records for Bullet.
     * @param builder The non-null {@link TopologyBuilder} that was used to create your topology.
     * @throws Exception if there were issues creating the topology.
     */
    public static void submit(BulletStormConfig config, String recordComponent, TopologyBuilder builder) throws Exception {
        Objects.requireNonNull(config);
        Objects.requireNonNull(recordComponent);
        Objects.requireNonNull(builder);

        String name = config.getAs(BulletStormConfig.TOPOLOGY_NAME, String.class);

        Number querySpoutParallelism = config.getAs(BulletStormConfig.QUERY_SPOUT_PARALLELISM, Number.class);
        Number querySpoutCPULoad = config.getAs(BulletStormConfig.QUERY_SPOUT_CPU_LOAD, Number.class);
        Number querySpoutMemoryOnHeapLoad = config.getAs(BulletStormConfig.QUERY_SPOUT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number querySpoutMemoryOffHeapLoad = config.getAs(BulletStormConfig.QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD, Number.class);

        // Tick parallelism must be 1 otherwise multiple ticks will get delivered to a component
        Number tickSpoutParallelism = BulletStormConfig.TICK_SPOUT_PARALLELISM;
        Number tickSpoutCPULoad = config.getAs(BulletStormConfig.TICK_SPOUT_CPU_LOAD, Number.class);
        Number tickSpoutMemoryOnheapLoad = config.getAs(BulletStormConfig.TICK_SPOUT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number tickSpoutMemoryOffHeapLoad = config.getAs(BulletStormConfig.TICK_SPOUT_MEMORY_OFF_HEAP_LOAD, Number.class);

        Number filterBoltParallelism = config.getAs(BulletStormConfig.FILTER_BOLT_PARALLELISM, Number.class);
        Number filterBoltCPULoad = config.getAs(BulletStormConfig.FILTER_BOLT_CPU_LOAD, Number.class);
        Number filterBoltMemoryOnheapLoad = config.getAs(BulletStormConfig.FILTER_BOLT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number filterBoltMemoryOffHeapLoad = config.getAs(BulletStormConfig.FILTER_BOLT_MEMORY_OFF_HEAP_LOAD, Number.class);

        Number joinBoltParallelism = config.getAs(BulletStormConfig.JOIN_BOLT_PARALLELISM, Number.class);
        Number joinBoltCPULoad = config.getAs(BulletStormConfig.JOIN_BOLT_CPU_LOAD, Number.class);
        Number joinBoltMemoryOnHeapLoad = config.getAs(BulletStormConfig.JOIN_BOLT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number joinBoltMemoryOffHeapLoad = config.getAs(BulletStormConfig.JOIN_BOLT_MEMORY_OFF_HEAP_LOAD, Number.class);

        Number resultBoltParallelism = config.getAs(BulletStormConfig.RESULT_BOLT_PARALLELISM, Number.class);
        Number resultBoltCPULoad = config.getAs(BulletStormConfig.RESULT_BOLT_CPU_LOAD, Number.class);
        Number resultBoltMemoryOnHeapLoad = config.getAs(BulletStormConfig.RESULT_BOLT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number resultBoltMemoryOffHeapLoad = config.getAs(BulletStormConfig.RESULT_BOLT_MEMORY_OFF_HEAP_LOAD, Number.class);

        Number loopBoltParallelism = config.getAs(BulletStormConfig.LOOP_BOLT_PARALLELISM, Number.class);
        Number loopBoltCPULoad = config.getAs(BulletStormConfig.LOOP_BOLT_CPU_LOAD, Number.class);
        Number loopBoltMemoryOnHeapLoad = config.getAs(BulletStormConfig.LOOP_BOLT_MEMORY_ON_HEAP_LOAD, Number.class);
        Number loopBoltMemoryOffHeapLoad = config.getAs(BulletStormConfig.LOOP_BOLT_MEMORY_OFF_HEAP_LOAD, Number.class);

        builder.setSpout(QUERY_COMPONENT, new QuerySpout(config), querySpoutParallelism)
               .setCPULoad(querySpoutCPULoad).setMemoryLoad(querySpoutMemoryOnHeapLoad, querySpoutMemoryOffHeapLoad);

        builder.setSpout(TICK_COMPONENT, new TickSpout(config), tickSpoutParallelism)
               .setCPULoad(tickSpoutCPULoad).setMemoryLoad(tickSpoutMemoryOnheapLoad, tickSpoutMemoryOffHeapLoad);

        // Hook in the source of the BulletRecords
        builder.setBolt(FILTER_COMPONENT, new FilterBolt(recordComponent, config), filterBoltParallelism)
               .shuffleGrouping(recordComponent)
               .allGrouping(QUERY_COMPONENT, QUERY_STREAM)
               .allGrouping(QUERY_COMPONENT, METADATA_STREAM)
               .allGrouping(TICK_COMPONENT, TICK_STREAM)
               .setCPULoad(filterBoltCPULoad).setMemoryLoad(filterBoltMemoryOnheapLoad, filterBoltMemoryOffHeapLoad);

        builder.setBolt(JOIN_COMPONENT, new JoinBolt(config), joinBoltParallelism)
               .fieldsGrouping(QUERY_COMPONENT, QUERY_STREAM, new Fields(ID_FIELD))
               .fieldsGrouping(QUERY_COMPONENT, METADATA_STREAM, new Fields(ID_FIELD))
               .fieldsGrouping(FILTER_COMPONENT, DATA_STREAM, new Fields(ID_FIELD))
               .allGrouping(TICK_COMPONENT, TICK_STREAM)
               .setCPULoad(joinBoltCPULoad).setMemoryLoad(joinBoltMemoryOnHeapLoad, joinBoltMemoryOffHeapLoad);

        builder.setBolt(TopologyConstants.RESULT_COMPONENT, new ResultBolt(config), resultBoltParallelism)
               .shuffleGrouping(JOIN_COMPONENT, RESULT_STREAM)
               .setCPULoad(resultBoltCPULoad).setMemoryLoad(resultBoltMemoryOnHeapLoad, resultBoltMemoryOffHeapLoad);

        // Hook in the Loop Bolt only if windowing is enabled
        boolean isWindowingDisabled = config.getAs(BulletConfig.WINDOW_DISABLE, Boolean.class);
        if (isWindowingDisabled) {
            log.info("Windowing is disabled. Skipping hooking in the Loop Bolt...");
        } else {
            builder.setBolt(LOOP_COMPONENT, new LoopBolt(config), loopBoltParallelism)
                   .shuffleGrouping(JOIN_COMPONENT, FEEDBACK_STREAM)
                   .setCPULoad(loopBoltCPULoad).setMemoryLoad(loopBoltMemoryOnHeapLoad, loopBoltMemoryOffHeapLoad);
        }

        Config stormConfig = new Config();

        // Scheduler
        stormConfig.setTopologyStrategy(DefaultResourceAwareStrategy.class);

        // Metrics
        Boolean enableMetrics = (Boolean) config.get(BulletStormConfig.TOPOLOGY_METRICS_ENABLE);
        if (enableMetrics) {
            List<String> classNames = config.getAs(BulletStormConfig.TOPOLOGY_METRICS_CLASSES, List.class);
            classNames.forEach(className -> ReflectionUtils.registerMetricsConsumer(className, stormConfig, config));
        }

        // Put the rest of the other possible custom Storm settings without checking their types
        stormConfig.putAll(config.getCustomStormSettings());

        StormSubmitter.submitTopology(name, stormConfig, builder.createTopology());
    }

    /**
     * This submits a topology after loading the given spout with the given configuration as the source of
     * {@link com.yahoo.bullet.record.BulletRecord}.
     *
     * @param spout The name of the instance of an {@link org.apache.storm.topology.IRichSpout} to load.
     * @param args The arguments to pass to the constructor of this spout (otherwise the default constructor is used).
     * @param config The Storm settings for this Bullet topology.
     * @param parallelism The parallelism of the spout component.
     * @param cpuLoad The CPU load for the Storm RAS scheduler.
     * @param onHeapMemoryLoad The on heap memory load for the Storm RAS scheduler.
     * @param offHeapMemoryLoad The off heap memory load for the Storm RAS scheduler.
     * @throws Exception if there were issues creating the topology.
     */
    public static void submit(String spout, List<String> args, BulletStormConfig config, Number parallelism,
                              Number cpuLoad, Number onHeapMemoryLoad, Number offHeapMemoryLoad) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(TopologyConstants.RECORD_COMPONENT, ReflectionUtils.getSpout(spout, args), parallelism)
               .setCPULoad(cpuLoad).setMemoryLoad(onHeapMemoryLoad, offHeapMemoryLoad);
        log.info("Added spout " + spout + " with parallelism " + parallelism + ", CPU load " + cpuLoad +
                 ", On-heap memory " + onHeapMemoryLoad + ", Off-heap memory " + offHeapMemoryLoad);
        submit(config, TopologyConstants.RECORD_COMPONENT, builder);
    }
}
