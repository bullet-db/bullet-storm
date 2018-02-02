/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

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
     * @param config The non-null {@link BulletStormConfig} that contains the necessary configuration.
     * @param recordComponent The non-null name of the component used in your topology that is the source of records for Bullet.
     * @param builder The non-null {@link TopologyBuilder} that was used to create your topology.
     * @throws Exception if there were issues creating the topology.
     */
    public static void submit(BulletStormConfig config, String recordComponent, TopologyBuilder builder) throws Exception {
        Objects.requireNonNull(config);
        Objects.requireNonNull(recordComponent);
        Objects.requireNonNull(builder);

        String name = (String) config.get(BulletStormConfig.TOPOLOGY_NAME);

        Number querySpoutParallelism = (Number) config.get(BulletStormConfig.QUERY_SPOUT_PARALLELISM);
        Number querySpoutCPULoad = (Number) config.get(BulletStormConfig.QUERY_SPOUT_CPU_LOAD);
        Number querySpoutMemoryOnHeapLoad = (Number) config.get(BulletStormConfig.QUERY_SPOUT_MEMORY_ON_HEAP_LOAD);
        Number querySpoutMemoryOffHeapLoad = (Number) config.get(BulletStormConfig.QUERY_SPOUT_MEMORY_OFF_HEAP_LOAD);

        // Ticks are 1
        Number tickSpoutParallelism = BulletStormConfig.TICK_SPOUT_PARALLELISM;
        Number tickSpoutCPULoad = (Number) config.get(BulletStormConfig.TICK_SPOUT_CPU_LOAD);
        Number tickSpoutMemoryOnheapLoad = (Number) config.get(BulletStormConfig.TICK_SPOUT_MEMORY_ON_HEAP_LOAD);
        Number tickSpoutMemoryOffHeapLoad = (Number) config.get(BulletStormConfig.TICK_SPOUT_MEMORY_OFF_HEAP_LOAD);

        Number filterBoltParallelism = (Number) config.get(BulletStormConfig.FILTER_BOLT_PARALLELISM);
        Number filterBoltCPULoad = (Number) config.get(BulletStormConfig.FILTER_BOLT_CPU_LOAD);
        Number filterBoltMemoryOnheapLoad = (Number) config.get(BulletStormConfig.FILTER_BOLT_MEMORY_ON_HEAP_LOAD);
        Number filterBoltMemoryOffHeapLoad = (Number) config.get(BulletStormConfig.FILTER_BOLT_MEMORY_OFF_HEAP_LOAD);

        Number joinBoltParallelism = (Number) config.get(BulletStormConfig.JOIN_BOLT_PARALLELISM);
        Number joinBoltCPULoad = (Number) config.get(BulletStormConfig.JOIN_BOLT_CPU_LOAD);
        Number joinBoltMemoryOnHeapLoad = (Number) config.get(BulletStormConfig.JOIN_BOLT_MEMORY_ON_HEAP_LOAD);
        Number joinBoltMemoryOffHeapLoad = (Number) config.get(BulletStormConfig.JOIN_BOLT_MEMORY_OFF_HEAP_LOAD);

        Number resultBoltParallelism = (Number) config.get(BulletStormConfig.RESULT_BOLT_PARALLELISM);
        Number resultBoltCPULoad = (Number) config.get(BulletStormConfig.RESULT_BOLT_CPU_LOAD);
        Number resultBoltMemoryOnHeapLoad = (Number) config.get(BulletStormConfig.RESULT_BOLT_MEMORY_ON_HEAP_LOAD);
        Number resultBoltMemoryOffHeapLoad = (Number) config.get(BulletStormConfig.RESULT_BOLT_MEMORY_OFF_HEAP_LOAD);

        Number loopBoltParallelism = (Number) config.get(BulletStormConfig.LOOP_BOLT_PARALLELISM);
        Number loopBoltCPULoad = (Number) config.get(BulletStormConfig.LOOP_BOLT_CPU_LOAD);
        Number loopBoltMemoryOnHeapLoad = (Number) config.get(BulletStormConfig.LOOP_BOLT_MEMORY_ON_HEAP_LOAD);
        Number loopBoltMemoryOffHeapLoad = (Number) config.get(BulletStormConfig.LOOP_BOLT_MEMORY_OFF_HEAP_LOAD);

        builder.setSpout(TopologyConstants.QUERY_COMPONENT, new QuerySpout(config), querySpoutParallelism)
               .setCPULoad(querySpoutCPULoad)
               .setMemoryLoad(querySpoutMemoryOnHeapLoad, querySpoutMemoryOffHeapLoad);

        builder.setSpout(TopologyConstants.TICK_COMPONENT, new TickSpout(config), tickSpoutParallelism)
               .setCPULoad(tickSpoutCPULoad)
               .setMemoryLoad(tickSpoutMemoryOnheapLoad, tickSpoutMemoryOffHeapLoad);

        // Hook in the source of the BulletRecords
        builder.setBolt(TopologyConstants.FILTER_COMPONENT, new FilterBolt(recordComponent, config), filterBoltParallelism)
               .shuffleGrouping(recordComponent)
               .allGrouping(TopologyConstants.QUERY_COMPONENT, TopologyConstants.QUERY_STREAM)
               .allGrouping(TopologyConstants.TICK_COMPONENT, TopologyConstants.TICK_STREAM)
               .setCPULoad(filterBoltCPULoad)
               .setMemoryLoad(filterBoltMemoryOnheapLoad, filterBoltMemoryOffHeapLoad);

        builder.setBolt(TopologyConstants.JOIN_COMPONENT, new JoinBolt(config), joinBoltParallelism)
               .fieldsGrouping(TopologyConstants.QUERY_COMPONENT, TopologyConstants.QUERY_STREAM, new Fields(TopologyConstants.ID_FIELD))
               .fieldsGrouping(TopologyConstants.FILTER_COMPONENT, TopologyConstants.DATA_STREAM, new Fields(TopologyConstants.ID_FIELD))
               .allGrouping(TopologyConstants.TICK_COMPONENT, TopologyConstants.TICK_STREAM)
               .setCPULoad(joinBoltCPULoad)
               .setMemoryLoad(joinBoltMemoryOnHeapLoad, joinBoltMemoryOffHeapLoad);

        builder.setBolt(TopologyConstants.RESULT_COMPONENT, new ResultBolt(config), resultBoltParallelism)
               .shuffleGrouping(TopologyConstants.JOIN_COMPONENT, TopologyConstants.RESULT_STREAM)
               .setCPULoad(resultBoltCPULoad)
               .setMemoryLoad(resultBoltMemoryOnHeapLoad, resultBoltMemoryOffHeapLoad);

        builder.setBolt(TopologyConstants.LOOP_COMPONENT, new LoopBolt(config), loopBoltParallelism)
               .shuffleGrouping(TopologyConstants.JOIN_COMPONENT, TopologyConstants.METADATA_STREAM)
               .setCPULoad(loopBoltCPULoad)
               .setMemoryLoad(loopBoltMemoryOnHeapLoad, loopBoltMemoryOffHeapLoad);

        Config stormConfig = new Config();

        // Scheduler
        stormConfig.setTopologyStrategy(DefaultResourceAwareStrategy.class);

        // Metrics
        Boolean enableMetrics = (Boolean) config.get(BulletStormConfig.TOPOLOGY_METRICS_ENABLE);
        if (enableMetrics) {
            List<String> classNames = (List<String>) config.getOrDefault(BulletStormConfig.TOPOLOGY_METRICS_CLASSES, emptyList());
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
     * @param spout The name of the {@link IRichSpout} to load.
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
               .setCPULoad(cpuLoad)
               .setMemoryLoad(onHeapMemoryLoad, offHeapMemoryLoad);
        log.info("Added spout " + spout + " with parallelism " + parallelism + ", CPU load " + cpuLoad +
                 ", On-heap memory " + onHeapMemoryLoad + ", Off-heap memory " + offHeapMemoryLoad);
        submit(config, TopologyConstants.RECORD_COMPONENT, builder);
    }
}
