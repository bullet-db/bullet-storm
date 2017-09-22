/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

@Slf4j
public class Topology {
    public static final int DEFAULT_PARALLELISM = 10;
    public static final String RESOURCE_AWARE_SCHEDULING_STRATEGY = "ras";
    public static final double DEFAULT_CPU_LOAD = 50.0;
    public static final double DEFAULT_ON_HEAP_MEMORY_LOAD = 256.0;
    public static final double DEFAULT_OFF_HEAP_MEMORY_LOAD = 160.0;
    public static final String SPOUT_ARG = "bullet-spout";
    public static final String PARALLELISM_ARG = "bullet-spout-parallelism";
    public static final String CPU_LOAD_ARG = "bullet-spout-cpu-load";
    public static final String ON_HEAP_MEMORY_LOAD_ARG = "bullet-spout-on-heap-memory-load";
    public static final String OFF_HEAP_MEMORY_LOAD_ARG = "bullet-spout-off-heap-memory-load";
    public static final String ARGUMENT_ARG = "bullet-spout-arg";
    public static final String CONFIGURATION_ARG = "bullet-conf";
    public static final String HELP_ARG = "help";
    public static final String REGISTER_METHOD = "register";

    public static final OptionParser PARSER = new OptionParser() {
        {
            accepts(SPOUT_ARG, "The spout class that implements IRichSpout")
                    .withRequiredArg()
                    .describedAs("Spout Class");
            accepts(SPOUT_ARG, "The spout class that implements IRichSpout")
                    .withRequiredArg()
                    .describedAs("Spout Class");
            accepts(PARALLELISM_ARG, "The parallelism hint to use for your spout for Storm.")
                    .withOptionalArg()
                    .ofType(Integer.class)
                    .describedAs("The parallism hint for the Spout component")
                    .defaultsTo(DEFAULT_PARALLELISM);
            accepts(CPU_LOAD_ARG, "The CPU load to use for your spout in the Storm RAS scheduler.")
                    .withOptionalArg()
                    .ofType(Double.class)
                    .describedAs("The rough CPU load estimate for the Spout component in the Strom RAS scheduler.")
                    .defaultsTo(DEFAULT_CPU_LOAD);
            accepts(ON_HEAP_MEMORY_LOAD_ARG, "The on-heap memory to use for your spout in the Storm RAS scheduler.")
                    .withOptionalArg()
                    .ofType(Double.class)
                    .describedAs("The rough on-heap memory estimate for the Spout component in the Storm RAS scheduler.")
                    .defaultsTo(DEFAULT_ON_HEAP_MEMORY_LOAD);
            accepts(OFF_HEAP_MEMORY_LOAD_ARG, "The off-heap memory to use for your spout in the Storm RAS scheduler.")
                    .withOptionalArg()
                    .ofType(Double.class)
                    .describedAs("The rough off-heap memory estimate for the Spout component in the Storm RAS scheduler.")
                    .defaultsTo(DEFAULT_OFF_HEAP_MEMORY_LOAD);
            accepts(CONFIGURATION_ARG, "An optional configuration YAML file for Bullet")
                    .withOptionalArg()
                    .describedAs("Configuration file used to override Bullet's default settings");
            accepts(ARGUMENT_ARG, "Pass arguments to your Spout class")
                    .withOptionalArg()
                    .describedAs("Arguments to be collected and passed in as a List<String> to your Spout constructor");
            accepts(HELP_ARG, "Show this help message")
                    .withOptionalArg()
                    .describedAs("Print help message");
            allowsUnrecognizedOptions();
        }
    };

    /**
     * This function can be used to wire up the source of the records to Bullet. Your source may be as simple
     * as a Spout (in which case, just use the {@link #main(String[])} method with the class name of your Spout.
     * This method is more for wiring up and arbitrary topology to Bullet. The name of last component in your
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

        Integer tickInterval = ((Number) config.get(BulletStormConfig.TICK_INTERVAL_SECS)).intValue();

        builder.setSpout(TopologyConstants.QUERY_COMPONENT, new QuerySpout(config), querySpoutParallelism)
               .setCPULoad(querySpoutCPULoad)
               .setMemoryLoad(querySpoutMemoryOnHeapLoad, querySpoutMemoryOffHeapLoad);

        // Hook in the source of the BulletRecords
        builder.setBolt(TopologyConstants.FILTER_COMPONENT, new FilterBolt(recordComponent, tickInterval), filterBoltParallelism)
               .shuffleGrouping(recordComponent)
               .allGrouping(TopologyConstants.QUERY_COMPONENT, TopologyConstants.QUERY_STREAM)
               .setCPULoad(filterBoltCPULoad)
               .setMemoryLoad(filterBoltMemoryOnheapLoad, filterBoltMemoryOffHeapLoad);

        builder.setBolt(TopologyConstants.JOIN_COMPONENT, new JoinBolt(tickInterval), joinBoltParallelism)
               .fieldsGrouping(TopologyConstants.QUERY_COMPONENT, TopologyConstants.QUERY_STREAM, new Fields(TopologyConstants.ID_FIELD))
               .fieldsGrouping(TopologyConstants.QUERY_COMPONENT, TopologyConstants.METADATA_STREAM, new Fields(TopologyConstants.ID_FIELD))
               .fieldsGrouping(TopologyConstants.FILTER_COMPONENT, TopologyConstants.FILTER_STREAM, new Fields(TopologyConstants.ID_FIELD))
               .setCPULoad(joinBoltCPULoad)
               .setMemoryLoad(joinBoltMemoryOnHeapLoad, joinBoltMemoryOffHeapLoad);

        builder.setBolt(TopologyConstants.RESULT_COMPONENT, new ResultBolt(config), resultBoltParallelism)
               .shuffleGrouping(TopologyConstants.JOIN_COMPONENT, TopologyConstants.JOIN_STREAM)
               .setCPULoad(resultBoltCPULoad)
               .setMemoryLoad(resultBoltMemoryOnHeapLoad, resultBoltMemoryOffHeapLoad);


        Config stormConfig = new Config();

        // Enable debug logging
        Boolean debug = (Boolean) config.get(BulletStormConfig.TOPOLOGY_DEBUG);
        stormConfig.setDebug(debug);

        // Scheduler
        String scheduler = (String) config.get(BulletStormConfig.TOPOLOGY_SCHEDULER);
        stormConfig.setTopologyStrategy(getScheduler(scheduler));

        // Workers (only applicable for Multitenant Scheduler)
        Number workers = (Number) config.get(BulletStormConfig.TOPOLOGY_WORKERS);
        stormConfig.setNumWorkers(workers.intValue());

        // Metrics
        Boolean enableMetrics = (Boolean) config.get(BulletStormConfig.TOPOLOGY_METRICS_ENABLE);
        if (enableMetrics) {
            List<String> classNames = (List<String>) config.getOrDefault(BulletStormConfig.TOPOLOGY_METRICS_CLASSES, emptyList());
            classNames.stream().forEach(className -> registerMetricsConsumer(className, stormConfig, config));
        }

        // Put the rest of the Bullet settings without checking their types
        stormConfig.putAll(config.getNonTopologySubmissionSettings());

        StormSubmitter.submitTopology(name, stormConfig, builder.createTopology());
    }

    private static Class<? extends IStrategy> getScheduler(String scheduler) {
        switch (scheduler) {
            case RESOURCE_AWARE_SCHEDULING_STRATEGY:
                return DefaultResourceAwareStrategy.class;
            default:
                throw new RuntimeException("Only the ResourceAwareScheduler is supported at this time.");
        }
    }

    private static IRichSpout getSpout(String className, List<String> args) throws Exception {
        Class<? extends IRichSpout> spout = (Class<? extends IRichSpout>) Class.forName(className);
        IRichSpout initialized;
        try {
            Constructor constructor = spout.getConstructor(List.class);
            log.info("Initializing spout using constructor {} with args {}", constructor.toGenericString(), args);
            initialized = (IRichSpout) constructor.newInstance(args);
        } catch (Exception e) {
            log.info("Could not find or initialize a constructor taking a List. Trying default constructor...", e);
            initialized = spout.newInstance();
        }
        log.info("Initialized spout class {}", className);
        return initialized;
    }

    private static void registerMetricsConsumer(String className, Config stormConfig, BulletStormConfig bulletStormConfig) {
        try {
            Class<? extends IMetricsConsumer> consumer = (Class<? extends IMetricsConsumer>) Class.forName(className);
            Method method = consumer.getMethod(REGISTER_METHOD, Config.class, BulletStormConfig.class);
            log.info("Calling the IMetricsConsumer register method for class {} using method {}", className, method.toGenericString());
            method.invoke(null, stormConfig, bulletStormConfig);
            log.info("Registered the IMetricsConsumer class {}", className);
        } catch (Exception e) {
            log.info("Could not call the register method for " + className, e);
        }
    }

    private static void printHelp() throws IOException {
        System.out.println("If you are looking to connect your existing topology to Bullet, you should compile");
        System.out.println("in the Bullet jar and use the submit method in this class to wire up Bullet");
        System.out.println("to the tail end of your topology that produces BulletRecords. If you are simply");
        System.out.println("looking to connect a Spout class that implements IRichSpout and emits BulletRecords,");
        System.out.println("use the main class directly with the arguments below.");
        PARSER.printHelpOn(System.out);
    }


    /**
     * Main. Launches a remote Storm topology.
     * @param args The input args.
     * @throws Exception if any.
     */
    public static void main(String[] args) throws Exception {
        OptionSet options = PARSER.parse(args);
        if (!options.hasOptions() || options.has(HELP_ARG) || !options.has(SPOUT_ARG)) {
            printHelp();
            return;
        }
        String spoutClass = (String) options.valueOf(SPOUT_ARG);
        List<String> arguments = (List<String>) options.valuesOf(ARGUMENT_ARG);
        Integer parallelism = (Integer) options.valueOf(PARALLELISM_ARG);
        Double cpuLoad = (Double) options.valueOf(CPU_LOAD_ARG);
        Double onHeapMemoryLoad = (Double) options.valueOf(ON_HEAP_MEMORY_LOAD_ARG);
        Double offHeapMemoryLoad = (Double) options.valueOf(OFF_HEAP_MEMORY_LOAD_ARG);
        String configuration = (String) options.valueOf(CONFIGURATION_ARG);

        BulletStormConfig bulletStormConfig = new BulletStormConfig(configuration);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(TopologyConstants.RECORD_COMPONENT, getSpout(spoutClass, arguments), parallelism)
               .setCPULoad(cpuLoad)
               .setMemoryLoad(onHeapMemoryLoad, offHeapMemoryLoad);
        log.info("Added spout " + spoutClass + " with parallelism " + parallelism + ", CPU load " + cpuLoad +
                 ", On-heap memory " + onHeapMemoryLoad + ", Off-heap memory " + offHeapMemoryLoad);

        submit(bulletStormConfig, TopologyConstants.RECORD_COMPONENT, builder);
    }
}

