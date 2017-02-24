/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.yahoo.bullet.storm.FilterBolt;
import com.yahoo.bullet.storm.JoinBolt;
import com.yahoo.bullet.storm.PrepareRequestBolt;
import com.yahoo.bullet.storm.TopologyConstants;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

@Slf4j
public class Topology {
    public static final int DEFAULT_PARALLELISM = 10;
    public static final String SPOUT_ARG = "bullet-spout";
    public static final String PARALLELISM_ARG = "bullet-spout-parallelism";
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
     * @param config The non-null {@link BulletConfig} that contains the necessary configuration.
     * @param recordComponent The non-null name of the component used in your topology that is the source of records for Bullet.
     * @param builder The non-null {@link TopologyBuilder} that was used to create your topology.
     * @throws Exception if there were issues creating the topology.
     */
    public static void submit(BulletConfig config, String recordComponent, TopologyBuilder builder) throws Exception {
        Objects.requireNonNull(config);
        Objects.requireNonNull(recordComponent);
        Objects.requireNonNull(builder);

        String name = (String) config.get(BulletConfig.TOPOLOGY_NAME);
        String function = (String) config.get(BulletConfig.TOPOLOGY_FUNCTION);

        Number drpcSpoutParallelism = (Number) config.get(BulletConfig.DRPC_SPOUT_PARALLELISM);

        Number prepareBoltParallelism = (Number) config.get(BulletConfig.PREPARE_BOLT_PARALLELISM);

        Number filterBoltParallelism = (Number) config.get(BulletConfig.FILTER_BOLT_PARALLELISM);

        Number joinBoltParallelism = (Number) config.get(BulletConfig.JOIN_BOLT_PARALLELISM);

        Number returnBoltParallelism = (Number) config.get(BulletConfig.RETURN_BOLT_PARALLELISM);

        Integer tickInterval = ((Number) config.get(BulletConfig.TICK_INTERVAL_SECS)).intValue();

        builder.setSpout(TopologyConstants.DRPC_COMPONENT, new DRPCSpout(function), drpcSpoutParallelism);

        builder.setBolt(TopologyConstants.PREPARE_COMPONENT, new PrepareRequestBolt(), prepareBoltParallelism)
               .shuffleGrouping(TopologyConstants.DRPC_COMPONENT);

        // Hook in the source of the BulletRecords
        builder.setBolt(TopologyConstants.FILTER_COMPONENT, new FilterBolt(recordComponent, tickInterval), filterBoltParallelism)
               .shuffleGrouping(recordComponent)
               .allGrouping(TopologyConstants.PREPARE_COMPONENT, TopologyConstants.ARGS_STREAM);

        builder.setBolt(TopologyConstants.JOIN_COMPONENT, new JoinBolt(tickInterval), joinBoltParallelism)
               .fieldsGrouping(TopologyConstants.PREPARE_COMPONENT, TopologyConstants.ARGS_STREAM, new Fields(TopologyConstants.ID_FIELD))
               .fieldsGrouping(TopologyConstants.PREPARE_COMPONENT, TopologyConstants.RETURN_STREAM, new Fields(TopologyConstants.ID_FIELD))
               .fieldsGrouping(TopologyConstants.FILTER_COMPONENT, TopologyConstants.FILTER_STREAM, new Fields(TopologyConstants.ID_FIELD));

        builder.setBolt(TopologyConstants.RETURN_COMPONENT, new ReturnResults(), returnBoltParallelism)
               .shuffleGrouping(TopologyConstants.JOIN_COMPONENT, TopologyConstants.JOIN_STREAM);


        Config stormConfig = new Config();

        // Enable debug logging
        Boolean debug = (Boolean) config.get(BulletConfig.TOPOLOGY_DEBUG);
        stormConfig.setDebug(debug);

        // Workers
        Number workers = (Number) config.get(BulletConfig.TOPOLOGY_WORKERS);
        stormConfig.setNumWorkers(workers.intValue());

        Boolean enableMetrics = (Boolean) config.get(BulletConfig.TOPOLOGY_METRICS_ENABLE);
        if (enableMetrics) {
            List<String> classNames = (List<String>) config.getOrDefault(BulletConfig.TOPOLOGY_METRICS_CLASSES, emptyList());
            classNames.stream().forEach(className -> registerMetricsConsumer(className, stormConfig, config));
        }

        // Put the rest of the Bullet settings without checking their types
        stormConfig.putAll(config.getNonTopologySubmissionSettings());

        StormSubmitter.submitTopology(name, stormConfig, builder.createTopology());
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

    private static void registerMetricsConsumer(String className, Config stormConfig, BulletConfig bulletConfig) {
        try {
            Class<? extends IMetricsConsumer> consumer = (Class<? extends IMetricsConsumer>) Class.forName(className);
            Method method = consumer.getMethod(REGISTER_METHOD, Config.class, BulletConfig.class);
            log.info("Calling the IMetricsConsumer register method for class {} using method {}", className, method.toGenericString());
            method.invoke(null, stormConfig, bulletConfig);
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
        String configuration = (String) options.valueOf(CONFIGURATION_ARG);

        BulletConfig bulletConfig = new BulletConfig(configuration);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(TopologyConstants.RECORD_COMPONENT, getSpout(spoutClass, arguments), parallelism);

        log.info("Added spout " + spoutClass + " with parallelism " + parallelism);

        submit(bulletConfig, TopologyConstants.RECORD_COMPONENT, builder);
    }
}

