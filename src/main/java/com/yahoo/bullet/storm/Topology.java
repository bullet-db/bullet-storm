/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

@Slf4j
public class Topology {
    public static final int DEFAULT_PARALLELISM = 10;
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

    public static final OptionParser PARSER = new OptionParser() {
        {
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

        String yamlPath = (String) options.valueOf(CONFIGURATION_ARG);
        BulletStormConfig config = new BulletStormConfig(yamlPath);

        StormUtils.submit(spoutClass, arguments, config, parallelism, cpuLoad, onHeapMemoryLoad, offHeapMemoryLoad);
    }
}
