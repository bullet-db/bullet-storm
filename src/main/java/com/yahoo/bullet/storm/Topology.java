/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class Topology {
    public static final int DEFAULT_PARALLELISM = 10;
    public static final String SPOUT_ARG = "bullet-spout";
    public static final String PARALLELISM_ARG = "bullet-spout-parallelism";
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
     * Main. Launches a remote Storm topology.
     * @param args The input args.
     * @throws Exception if any.
     */
    public static void main(String[] args) throws Exception {
        OptionSet options = PARSER.parse(args);
        if (!options.hasOptions() || options.has(HELP_ARG) || !options.has(SPOUT_ARG)) {
            System.out.println("If you are looking to connect your existing topology to Bullet, you should compile");
            System.out.println("in the Bullet jar and use the submit method in the StormUtils class to wire up Bullet");
            System.out.println("to the tail end of your topology that produces BulletRecords. If you are simply");
            System.out.println("looking to connect a Spout class that implements IRichSpout and emits BulletRecords,");
            System.out.println("use the main class directly with the arguments below.");
            PARSER.printHelpOn(System.out);
            return;
        }
        String spoutClass = (String) options.valueOf(SPOUT_ARG);
        List<String> arguments = (List<String>) options.valuesOf(ARGUMENT_ARG);
        Integer parallelism = (Integer) options.valueOf(PARALLELISM_ARG);

        String yamlPath = (String) options.valueOf(CONFIGURATION_ARG);
        BulletStormConfig config = new BulletStormConfig(yamlPath);
        log.info(config.toString());

        StormUtils.submit(spoutClass, arguments, config, parallelism);
    }
}
