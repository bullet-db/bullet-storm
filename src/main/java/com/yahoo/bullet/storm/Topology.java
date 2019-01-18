/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.topology.TopologyBuilder;

import java.io.IOException;

@Slf4j
public class Topology {
    public static final String CONFIGURATION_ARG = "bullet-conf";
    public static final String HELP_ARG = "help";

    public static final OptionParser PARSER = new OptionParser() {
        {
            accepts(CONFIGURATION_ARG, "The configuration YAML file for Bullet")
                    .withRequiredArg()
                    .describedAs("Configuration file used to specify your spout/bolt and override Bullet's default settings");
            accepts(HELP_ARG, "Shows the help message")
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

        if (options.has(HELP_ARG) || !options.has(CONFIGURATION_ARG)) {
            printHelp();
            return;
        }

        String yamlPath = (String) options.valueOf(CONFIGURATION_ARG);
        BulletStormConfig config = new BulletStormConfig(yamlPath);
        log.info(config.toString());

        StormUtils.submit(config, new TopologyBuilder());
    }

    private static void printHelp() throws IOException  {
        // TODO requesting help with this help message
        System.out.println("If you want to connect your existing topology to Bullet, you should compile in\n" +
                           "the Bullet jar and use the submit() method in the StormUtils class to wire up\n" +
                           "Bullet to the tail end of your topology (that should be producing BulletRecords).\n\n" +
                           "If you want to use Bullet DSL to plug in an existing data source, please set the\n" +
                           "following in your YAML configuration:\n" +
                           " - bullet.topology.dsl.spout.enable: true\n" +
                           " - bullet.topology.dsl.spout.parallelism: (The parallelism hint for the DSL spout)\n" +
                           " - bullet.topology.dsl.spout.cpu.load: (The CPU load given to the DSL spout in the Storm RAS scheduler)\n" +
                           " - bullet.topology.dsl.spout.memory.on.heap.load: (The on-heap memory given to the DSL spout in the Storm RAS scheduler)\n" +
                           " - bullet.topology.dsl.spout.memory.off.heap.load: (The off-heap memory given to the DSL spout in the Storm RAS scheduler)\n\n" +
                           "If you want to use a DSL Bolt in addition to the DSL Spout, set the following:\n" +
                           " - bullet.topology.dsl.bolt.enable: true\n" +
                           " - bullet.topology.dsl.bolt.parallelism: 10\n" +
                           " - bullet.topology.dsl.bolt.cpu.load: 50.0\n" +
                           " - bullet.topology.dsl.bolt.memory.on.heap.load: 256.0\n" +
                           " - bullet.topology.dsl.bolt.memory.off.heap.load: 160.0\n\n" +
                           "If you wish to connect a Spout that implements IRichSpout and emits\n" +
                           "BulletRecords, set the following:\n" +
                           " - bullet.topology.bullet.spout.class.name: \"your-bullet-spout\"\n" +
                           " - bullet.topology.bullet.spout.args: []\n" +
                           " - bullet.topology.bullet.spout.parallelism: 10\n" +
                           " - bullet.topology.bullet.spout.cpu.load: 50.0\n" +
                           " - bullet.topology.bullet.spout.memory.on.heap.load: 256.0\n" +
                           " - bullet.topology.bullet.spout.memory.off.heap.load: 160.0\n\n" +
                           "If you wish to direct your Spout to your Bolt, set the following:\n" +
                           " - bullet.topology.bullet.bolt.enable: false\n" +
                           " - bullet.topology.bullet.bolt.class.name: \"your-bullet-bolt\"\n" +
                           " - bullet.topology.bullet.bolt.args: []\n" +
                           " - bullet.topology.bullet.bolt.parallelism: 10\n" +
                           " - bullet.topology.bullet.bolt.cpu.load: 50.0\n" +
                           " - bullet.topology.bullet.bolt.memory.on.heap.load: 256.0\n" +
                           " - bullet.topology.bullet.bolt.memory.off.heap.load: 160.0");
        PARSER.printHelpOn(System.out);
    }
}
