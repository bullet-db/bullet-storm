/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import lombok.Getter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;

@Getter
public class CustomTopologyBuilder extends TopologyBuilder {
    private boolean topologyCreated = false;
    private List<CustomSpoutDeclarer> createdSpouts = new ArrayList<>();
    private List<CustomBoltDeclarer> createdBolts = new ArrayList<>();

    @Override
    public StormTopology createTopology() {
        topologyCreated = true;
        throw new RuntimeException("You should handle this exception silently since the topology should not be submitted");
    }

    @Override
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism) throws IllegalArgumentException {
        CustomSpoutDeclarer declarer = new CustomSpoutDeclarer(id, spout, parallelism);
        createdSpouts.add(declarer);
        return declarer;
    }

    @Override
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism) throws IllegalArgumentException {
        CustomBoltDeclarer declarer = new CustomBoltDeclarer(id, bolt, parallelism);
        createdBolts.add(declarer);
        return declarer;
    }
}
