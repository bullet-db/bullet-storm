/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class CustomTopologyBuilder extends TopologyBuilder {
    @Setter
    private boolean throwExceptionOnCreate = true;
    private boolean topologyCreated = false;
    private List<CustomSpoutDeclarer> createdSpouts = new ArrayList<>();
    private List<CustomBoltDeclarer> createdBolts = new ArrayList<>();

    @Override
    public StormTopology createTopology() {
        topologyCreated = true;
        if (throwExceptionOnCreate) {
            throw new RuntimeException("You should handle this exception silently since the topology should not be submitted");
        }
        return null;
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
