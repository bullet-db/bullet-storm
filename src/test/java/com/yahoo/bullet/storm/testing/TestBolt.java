package com.yahoo.bullet.storm.testing;

import lombok.Getter;

import java.util.List;

public class TestBolt extends CustomIRichBolt {
    @Getter
    private List<String> args;

    public TestBolt(List<String> args) {
        this.args = args;
    }
}
