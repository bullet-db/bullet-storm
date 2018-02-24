/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import lombok.Getter;

import java.util.List;

public class TestSpout extends CustomIRichSpout {
    @Getter
    private List<String> args;

    public TestSpout(List<String> args) {
        this.args = args;
    }
}
