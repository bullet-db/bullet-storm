/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.dsl.deserializer.BulletDeserializer;

import java.util.Collections;
import java.util.Map;

public class MockDeserializer extends BulletDeserializer {

    /**
     * Creates a MockDeserializer.
     *
     * @param config Not used.
     */
    public MockDeserializer(BulletConfig config) {
        super(config);
    }

    @Override
    public Object deserialize(Object object) {
        Map<String, String> map = (Map<String, String>) object;
        return Collections.singletonMap("bar", map.get("foo"));
    }
}
