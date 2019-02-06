/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

package com.yahoo.bullet.storm;

import com.yahoo.bullet.dsl.deserializer.BulletDeserializer;

/**
 * BulletDeserializer that does nothing.
 */
public class IdentityDeserializer extends BulletDeserializer {
    @Override
    public Object deserialize(Object object) {
        return object;
    }
}
