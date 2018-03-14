/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;

import java.io.Serializable;
import java.util.Collections;

public class DRPCError extends BulletError implements Serializable {
    public static final String GENERIC_RESOLUTION = "Please try again later";
    public static final String GENERIC_ERROR = "Cannot reach the DRPC server";
    public static final DRPCError CANNOT_REACH_DRPC = new DRPCError(GENERIC_ERROR, GENERIC_RESOLUTION);

    /**
     * Constructor that takes an error message and resolution for it.
     *
     * @param error The error message.
     * @param resolution The resolution that can be taken.
     */
    public DRPCError(String error, String resolution) {
        super(error, Collections.singletonList(resolution));
    }

    /**
     * Write this error as a JSON Bullet error response in the {@link Meta} of a {@link Clip}.
     *
     * @return A String JSON version of this error.
     */
    public String asJSONClip() {
        return Clip.of(Meta.of(this)).asJSON();
    }
}
