/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class DRPCError {
    public static final DRPCError CANNOT_REACH_DRPC = new DRPCError("Cannot reach DRPC server.", "Please try again later.");
    public static final DRPCError RETRY_LIMIT_EXCEEDED = new DRPCError("Retry limit exceeded.", "Please try again later.");
    public static final DRPCError INVALID_QUERY = new DRPCError("Query could not be completed. ", "Please check query and retry.");
    public static final DRPCError SERVER_UNAVAILABLE = new DRPCError("Server temporarily unavailable", "Please try again later.");

    private String error;
    private String resolution;

    /**
     * Constructor that takes an error message and resolution for it.
     *
     * @param error The error message.
     * @param resolution The resolution that can be taken.
     */
    public DRPCError(String error, String resolution) {
        this.error = error;
        this.resolution = resolution;
    }
}
