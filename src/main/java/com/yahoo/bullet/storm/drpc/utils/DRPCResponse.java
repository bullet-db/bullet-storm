/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import lombok.Setter;

@Setter
public class DRPCResponse {
    String content;
    DRPCError error;

    /**
     *  Get the content of this DRPCResponse object.
     *
     * @return The content of this Response which will be a JSON object.
     */
    public String getContent() {
        return content;
    }

    /**
     * Get the error of this DRPCResponse.
     *
     * @return The error of this Response.  Null if no error occurred.
     */
    public DRPCError getError() {
        return error;
    }

    /**
     * Indicates whether this DRPCResponse object has an error.
     *
     * @return true if this object has an error object, false otherwise.
     */
    public boolean hasError() {
        return error != null;
    }

    /**
     * Constructor that creates a DRPCResponse using a String JSON.
     *
     * @param content The String JSON response from DRPC.
     */
    public DRPCResponse(String content) {
        this.content = content;
    }

    /**
     * Constructor that creates a DRPCResponse using a {@link DRPCError}.
     *
     * @param error The {@link DRPCError} that should be returned as the response.
     */
    public DRPCResponse(DRPCError error) {
        this.error = error;
    }
}
