/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.result;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public interface JSONFormatter {
    Gson GSON = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();

    /**
     * Returns a JSON string representation of object.
     * @param object The object to make a JSON out of.
     * @return JSON string of the object.
     */
    static String asJSON(Object object) {
        return GSON.toJson(object);
    }

    /**
     * Convert this object to a JSON string.
     * @return The JSON representation of this.
     */
    String asJSON();
}
