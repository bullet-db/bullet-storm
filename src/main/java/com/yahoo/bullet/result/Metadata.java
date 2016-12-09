/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.result;

import com.yahoo.bullet.parsing.Error;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Metadata {
    private Map<String, Object> meta = new HashMap<>();

    public static final String ERROR_KEY = "errors";
    public static final String CREATION_TIME = "Creation Time";
    public static final String TERMINATION_TIME = "Termination Time";
    public static final String RULE_ID = "Rule Identifier";
    public static final String RULE_BODY = "Rule Body";

    /**
     * Returns a backing view of the meta information as a Map.
     * @return A Map of keys to objects that denote the meta information.
     */
    public Map<String, Object> asMap() {
        return meta;
    }

    /**
     * Add a piece of meta information.
     * @param key The name of the meta tag
     * @param information An object that represents the information.
     * @return This object for chaining.
     */
    public Metadata add(String key, Object information) {
        meta.put(key, information);
        return this;
    }

    /**
     * Add an error to the Metadata.
     * @param errors Error objects to add.
     * @return This object for chaining.
     */
    public Metadata addErrors(List<Error> errors) {
        Objects.requireNonNull(errors);
        List<Error> existing = (List<Error>) meta.get(ERROR_KEY);
        if (existing != null) {
            existing.addAll(errors);
        } else {
            meta.put(ERROR_KEY, new ArrayList<>(errors));
        }
        return this;
    }

    /**
     * Static construction of Metadata with some errors.
     * @param errors A non-null list of Error objects.
     * @return The Metadata object with the errors.
     */
    public static Metadata of(Error... errors) {
        Metadata meta = new Metadata();
        meta.addErrors(Arrays.asList(errors));
        return meta;
    }

    /**
     * Static construction of Metadata with some errors.
     * @param errors A non-null list of Error objects.
     * @return The Metadata object with the errors.
     */
    public static Metadata of(List<Error> errors) {
        Metadata meta = new Metadata();
        meta.addErrors(errors);
        return meta;
    }

    /**
     * Merge another Metadata into this Metadata.
     * @param metadata A Metadata to merge.
     * @return This Object after the merge.
     */
    public Metadata merge(Metadata metadata) {
        if (metadata != null) {
            this.meta.putAll(metadata.asMap());
        }
        return this;
    }
}
