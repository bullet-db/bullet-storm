/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.tracing;

/**
 * Encapsulates the concept of a Rule. A Rule can consume a type T as data and supply another type R as results.
 *
 * @param <T> The type of the data that this Rule can consume.
 * @param <R> The type of the data that this Rule produces.
 */
public interface Rule<T, R> {
    /**
     * Returns true iff the Rule has been satisfied for this data.
     *
     * @param data The data to consume.
     *
     * @return true if the consumed data caused the Rule to become satisfied.
     */
    boolean consume(T data);

    /**
     * Gets the data as processed so far. Depending on the rule, {@link #consume(Object)} may need to be true first.
     *
     * @return The data produced by the Rule.
     */
    R getData();
}
