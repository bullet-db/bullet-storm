/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import lombok.Getter;
import lombok.Setter;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.yahoo.bullet.storm.TopologyConstants.ERROR_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.FILTER_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.FILTER_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.JOIN_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.JOIN_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.META_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.QUERY_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.QUERY_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.RECORD_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.TICK_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.TICK_STREAM;

public class TupleClassifier {
    /**
     * Enumerated types of tuples that are checked for in the topology.
     */
    @Getter
    public enum Type {
        TICK_TUPLE(TICK_COMPONENT, TICK_STREAM),
        QUERY_TUPLE(QUERY_COMPONENT, QUERY_STREAM),
        META_TUPLE(null, META_STREAM),
        FILTER_TUPLE(FILTER_COMPONENT, FILTER_STREAM),
        RECORD_TUPLE(RECORD_COMPONENT, null),
        ERROR_TUPLE(FILTER_COMPONENT, ERROR_STREAM),
        JOIN_TUPLE(JOIN_COMPONENT, JOIN_STREAM),
        UNKNOWN_TUPLE("", "");

        private String stream;
        private String component;

        Type(String component, String stream) {
            this.component = component;
            this.stream = stream;
        }

        /**
         * Returns true iff the given tuple is of this Type.
         *
         * @param tuple The tuple to check for.
         * @return A boolean denoting whether this tuple is of this Type.
         */
        public boolean isMe(Tuple tuple) {
            return checkField(component, tuple.getSourceComponent()) && checkField(stream, tuple.getSourceStreamId());
        }

        private boolean checkField(String field, String value) {
            return field == null || value.equals(field);
        }
    }

    private static final Set<Type> VALID_TYPES = new HashSet<>(Arrays.asList(Type.values()));
    static {
        VALID_TYPES.remove(Type.UNKNOWN_TUPLE);
        VALID_TYPES.remove(Type.RECORD_TUPLE);
    }

    @Setter
    private String recordComponent = TopologyConstants.RECORD_COMPONENT;

    /**
     * Returns the {@link TupleClassifier.Type} of this tuple. If you have a custom record component name, you should
     * use {@link #setRecordComponent(String)}. Otherwise, {@link TopologyConstants#RECORD_COMPONENT} is used. The type
     * returned will never be a {@link Type#UNKNOWN_TUPLE}. See {@link #classifyNonRecord(Tuple)} if you know you will
     * not be passing in {@link Type#RECORD_TUPLE} tuples.
     *
     * @param tuple The tuple whose type is needed.
     * @return An {@link Optional} {@link TupleClassifier.Type} for the tuple.
     */
    public Optional<Type> classify(Tuple tuple) {
        return recordComponent.equals(tuple.getSourceComponent()) ? Optional.of(Type.RECORD_TUPLE) : classifyNonRecord(tuple);
    }

    /**
     * Returns the {@link Type} of this tuple. It will never be a {@link Type#RECORD_TUPLE} or a
     * {@link Type#UNKNOWN_TUPLE}. See {@link #classify(Tuple)} for full classification.
     *
     * @param tuple The tuple whose type is needed.
     * @return An {@link Optional} {@link Type} for the tuple.
     */
    public Optional<Type> classifyNonRecord(Tuple tuple) {
        return VALID_TYPES.stream().filter(x -> x.isMe(tuple)).findFirst();
    }
}
