/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import lombok.Getter;
import backtype.storm.tuple.Tuple;

import java.util.Optional;
import java.util.stream.Stream;

import static com.yahoo.bullet.storm.TopologyConstants.QUERY_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.QUERY_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.FILTER_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.FILTER_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.JOIN_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.JOIN_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.RECORD_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.RECORD_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.METADATA_STREAM;
import static com.yahoo.bullet.storm.TopologyConstants.TICK_COMPONENT;
import static com.yahoo.bullet.storm.TopologyConstants.TICK_STREAM;

public class TupleType {
    private static final Type[] ALL_TYPES = Type.values();

    /**
     * Enumerated types of tuples that are checked for in the topology.
     */
    @Getter
    public enum Type {
        TICK_TUPLE(TICK_COMPONENT, TICK_STREAM),
        QUERY_TUPLE(QUERY_COMPONENT, QUERY_STREAM),
        METADATA_TUPLE(QUERY_COMPONENT, METADATA_STREAM),
        FILTER_TUPLE(FILTER_COMPONENT, FILTER_STREAM),
        RECORD_TUPLE(RECORD_COMPONENT, RECORD_STREAM),
        JOIN_TUPLE(JOIN_COMPONENT, JOIN_STREAM);

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
         * @return boolean denoting whether this tuple is of this Type.
         */
        public boolean isMe(Tuple tuple) {
            return tuple.getSourceComponent().equals(component) && tuple.getSourceStreamId().equals(stream);
        }
    }

    /**
     * Returns the {@link TupleType.Type} of this tuple.
     *
     * @param tuple The tuple whose type is needed.
     * @return An optional {@link TupleType.Type} for the tuple.
     */
    public static Optional<Type> classify(Tuple tuple) {
        return Stream.of(ALL_TYPES).filter(x -> x.isMe(tuple)).findFirst();
    }
}
