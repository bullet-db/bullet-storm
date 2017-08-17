/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;

import java.util.Arrays;

import static com.yahoo.bullet.storm.TupleType.Type;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TupleUtils {
    public static Tuple makeIDTuple(Type type, Object... contents) {
        Tuple tuple = makeTuple(type, contents);
        when(tuple.getString(TopologyConstants.ID_POSITION)).thenReturn((String) contents[TopologyConstants.ID_POSITION]);
        return tuple;
    }

    private static Tuple pushInto(Tuple mocked, Object... contents) {
        when(mocked.getValues()).thenReturn(Arrays.asList(contents));
        when(mocked.size()).thenReturn(contents.length);
        for (int i = 0; i < contents.length; ++i) {
            when(mocked.getValue(i)).thenReturn(contents[i]);
            when(mocked.getString(i)).thenReturn(contents[i].toString());
        }
        return mocked;
    }

    public static Tuple makeTuple(Object... contents) {
        Tuple tuple = mock(TupleImpl.class);
        return pushInto(tuple, contents);
    }

    public static Tuple makeTuple(Type type, Object... contents) {
        return makeRawTuple(type.getComponent(), type.getStream(), contents);
    }

    public static Tuple makeRawTuple(String component, String stream, Object... contents) {
        Tuple tuple = mock(TupleImpl.class);
        when(tuple.getSourceComponent()).thenReturn(component);
        when(tuple.getSourceStreamId()).thenReturn(stream);
        return pushInto(tuple, contents);
    }
}
