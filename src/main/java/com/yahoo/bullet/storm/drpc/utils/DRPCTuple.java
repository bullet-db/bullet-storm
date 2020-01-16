/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import lombok.AllArgsConstructor;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;

import java.util.List;

@AllArgsConstructor
public class DRPCTuple implements Tuple {
    private final List<Object> values;

    @Override
    public Object getValue(int i) {
        return values.get(i);
    }

    @Override
    public String getString(int i) {
        return getValue(i).toString();
    }

    @Override
    public List<Object> getValues() {
        return values;
    }

    @Override
    public int size() {
        return values.size();
    }

    // No need to implement the rest

    @Override
    public GlobalStreamId getSourceGlobalStreamId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSourceComponent() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getSourceTask() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSourceStreamId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MessageId getMessageId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public GeneralTopologyContext getContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Fields getFields() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int fieldIndex(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Object> select(Fields selector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getInteger(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long getLong(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Boolean getBoolean(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Short getShort(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Byte getByte(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Double getDouble(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Float getFloat(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBinary(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getValueByField(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getStringByField(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getIntegerByField(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long getLongByField(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Boolean getBooleanByField(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Short getShortByField(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Byte getByteByField(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Double getDoubleByField(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Float getFloatByField(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBinaryByField(String field) {
        throw new UnsupportedOperationException();
    }
}
