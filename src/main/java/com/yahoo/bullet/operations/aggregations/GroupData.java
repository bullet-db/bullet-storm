/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.operations.AggregationOperations;
import com.yahoo.bullet.operations.AggregationOperations.AggregationOperator;
import com.yahoo.bullet.operations.AggregationOperations.GroupOperationType;
import com.yahoo.bullet.record.BulletRecord;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.AVG;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.COUNT_FIELD;

/**
 * This class represents the results of a GroupOperations. The result is always a {@link Number}, so
 * that is what this class stores. It is {@link Serializable} and provides convenience static methods to
 * manually perform serialization {@link #toBytes(GroupData)} and deserialization {@link #fromBytes(byte[])}
 *
 * It can compute all the operations if presented with a {@link BulletRecord}, merge other GroupData and
 * present the results of the operations as a BulletRecord.
 */
@Slf4j
public class GroupData implements Serializable {
    public static final long serialVersionUID = 387461949277948303L;

    public static final String NAME_SEPARATOR = "_";

    private Map<GroupOperation, Number> metrics = new HashMap<>();

    /**
     * Constructor that initializes the GroupData with a {@link Set} of {@link GroupOperation}.
     *
     * @param operations the non-null operations that this will compute metrics for.
     */
    public GroupData(Set<GroupOperation> operations) {
        // Initialize with nulls.
        for (GroupOperation operation : operations) {
            metrics.put(operation, null);
            if (operation.getType() == AVG) {
                // For AVG we store an addition COUNT_FIELD operation to store the count (the sum is stored in AVG)
                metrics.put(new GroupOperation(COUNT_FIELD, operation.getField(), null), null);
            }
        }
    }

    /**
     * Consumes the given {@link BulletRecord} and computes group operation metrics.
     *
     * @param data The record to compute metrics for.
     */
    public void consume(BulletRecord data) {
        metrics.entrySet().stream().forEach(e -> consume(e, data));
    }

    /**
     * Merges the serialized form of a GroupData into this. For all GroupOperations present, their corresponding
     * values will be merged according to their respective additive operation.
     *
     * @param serializedGroupData the serialized bytes of a GroupData.
     */
    public void combine(byte[] serializedGroupData) {
        GroupData otherMetric = GroupData.fromBytes(serializedGroupData);
        if (otherMetric == null) {
            log.error("Could not create a GroupData. Skipping...");
            return;
        }
        combine(otherMetric);
    }

    /**
     * Merge a GroupData into this. For all GroupOperations present, their corresponding values will be
     * merged according to their respective additive operation.
     *
     * @param otherData The other GroupData to merge.
     */
    public void combine(GroupData otherData) {
        metrics.entrySet().stream().forEach(e -> combine(e, otherData));
    }

    /**
     * Gets the data stored for the group as a {@link BulletRecord}.
     *
     * @return A non-null {@link BulletRecord} containing the data stored in this object.
     */
    public BulletRecord getAsBulletRecord() {
        BulletRecord record = new BulletRecord();
        metrics.entrySet().stream().forEach(e -> addToRecord(e, record));
        return record;
    }

    private void consume(Map.Entry<GroupOperation, Number> metric, BulletRecord data) {
        GroupOperation operation = metric.getKey();
        Object value = data.get(operation.getField());
        switch (operation.getType()) {
            case MIN:
                updateMetric(value, metric, AggregationOperations.MIN);
                break;
            case MAX:
                updateMetric(value, metric, AggregationOperations.MAX);
                break;
            case COUNT:
                updateMetric(1L, metric, AggregationOperations.COUNT);
                break;
            case COUNT_FIELD:
                updateMetric(value != null ? 1L : null, metric, AggregationOperations.COUNT);
                break;
            case SUM:
            case AVG:
                updateMetric(value, metric, AggregationOperations.SUM);
                break;
        }
    }

    private void combine(Map.Entry<GroupOperation, Number> metric, GroupData otherData) {
        GroupOperation operation = metric.getKey();
        Number value = otherData.metrics.get(metric.getKey());
        switch (operation.getType()) {
            case MIN:
                updateMetric(value, metric, AggregationOperations.MIN);
                break;
            case MAX:
                updateMetric(value, metric, AggregationOperations.MAX);
                break;
            case SUM:
            case AVG:
                updateMetric(value, metric, AggregationOperations.SUM);
                break;
            case COUNT:
            case COUNT_FIELD:
                updateMetric(value, metric, AggregationOperations.COUNT);
                break;
        }
    }

    private void addToRecord(Map.Entry<GroupOperation, Number> metric, BulletRecord record) {
        GroupOperation operation = metric.getKey();
        Number value = metric.getValue();
        switch (operation.getType()) {
            case COUNT:
                record.setLong(getResultName(operation), value == null ? 0 : value.longValue());
                break;
            case AVG:
                record.setDouble(getResultName(operation), calculateAvg(value, operation.getField()));
                break;
            case COUNT_FIELD:
                break;
            case MIN:
            case MAX:
            case SUM:
                record.setDouble(getResultName(operation), value == null ? null : value.doubleValue());
                break;
        }
    }

    private Double calculateAvg(Number sum, String field) {
        Number count = metrics.get(new GroupOperation(COUNT_FIELD, field, null));
        if (sum == null || count == null) {
            return null;
        }
        return sum.doubleValue() / count.longValue();
    }

    /**
     * Returns the name of the result field to use for the given {@link GroupOperation}. If the operation
     * does specify a newName, it will be returned. Otherwise, a composite name containing the type of the
     * operation as well as the field name will be used (if provided).
     *
     * @param operation The operation to get the name for.
     * @return a String representing a name for the result of the operation.
     */
    public static String getResultName(GroupOperation operation) {
        String name = operation.getNewName();
        if (name != null) {
            return name;
        }
        GroupOperationType type = operation.getType();
        String field = operation.getField();
        if (field == null) {
            return type.getName();
        }
        return type.getName() + NAME_SEPARATOR + operation.getField();
    }

    /**
     * Convenience method to deserialize an instance from raw serialized data produced by {@link #toBytes(GroupData)}.
     *
     * @param data The raw serialized byte[] representing the data.
     * @return A reified object or null if not successful.
     */
    public static GroupData fromBytes(byte[] data) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return (GroupData) ois.readObject();
        } catch (IOException | ClassNotFoundException | RuntimeException e) {
            log.error("Could not reify a GroupData from raw data {}", data);
            log.error("Exception when parsing GroupData", e);
        }
        return null;
    }

    /**
     * Convenience method to serializes the given GroupData to raw byte[].
     *
     * @param metric The GroupData to serialize.
     * @return the serialized byte[] or null if not successful.
     */
    public static byte[] toBytes(GroupData metric) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(metric);
            oos.close();
            return bos.toByteArray();
        } catch (IOException | RuntimeException e) {
            log.error("Could not serialize given GroupData contents", metric.metrics);
            log.error("Exception when serializing GroupData", e);
        }
        return null;
    }

    /*
     * This function accepts an AggregationOperator and applies it to the new and current value for the given
     * GroupOperation and updates metrics accordingly.
     */
    private void updateMetric(Object object, Map.Entry<GroupOperation, Number> metric, AggregationOperator operator) {
        // Also catches null.
        if (object instanceof Number) {
            Number current = metric.getValue();
            Number number = (Number) object;
            metrics.put(metric.getKey(), current == null ? number : operator.apply(number, current));
        }
    }
}
