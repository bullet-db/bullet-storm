/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

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
        // TODO Could do the identity for each operation instead of null
        // Initialize with nulls.
        operations.stream().forEach(o -> metrics.put(o, null));
    }

    /**
     * Computes and adds metrics from the given {@link BulletRecord} for the {@link GroupOperation} defined.
     *
     * @param data The record to compute metrics for.
     */
    public void compute(BulletRecord data) {
        metrics.entrySet().stream().forEach(e -> compute(e, data));
    }

    /**
     * Merges the serialized form of a GroupData into this. For all GroupOperations present, their corresponding
     * values will be merged according to their respective additive operation.
     *
     * @param serializedGroupData the serialized bytes of a GroupData.
     */
    public void merge(byte[] serializedGroupData) {
        GroupData otherMetric = GroupData.fromBytes(serializedGroupData);
        if (otherMetric == null) {
            log.error("Could not create a GroupData. Skipping...");
            return;
        }
        merge(otherMetric);
    }

    /**
     * Merge a GroupData into this. For all GroupOperations present, their corresponding values will be
     * merged according to their respective additive operation.
     *
     * @param otherData The other GroupData to merge.
     */
    public void merge(GroupData otherData) {
        metrics.entrySet().stream().forEach(e -> merge(e, otherData));
    }

    /**
     * Gets the data stored for the group as a {@link BulletRecord}.
     *
     * @return A non-null {@link BulletRecord} containing the data stored in this object.
     */
    public BulletRecord getAsBulletRecord() {
        BulletRecord record = new BulletRecord();
        metrics.entrySet().stream().forEach(e -> update(e, record));
        return record;
    }

    private Number getOrDefault(GroupOperation operation, Number defaultValue) {
        // Can't use metrics.getOrDefault since that checks for containsKey - we put explicit nulls into it
        Number value = metrics.get(operation);
        return value == null ? defaultValue : value;
    }

    private void compute(Map.Entry<GroupOperation, Number> metric, BulletRecord data) {
        GroupOperation operation = metric.getKey();
        switch (operation.getType()) {
            case COUNT:
                incrementCount(operation);
                break;
            default:
                // These other cases can't happen yet
                break;
        }
    }

    private void merge(Map.Entry<GroupOperation, Number> metric, GroupData otherData) {
        GroupOperation operation = metric.getKey();
        switch (operation.getType()) {
            case COUNT:
                incrementCount(operation, otherData.getCount(operation));
                break;
            default:
                // These other cases can't happen yet
                break;
        }
    }

    private void update(Map.Entry<GroupOperation, Number> metric, BulletRecord record) {
        GroupOperation operation = metric.getKey();
        switch (operation.getType()) {
            case COUNT:
                record.setLong(getResultName(operation), getCount(operation));
                break;
            default:
                // These other cases can't happen yet
                break;
        }
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

    /* ======================== OPERATION IMPLEMENTATIONS ============================ */

    private void incrementCount(GroupOperation operation) {
        incrementCount(operation, 1L);
    }

    private void incrementCount(GroupOperation operation, Long by) {
        Number count = getOrDefault(operation, 0L);
        metrics.put(operation, count.longValue() + by);
    }

    private Long getCount(GroupOperation operation) {
        Number count = getOrDefault(operation, 0L);
        return count.longValue();
    }

}
