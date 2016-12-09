/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.record.BulletRecord;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class is the top level Bullet Rule Specification. It holds the definition of the Rule.
 */
@Getter @Setter(AccessLevel.PACKAGE)
public class Specification implements Configurable, Validatable  {
    @Expose
    private Projection projection;
    @Expose
    private List<Clause> filters;
    @Expose
    private Aggregation aggregation;
    @Expose
    private Integer duration;

    private Boolean shouldInjectTimestamp;
    private String timestampKey;

    public static final String DEFAULT_RECEIVE_TIMESTAMP_KEY = "__receive_timestamp";
    public static final Integer DEFAULT_DURATION_MS = 30 * 1000;
    public static final Integer DEFAULT_MAX_DURATION_MS = 120 * 1000;
    public static final String SUB_KEY_SEPERATOR = "\\.";

    /**
     * Default constructor. GSON recommended.
     */
    public Specification() {
        filters = null;
        // If no aggregation is provided, the default one is used. Aggregations must be present.
        aggregation = new Aggregation();
    }

    /**
     * Runs the specification on this record and returns true if this record matched its filters.
     *
     * @param record The input record.
     * @return true if this record matches this specification's filters.
    */
    public boolean filter(BulletRecord record) {
        // Add the record if we have no filters
        if (filters == null) {
            return true;
        }
        // Otherwise short circuit evaluate till the first filter fails. Filters are ANDed.
        return filters.stream().allMatch(c -> c.check(record));
    }

    /**
     * Run the specification's projections on the record.
     *
     * @param record The input record.
     * @return The projected record.
     */
    public BulletRecord project(BulletRecord record) {
        BulletRecord projected = projection != null ? projection.project(record) : record;
        return addAdditionalFields(projected);
    }

    /**
     * Presents the aggregation with a {@link BulletRecord}.
     *
     * @param record The record to insert into the aggregation.
     */
    public void aggregate(BulletRecord record) {
        aggregation.getStrategy().consume(record);
    }

    /**
     * Presents the aggregation with a serialized data representation of a prior aggregation.
     *
     * @param data The serialized data that represents a partial aggregation.
     */
    public void aggregate(byte[] data) {
        aggregation.getStrategy().combine(data);
    }

    /**
     * Checks to see if this specification is accepting more data.
     *
     * @return a boolean denoting whether more data should be presented to this specification.
     */
    public boolean isAcceptingData() {
        return aggregation.getStrategy().isAcceptingData();
    }

    /**
     * Get the aggregate matched records so far.
     *
     * @return The byte[] representation of the serialized aggregate.
     */
    public byte[] getSerializedAggregate() {
        return aggregation.getStrategy().getSerializedAggregation();
    }

    /**
     * Gets the aggregated records.
     *
     * @return a non-null {@link List} representing the aggregation.
     */
    public List<BulletRecord> getAggregate() {
        return aggregation.getStrategy().getAggregation();
    }

    /**
     * Checks to see if this specification has reached a micro-batch size.
     *
     * @return a boolean denoting whether the specification has reached a micro-batch size.
     */
    public boolean isMicroBatch() {
        return aggregation.getStrategy().isMicroBatch();
    }

    @Override
    public void configure(Map configuration) {
        if (filters != null) {
            filters.forEach(f -> f.configure(configuration));
        }
        if (projection != null) {
            projection.configure(configuration);
        }
        // Must have an aggregation
        if (aggregation == null) {
            aggregation = new Aggregation();
        }
        aggregation.configure(configuration);

        shouldInjectTimestamp = (Boolean) configuration.getOrDefault(BulletConfig.RECORD_INJECT_TIMESTAMP, false);
        timestampKey = (String) configuration.getOrDefault(BulletConfig.RECORD_INJECT_TIMESTAMP_KEY, DEFAULT_RECEIVE_TIMESTAMP_KEY);

        Number defaultDuration = (Number) configuration.getOrDefault(BulletConfig.SPECIFICATION_DEFAULT_DURATION, DEFAULT_DURATION_MS);
        Number maxDuration = (Number) configuration.getOrDefault(BulletConfig.SPECIFICATION_MAX_DURATION, DEFAULT_MAX_DURATION_MS);
        int durationDefault = defaultDuration.intValue();
        int durationMax = maxDuration.intValue();

        // Null or negative, then default, else min of duration and max.
        duration = (duration == null || duration < 0) ? durationDefault : Math.min(duration, durationMax);
    }

    private BulletRecord addAdditionalFields(BulletRecord record) {
        if (shouldInjectTimestamp) {
            record.setLong(timestampKey, System.currentTimeMillis());
        }
        return record;
    }

    /**
     * Takes a field and returns it split into subfields if necessary.
     *
     * @param field The non-null field to get.
     * @return The field split into field or subfield if it was a map field, or just the field itself.
     */
    public static String[] getFields(String field) {
        return field.split(SUB_KEY_SEPERATOR, 2);
    }

    @Override
    public Optional<List<Error>> validate() {
        List<Error> errors = new ArrayList<>();
        if (filters != null) {
            for (Clause clause : filters) {
                clause.validate().ifPresent(errors::addAll);
            }
        }
        if (projection != null) {
            projection.validate().ifPresent(errors::addAll);
        }
        if (aggregation != null) {
            aggregation.validate().ifPresent(errors::addAll);
        }
        return errors.isEmpty() ? Optional.empty() : Optional.of(errors);
    }
}
