/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.querying.Querier;
import com.yahoo.bullet.record.BulletRecord;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

/**
 * This categorizes running queries into whether they are done, closed or have exceeded the rate limits. Running queries
 * are provided as a {@link Map} of String query IDs to non-null, valid, initialized {@link Querier} objects. Use
 * {@link #categorize(Map, Predicate)} for categorizing such queries. The Predicate is used to test if the Querier is
 * closed or not. The {@link #categorize(BulletRecord, Map)} method assumes that the data is partitioned and will
 * use {@link Querier#isClosedForPartition()} after  making the Querier instances {@link Querier#consume(BulletRecord)}.
 */
@Getter @Slf4j
public class QueryCategorizer {
    private Map<String, Querier> rateLimited = new HashMap<>();
    private Map<String, Querier> closed = new HashMap<>();
    private Map<String, Querier> done = new HashMap<>();

    /**
     * Categorize the given {@link Map} of query IDs to {@link Querier} instances.
     *
     * @param queries The queries to categorize.
     * @param isClosed A {@link Predicate} used to test if the {@link Querier} is closed. If the data the querier is
     *                 is seeing is partitioned, you should use {@link Querier#isClosedForPartition()} otherwise you
     *                 use {@link Querier#isClosed()}.
     * @return This object for chaining.
     */
    public QueryCategorizer categorize(Map<String, Querier> queries, Predicate<Querier> isClosed) {
        queries.entrySet().forEach(e -> classify(e, isClosed));
        return this;
    }

    /**
     * Categorize the given {@link Map} of query IDs to {@link Querier} instances after consuming the given record.
     * Assumes that the queries are seeing a partition of the data and uses {@link Querier#isClosedForPartition()} to
     * check if the queriers are closed.
     *
     * @param record The {@link BulletRecord} to consume first.
     * @param queries The queries to categorize.
     * @return This object for chaining.
     */
    public QueryCategorizer categorize(BulletRecord record, Map<String, Querier> queries) {
        for (Map.Entry<String, Querier> query : queries.entrySet()) {
            query.getValue().consume(record);
            classify(query, Querier::isClosedForPartition);
        }
        return this;
    }

    private void classify(Map.Entry<String, Querier> query, Predicate<Querier> isClosed) {
        String id = query.getKey();
        Querier querier = query.getValue();
        if (querier.isDone()) {
            done.put(id, querier);
        } else if (querier.isExceedingRateLimit()) {
            rateLimited.put(id, querier);
        } else if (isClosed.test(querier)) {
            closed.put(id, querier);
        }
    }
}
