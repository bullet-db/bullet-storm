/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.TestHelpers;
import com.yahoo.bullet.operations.AggregationOperations;
import com.yahoo.bullet.operations.AggregationOperations.GroupOperationType;
import com.yahoo.bullet.operations.aggregations.CountDistinct;
import com.yahoo.bullet.operations.aggregations.GroupAll;
import com.yahoo.bullet.operations.aggregations.GroupBy;
import com.yahoo.bullet.operations.aggregations.Raw;
import com.yahoo.bullet.operations.aggregations.grouping.GroupOperation;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.COUNT_DISTINCT;
import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.GROUP;
import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.PERCENTILE;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.COUNT;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.COUNT_FIELD;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.SUM;
import static com.yahoo.bullet.parsing.AggregationUtils.makeAttributes;
import static com.yahoo.bullet.parsing.AggregationUtils.makeGroupOperation;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class AggregationTest {
    @Test
    public void testSize() {
        Aggregation aggregation = new Aggregation();

        aggregation.setSize(null);
        aggregation.configure(emptyMap());
        Assert.assertEquals(aggregation.getSize(), Aggregation.DEFAULT_SIZE);

        aggregation.setSize(-10);
        aggregation.configure(emptyMap());
        Assert.assertEquals(aggregation.getSize(), Aggregation.DEFAULT_SIZE);

        aggregation.setSize(0);
        aggregation.configure(emptyMap());
        Assert.assertEquals(aggregation.getSize(), (Integer) 0);

        aggregation.setSize(1);
        aggregation.configure(emptyMap());
        Assert.assertEquals(aggregation.getSize(), (Integer) 1);

        aggregation.setSize(Aggregation.DEFAULT_SIZE);
        aggregation.configure(emptyMap());
        Assert.assertEquals(aggregation.getSize(), Aggregation.DEFAULT_SIZE);

        aggregation.setSize(Aggregation.DEFAULT_MAX_SIZE + 1);
        aggregation.configure(emptyMap());
        Assert.assertEquals(aggregation.getSize(), Aggregation.DEFAULT_MAX_SIZE);
    }

    @Test
    public void testConfiguredSize() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.AGGREGATION_DEFAULT_SIZE, 10);
        config.put(BulletConfig.AGGREGATION_MAX_SIZE, 200);

        Aggregation aggregation = new Aggregation();

        aggregation.setSize(null);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);

        aggregation.setSize(-10);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);

        aggregation.setSize(0);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 0);

        aggregation.setSize(1);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 1);

        aggregation.setSize(10);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);

        aggregation.setSize(200);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 200);

        aggregation.setSize(4000);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 200);
    }

    @Test
    public void testFailValidateOnNoType() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(null);
        List<Error> errors = aggregation.validate().get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0).getError(), Aggregation.TYPE_NOT_SUPPORTED_ERROR_PREFIX);
    }

    @Test
    public void testFailValidateOnUnknownType() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(PERCENTILE);
        List<Error> errors = aggregation.validate().get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0).getError(), Aggregation.TYPE_NOT_SUPPORTED_ERROR_PREFIX + ": PERCENTILE");
    }

    @Test
    public void testSuccessfulValidate() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        aggregation.setAttributes(makeAttributes(makeGroupOperation(COUNT, null, "count")));

        aggregation.configure(emptyMap());
        Assert.assertFalse(aggregation.validate().isPresent());
    }

    @Test
    public void testValidateNoField() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        aggregation.setAttributes(makeAttributes(makeGroupOperation(SUM, null, null)));
        aggregation.configure(emptyMap());

        List<Error> errors = aggregation.validate().get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0).getError(), Aggregation.GROUP_OPERATION_REQUIRES_FIELD + SUM);
    }

    @Test
    public void testUnsupportedOperation() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        aggregation.setAttributes(makeAttributes(makeGroupOperation(COUNT_FIELD, "someField", "myCountField")));
        aggregation.configure(emptyMap());

        Set<GroupOperation> operations = aggregation.getGroupOperations();
        Assert.assertEquals(operations.size(), 0);
    }

    @Test
    public void testAttributeOperationMissing() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        aggregation.setAttributes(singletonMap(Aggregation.OPERATIONS, null));

        // Missing attribute operations should be silently ignored
        Assert.assertNull(aggregation.getGroupOperations());
        aggregation.configure(emptyMap());
        Assert.assertNotNull(aggregation.getGroupOperations());
        Assert.assertTrue(aggregation.getGroupOperations().isEmpty());
    }

    @Test
    public void testAttributeOperationBadFormat() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        aggregation.setAttributes(singletonMap(Aggregation.OPERATIONS, asList("foo")));

        Assert.assertNull(aggregation.getGroupOperations());
        aggregation.configure(emptyMap());
        Assert.assertNotNull(aggregation.getGroupOperations());
        Assert.assertTrue(aggregation.getGroupOperations().isEmpty());
    }

    @Test
    public void testAttributeOperationsUnknownOperation() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        aggregation.setAttributes(makeAttributes(makeGroupOperation(COUNT, null, "bar"),
                                                 makeGroupOperation(COUNT_FIELD, "foo", "foo_avg")));

        Assert.assertNull(aggregation.getGroupOperations());
        aggregation.configure(emptyMap());

        Set<GroupOperation> operations = aggregation.getGroupOperations();
        Assert.assertEquals(operations.size(), 1);
        GroupOperation operation = operations.stream().findFirst().get();
        Assert.assertEquals(operation.getType(), COUNT);
        Assert.assertNull(operation.getField());
        Assert.assertEquals(operation.getNewName(), "bar");
    }

    @Test
    public void testAttributeOperationsDuplicateOperation() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        // TODO Once other operations are supported, use them and not COUNT with fake fields as a proxy.
        aggregation.setAttributes(makeAttributes(makeGroupOperation(COUNT, null, null),
                                                 makeGroupOperation(COUNT, "foo", null),
                                                 makeGroupOperation(COUNT, null, null),
                                                 makeGroupOperation(COUNT, "bar", null),
                                                 makeGroupOperation(COUNT, "foo", null),
                                                 makeGroupOperation(COUNT, "bar", null)));


        Assert.assertNull(aggregation.getGroupOperations());
        aggregation.configure(emptyMap());
        Set<GroupOperation> operations = aggregation.getGroupOperations();

        Assert.assertEquals(operations.size(), 3);
        TestHelpers.assertContains(operations, new GroupOperation(COUNT, null, null));
        TestHelpers.assertContains(operations, new GroupOperation(COUNT, "foo", null));
        TestHelpers.assertContains(operations, new GroupOperation(COUNT, "bar", null));
    }

    @Test
    public void testFailValidateOnCountDistinctFieldsMissing() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(COUNT_DISTINCT);
        aggregation.configure(emptyMap());

        List<Error> errors = aggregation.validate().get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Aggregation.REQUIRES_FIELD_ERROR);

    }

    @Test
    public void testToString() {
        Aggregation aggregation = new Aggregation();
        aggregation.configure(emptyMap());

        Assert.assertEquals(aggregation.toString(), "{size: 1, type: RAW, fields: null, attributes: null}");

        aggregation.setType(COUNT_DISTINCT);
        Assert.assertEquals(aggregation.toString(), "{size: 1, type: COUNT_DISTINCT, fields: null, attributes: null}");

        aggregation.setFields(singletonMap("field", "newName"));
        Assert.assertEquals(aggregation.toString(),
                            "{size: 1, type: COUNT_DISTINCT, " + "fields: {field=newName}, attributes: null}");

        aggregation.setAttributes(singletonMap("foo", asList(1, 2, 3)));
        Assert.assertEquals(aggregation.toString(),
                "{size: 1, type: COUNT_DISTINCT, " + "fields: {field=newName}, attributes: {foo=[1, 2, 3]}}");
    }

    @Test
    public void testUnimplementedStrategies() {
        Aggregation aggregation = new Aggregation();

        aggregation.setType(AggregationOperations.AggregationType.PERCENTILE);
        aggregation.configure(Collections.emptyMap());
        Assert.assertNull(aggregation.getStrategy());

        aggregation.setType(AggregationOperations.AggregationType.TOP);
        aggregation.configure(Collections.emptyMap());
        Assert.assertNull(aggregation.getStrategy());
    }

    @Test
    public void testRawStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.configure(Collections.emptyMap());

        Assert.assertEquals(aggregation.findStrategy().getClass(), Raw.class);
    }

    @Test
    public void testGroupAllStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(AggregationOperations.AggregationType.GROUP);
        aggregation.setAttributes(singletonMap(Aggregation.OPERATIONS,
                                               asList(singletonMap(Aggregation.OPERATION_TYPE,
                                                                   GroupOperationType.COUNT.getName()))));
        aggregation.configure(Collections.emptyMap());

        Assert.assertEquals(aggregation.getStrategy().getClass(), GroupAll.class);
    }

    @Test
    public void testCountDistinctStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(AggregationOperations.AggregationType.COUNT_DISTINCT);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(Collections.emptyMap());

        Assert.assertEquals(aggregation.getStrategy().getClass(), CountDistinct.class);
    }

    @Test
    public void testDistinctStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(AggregationOperations.AggregationType.GROUP);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(Collections.emptyMap());

        Assert.assertEquals(aggregation.getStrategy().getClass(), GroupBy.class);
    }

    @Test
    public void testGroupByStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(AggregationOperations.AggregationType.GROUP);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.setAttributes(singletonMap(Aggregation.OPERATIONS,
                                                asList(singletonMap(Aggregation.OPERATION_TYPE,
                                                                    GroupOperationType.COUNT.getName()))));
        aggregation.configure(Collections.emptyMap());

        Assert.assertEquals(aggregation.getStrategy().getClass(), GroupBy.class);
    }
}

