package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.Utilities;
import com.yahoo.bullet.operations.aggregations.grouping.CachingGroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.operations.aggregations.sketches.TupleSketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.parsing.Specification;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.sketches.ResizeFactor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

/**
 * This {@link Strategy} implements a Tuple Sketch based approach to doing a group by. In particular, it
 * provides a uniform sample of the groups if the number of unique groups exceed the Sketch size. Metrics like
 * sum and count when summed across the uniform sample and divided the sketch theta gives an approximate estimate
 * of the total sum and count across all the groups.
 */
public class GroupBy extends KMVStrategy<TupleSketch> {
    // This is reused for the duration of the strategy.
    private final CachingGroupData container;

    // 13.27% error rate at 99.73% confidence (3 SD). Irrelevant since we are using this to cap the number of groups.
    public static final int DEFAULT_NOMINAL_ENTRIES = 512;

    private Set<GroupOperation> operations;

    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    public GroupBy(Aggregation aggregation) {
        super(aggregation);

        Map<String, Object> attributes = aggregation.getAttributes();
        operations = GroupOperation.getOperations(attributes);
        Map<GroupOperation, Number> metrics = GroupData.makeInitialMetrics(operations);
        container = new CachingGroupData(null, metrics);

        ResizeFactor resizeFactor = getResizeFactor(BulletConfig.GROUP_AGGREGATION_SKETCH_RESIZE_FACTOR);
        float samplingProbability = ((Number) config.getOrDefault(BulletConfig.GROUP_AGGREGATION_SKETCH_SAMPLING,
                                                                  DEFAULT_SAMPLING_PROBABILITY)).floatValue();
        int nominalEntries = ((Number) config.getOrDefault(BulletConfig.GROUP_AGGREGATION_SKETCH_ENTRIES,
                                                           DEFAULT_NOMINAL_ENTRIES)).intValue();
        int size = aggregation.getSize();
        Map<String, String> fieldsToNames = aggregation.getFields();

        sketch = new TupleSketch(resizeFactor, samplingProbability, nominalEntries, size, fieldsToNames);
    }

    @Override
    public List<Error> initialize() {
        if (Utilities.isEmpty(operations) && Utilities.isEmpty(fields)) {
            return singletonList(GroupOperation.REQUIRES_FIELD_OR_OPERATION_ERROR);
        }
        return GroupOperation.checkOperations(operations);
    }

    @Override
    public void consume(BulletRecord data) {
        Map<String, String> fieldToValues = getGroups(data);
        String key = getFieldsAsString(fields, fieldToValues, separator);

        // Set the record and the group values into the container. The metrics are already initialized.
        container.setCachedRecord(data);
        container.setGroupFields(fieldToValues);
        sketch.update(key, container);
    }

    private static String getFieldsAsString(List<String> fields, Map<String, String> mapping, String separator) {
        return fields.stream().map(mapping::get).collect(Collectors.joining(separator));
    }

    private Map<String, String> getGroups(BulletRecord record) {
        Map<String, String> groupMapping = new HashMap<>();
        for (String key : fields) {
            // This explicitly does not do a TypedObject checking. Nulls (and everything else) turn into Strings
            String value = Objects.toString(Specification.extractField(key, record));
            groupMapping.put(key, value);
        }
        return groupMapping;
    }
}
