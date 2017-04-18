package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.Utilities;
import com.yahoo.bullet.operations.aggregations.sketches.FrequentItemsSketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.sketches.frequencies.ErrorType;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public class TopK extends SketchingStrategy<FrequentItemsSketch> {
    public static final int MAX_MAP_ENTRIES = 1024;

    public static final String NO_FALSE_NEGATIVES = "NFN";
    public static final String NO_FALSE_POSITIVES = "NFP";
    public static final String DEFAULT_ERROR_TYPE = NO_FALSE_NEGATIVES;

    public static final String THRESHOLD_FIELD = "threshold";

    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public TopK(Aggregation aggregation) {
        super(aggregation);

        int maxMapSize = ((Number) config.getOrDefault(BulletConfig.TOP_K_AGGREGATION_SKETCH_ENTRIES,
                                                       MAX_MAP_ENTRIES)).intValue();

        String errorConfiguration = (config.getOrDefault(BulletConfig.TOP_K_AGGREGATION_SKETCH_ERROR_TYPE,
                                                         DEFAULT_ERROR_TYPE)).toString();

        ErrorType errorType = getErrorType(errorConfiguration);

        Long threshold = getThreshold(aggregation.getAttributes());

        sketch = threshold != null ? new FrequentItemsSketch(errorType, maxMapSize, threshold) :
                                     new FrequentItemsSketch(errorType, maxMapSize);
    }

    @Override
    public List<Error> initialize() {
        if (Utilities.isEmpty(fields)) {
            return singletonList(REQUIRES_FIELD_ERROR);
        }
        return null;
    }

    @Override
    public void consume(BulletRecord data) {
        sketch.update(composeField(data));
    }

    @Override
    public Clip getAggregation() {
        Clip result = super.getAggregation();
        result.getRecords().forEach(this::splitFields);
        return result;
    }

    private void splitFields(BulletRecord record) {
        String field = record.get(FrequentItemsSketch.ITEM_FIELD).toString();
        List<String> values = decomposeField(field);
        for (int i = 0; i < fields.size(); ++i) {
            record.setString(fields.get(i), values.get(i));
        }
        record.remove(FrequentItemsSketch.ITEM_FIELD);
    }

    private static Long getThreshold(Map<String, Object> attributes)  {
        if (Utilities.isEmpty(attributes)) {
            return null;
        }
        return Utilities.getCasted(attributes, THRESHOLD_FIELD, Long.class);
    }

    private static ErrorType getErrorType(String errorType) {
        if (NO_FALSE_POSITIVES.equals(errorType)) {
            return ErrorType.NO_FALSE_POSITIVES;
        }
        return ErrorType.NO_FALSE_NEGATIVES;
    }
}
