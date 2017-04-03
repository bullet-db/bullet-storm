package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.Utilities;
import com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This {@link Strategy} uses {@link QuantileSketch} to find distributions of a numeric field. Based on the size
 * configured for the sketch, the normalized rank error can be determined and tightly bound.
 */
public class Distribution extends SketchingStrategy<QuantileSketch> {
    public static final int DEFAULT_ENTRIES = 1024;

    public static final int DEFAULT_MAX_POINTS = 100;

    // Distribution
    public static final String POINTS = "points";
    public static final String RANGE_START = "start";
    public static final String RANGE_END = "end";
    public static final String RANGE_INCREMENT = "increment";

    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public Distribution(Aggregation aggregation) {
        super(aggregation);
        int entries = ((Number) config.getOrDefault(BulletConfig.DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES,
                                                    DEFAULT_ENTRIES)).intValue();

        Map<String, Object> attributes = aggregation.getAttributes();

        Map<String, String> fieldsToNames = aggregation.getFields();
        sketch = new QuantileSketch(entries);
    }

    @Override
    public void consume(BulletRecord data) {
    }

    /**
     * Parses a {@link List} of {@link Double} data points to compute a distribution for.
     *
     * @param attributes The Map that contains a mapping for this List.
     * @param configuration A Map of configuration to apply additional configuration.
     * @return The {@link List} of data points or a {@link Collections#emptyList()}.
     */
    @SuppressWarnings("unchecked")
    public static List<Double> getPoints(Map<String, Object> attributes, Map configuration) {
        if (Utilities.isEmpty(attributes)) {
            return Collections.emptyList();
        }

        Integer maxPoints = ((Number) configuration.getOrDefault(BulletConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS,
                                                                 DEFAULT_MAX_POINTS)).intValue();
        try {
            List<Double> points = (List<Double>) attributes.get(POINTS);
            if (Utilities.isEmpty(points)) {
                return computePoints((Double) attributes.get(RANGE_START), (Double) attributes.get(RANGE_END),
                                     (Double) attributes.get(RANGE_INCREMENT), maxPoints);
            }
            return points.subList(0, Math.min(points.size(), maxPoints));
        } catch (ClassCastException cce) {
            return Collections.emptyList();
        }
    }

    /**
     * Generates a number of equidistant (by increment) points up to maximum from start (inclusive) to end (inclusive).
     *
     * @param start The start of the range.
     * @param end The end of the range.
     * @param increment The distance between points in the range.
     * @param maximum The maximum points to generate. Points will be generated at start till maximum or end is reached.
     * @return A {@link List} of {@link Double} points.
     */
    public static List<Double> computePoints(Double start, Double end, Double increment, Integer maximum) {
        if (start == null || end == null || start > end || increment == null || increment < 0) {
            return Collections.emptyList();
        }
        List<Double> points = new ArrayList<>();
        Double point = start;
        for (int i = 0; point <= end && i < maximum; ++i) {
            points.add(point);
            point += increment;
        }
        return points;
    }
}
