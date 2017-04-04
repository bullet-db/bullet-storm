package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.Utilities;
import com.yahoo.bullet.operations.AggregationOperations.DistributionType;
import com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.record.BulletRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.yahoo.bullet.parsing.Error.makeError;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

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
    public static final String NUMBER_OF_POINTS = "numberOfPoints";

    private final int entries;
    private final int maxPoints;
    // Copy of the aggregation
    private Aggregation aggregation;

    public static final Set<DistributionType> SUPPORTED_DISTRIBUTION_TYPES =
            new HashSet<>(asList(DistributionType.QUANTILE, DistributionType.CDF, DistributionType.PMF));

    public static final Error REQUIRES_POINTS_ERROR =
            makeError("The DISTRIBUTION type requires at least one point",
                      "Please add a list of numeric points, or specify a number of equidistant points to generate" +
                      "or specify a start, end and increment to generate points");

    public static final Error REQUIRES_ONE_FIELD_ERROR =
            makeError("The aggregation type requires exactly one field", "Please add exactly one field to fields");
    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public Distribution(Aggregation aggregation) {
        super(aggregation);
        entries = ((Number) config.getOrDefault(BulletConfig.DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES,
                                                DEFAULT_ENTRIES)).intValue();
        maxPoints = ((Number) config.getOrDefault(BulletConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS,
                                                  DEFAULT_MAX_POINTS)).intValue();
        this.aggregation = aggregation;

        // The sketch is initialized in validate!
    }

    @Override
    public void consume(BulletRecord data) {
    }

    private static QuantileSketch getSketch(int entries, Integer maxPoints, Map<String, Object> attributes) {
        int equidistantPoints = getNumberOfEquidistantPoints(attributes);
        if (equidistantPoints > 0) {
            return new QuantileSketch(entries, Math.min(equidistantPoints, maxPoints));
        }
        List<Double> points = getProvidedPoints(attributes);
        if (Utilities.isEmpty(points)) {
            points = generatePoints(attributes);
        }

        // If still not good, return null
        if (Utilities.isEmpty(points)) {
            return null;
        }
        // Sort and get first maxPoints distinct values
        double[] cleanedPoints = points.stream().distinct().sorted().limit(maxPoints)
                                       .mapToDouble(d -> d).toArray();
        return new QuantileSketch(entries, cleanedPoints);
    }

    // Point generation methods

    private static List<Double> getProvidedPoints(Map<String, Object> attributes) {
        try {
            List<Double> points = Utilities.getCasted(attributes, POINTS);
            if (!Utilities.isEmpty(points)) {
                return points;
            }
        } catch (ClassCastException ignored) {
        }
        return Collections.emptyList();
    }

    private static List<Double> generatePoints(Map<String, Object> attributes) {
        Number start = Utilities.getCasted(attributes, RANGE_START);
        Number end = Utilities.getCasted(attributes, RANGE_END);
        Number increment = Utilities.getCasted(attributes, RANGE_INCREMENT);

        if (!areNumbersValid(start, end, increment)) {
            return Collections.emptyList();
        }
        Double from = start.doubleValue();
        Double to = end.doubleValue();
        Double by = increment.doubleValue();
        List<Double> points = new ArrayList<>();
        while (from <= to) {
            points.add(from);
            from += by;
        }
        return points;
    }

    private static int getNumberOfEquidistantPoints(Map<String, Object> attributes) {
        Number equidistantPoints = Utilities.getCasted(attributes, NUMBER_OF_POINTS);
        if (equidistantPoints == null) {
            return 0;
        }
        return equidistantPoints.intValue();
    }

    private static boolean areNumbersValid(Number start, Number end, Number increment) {
        if (start == null || end == null || increment == null) {
            return false;
        }
        return !(start.doubleValue() >= end.doubleValue() || increment.doubleValue() <= 0);
    }

    @Override
    public List<Error> validate() {
        Map<String, String> fields = aggregation.getFields();
        if (Utilities.isEmpty(fields) || fields.size() != 1) {
            return singletonList(REQUIRES_ONE_FIELD_ERROR);
        }

        Map<String, Object> attributes = aggregation.getAttributes();
        // Try to initialize sketch now
        sketch = getSketch(entries, maxPoints, attributes);
        return sketch == null ? singletonList(REQUIRES_POINTS_ERROR) : null;
    }

}
