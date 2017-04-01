package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.aggregations.sketches.KMVSketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.result.Metadata.Concept;
import com.yahoo.sketches.ResizeFactor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The parent class for {@link SketchingStrategy} that use the KMV type of Sketch - Theta and Tuple.
 */
public abstract class KMVStrategy<S extends KMVSketch> extends SketchingStrategy<S> {
    // Common defaults for KMV type sketches
    // No Sampling
    public static final float DEFAULT_SAMPLING_PROBABILITY = 1.0f;
    // Sketch * 8 its size upto 2 * nominal entries everytime it reaches cap
    public static final int DEFAULT_RESIZE_FACTOR = ResizeFactor.X8.lg();

    // Metadata keys for Standard Deviation
    public static final String META_STD_DEV_1 = "1";
    public static final String META_STD_DEV_2 = "2";
    public static final String META_STD_DEV_3 = "3";
    public static final String META_STD_DEV_UB = "upperBound";
    public static final String META_STD_DEV_LB = "lowerBound";

    // Separator for multiple fields when inserting into the Sketch
    protected final String separator;

    // The fields being inserted into the Sketch
    protected final List<String> fields;

    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public KMVStrategy(Aggregation aggregation) {
        super(aggregation);
        separator = config.getOrDefault(BulletConfig.AGGREGATION_COMPOSITE_FIELD_SEPARATOR,
                                        Aggregation.DEFAULT_FIELD_SEPARATOR).toString();

        fields = new ArrayList<>(aggregation.getFields().keySet());
    }

    @Override
    protected Map<String, Object> getSketchMetadata(Map<String, String> conceptKeys) {
        Map<String, Object> metadata = super.getSketchMetadata(conceptKeys);
        addIfKeyNonNull(metadata, conceptKeys.get(Concept.STANDARD_DEVIATIONS.getName()),
                        () -> getStandardDeviations(sketch));
        addIfKeyNonNull(metadata, conceptKeys.get(Concept.ESTIMATED_RESULT.getName()), sketch::isEstimationMode);
        addIfKeyNonNull(metadata, conceptKeys.get(Concept.THETA.getName()), sketch::getTheta);
        return metadata;
    }

    /**
     * Gets all the standard deviations for this sketch.
     *
     * @param sketch A KMVSketch object.
     * @return A standard deviations {@link Map} containing all the standard deviations.
     */
    public static Map<String, Map<String, Double>> getStandardDeviations(KMVSketch sketch) {
        Map<String, Map<String, Double>> standardDeviations = new HashMap<>();
        standardDeviations.put(META_STD_DEV_1, getStandardDeviation(sketch, 1));
        standardDeviations.put(META_STD_DEV_2, getStandardDeviation(sketch, 2));
        standardDeviations.put(META_STD_DEV_3, getStandardDeviation(sketch, 3));
        return standardDeviations;
    }

    /**
     * Gets the standard deviation for this sketch.
     *
     * @param sketch A wrapper around a sketch that can get the standard deviations.
     * @param standardDeviation The standard deviation to get. Valid values are 1, 2, and 3.
     * @return A standard deviation {@link Map} containing the upper and lower bound.
     */
    public static Map<String, Double> getStandardDeviation(KMVSketch sketch, int standardDeviation) {
        double lowerBound = sketch.getLowerBound(standardDeviation);
        double upperBound = sketch.getUpperBound(standardDeviation);
        Map<String, Double> bounds = new HashMap<>();
        bounds.put(META_STD_DEV_LB, lowerBound);
        bounds.put(META_STD_DEV_UB, upperBound);
        return bounds;
    }

    /**
     * Converts a integer representing the resizing for Sketches into a {@link ResizeFactor}.
     *
     * @param key The key to get the configured resize factor from the configuration.
     * @return A {@link ResizeFactor} represented by the integer or {@link ResizeFactor#X8} otherwise.
     */
    @SuppressWarnings("unchecked")
    public ResizeFactor getResizeFactor(String key) {
        return getResizeFactor((Number) config.getOrDefault(key, DEFAULT_RESIZE_FACTOR));
    }
    /**
     * Converts a integer representing the resizing for Sketches into a {@link ResizeFactor}.
     *
     * @param factor An int representing the scaling when the Sketch reaches its threshold. Supports 1, 2, 4 and 8.
     * @return A {@link ResizeFactor} represented by the integer or {@link ResizeFactor#X8} otherwise.
     */
    public static ResizeFactor getResizeFactor(Number factor) {
        int resizeFactor = factor.intValue();
        switch (resizeFactor) {
            case 1:
                return ResizeFactor.X1;
            case 2:
                return ResizeFactor.X2;
            case 4:
                return ResizeFactor.X4;
            default:
                return ResizeFactor.X8;
        }
    }
}
