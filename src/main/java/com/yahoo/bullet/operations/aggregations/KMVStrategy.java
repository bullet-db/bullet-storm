package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.aggregations.sketches.KMVSketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.result.Metadata.Concept;
import com.yahoo.sketches.ResizeFactor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The parent class for Sketching Strategies that use the KMV type of Sketch - theta and tuple.
 */
public abstract class KMVStrategy<S extends KMVSketch> implements Strategy {
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

    protected S sketch;

    // Separator for multiple fields when inserting into the Sketch
    protected final String separator;

    // The metadata concept to key mapping
    protected final Map<String, String> metadataKeys;
    // The fields being inserted into the Sketch
    protected final List<String> fields;
    // A  copy of the configuration
    protected final Map config;

    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public KMVStrategy(Aggregation aggregation) {
        config = aggregation.getConfiguration();
        metadataKeys = (Map<String, String>) config.getOrDefault(BulletConfig.RESULT_METADATA_METRICS_MAPPING,
                                                                 Collections.emptyMap());
        separator = config.getOrDefault(BulletConfig.AGGREGATION_COMPOSITE_FIELD_SEPARATOR,
                                        Aggregation.DEFAULT_FIELD_SEPARATOR).toString();

        fields = new ArrayList<>(aggregation.getFields().keySet());
    }

    @Override
    public void combine(byte[] serializedAggregation) {
        sketch.union(serializedAggregation);
    }

    @Override
    public byte[] getSerializedAggregation() {
        return sketch.serialize();
    }

    /**
     * Adds {@link Metadata} to the {@link Clip} if it is enabled.
     *
     * @param clip The clip to add the metadata to.
     * @return The original clip with or without metadata added.
     */
    protected Clip addMetadata(Clip clip) {
        String metaKey = getAggregationMetaKey();
        return metaKey == null ? clip : clip.add(new Metadata().add(metaKey, getSketchMetadata(metadataKeys)));
    }

    /**
     * Gets the common metadata for this Sketch strategy.
     *
     * @param conceptKeys The {@link Map} of {@link Concept} names to their keys.
     * @return The created {@link Map} of sketch metadata.
     */
    protected Map<String, Object> getSketchMetadata(Map<String, String> conceptKeys) {
        Map<String, Object> metadata = new HashMap<>();

        String standardDeviationsKey = conceptKeys.get(Concept.STANDARD_DEVIATIONS.getName());
        String isEstimatedKey = conceptKeys.get(Concept.ESTIMATED_RESULT.getName());
        String thetaKey = conceptKeys.get(Concept.SKETCH_THETA.getName());

        addIfKeyNonNull(metadata, standardDeviationsKey, () -> getStandardDeviations(sketch));
        addIfKeyNonNull(metadata, isEstimatedKey, sketch::isEstimationMode);
        addIfKeyNonNull(metadata, thetaKey, sketch::getTheta);

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

    /**
     * Utility function to add a key to the metadata map if the key is not null.
     *
     * @param metadata The non-null {@link Map} representing the metadata.
     * @param key The key to add if not null.
     * @param supplier A {@link Supplier} that can produce a value to add to the metadata for the key.
     */
    public static void addIfKeyNonNull(Map<String, Object> metadata, String key, Supplier<Object> supplier) {
        if (key != null) {
            metadata.put(key, supplier.get());
        }
    }

    private String getAggregationMetaKey() {
        return metadataKeys.getOrDefault(Concept.AGGREGATION_METADATA.getName(), null);
    }

}
