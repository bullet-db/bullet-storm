package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.aggregations.sketches.KMVSketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.sketches.ResizeFactor;

import java.util.ArrayList;
import java.util.List;

/**
 * The parent class for {@link SketchingStrategy} that use the KMV type of Sketch - Theta and Tuple.
 */
public abstract class KMVStrategy<S extends KMVSketch> extends SketchingStrategy<S> {
    // Common defaults for KMV type sketches
    // No Sampling
    public static final float DEFAULT_SAMPLING_PROBABILITY = 1.0f;
    // Sketch * 8 its size upto 2 * nominal entries everytime it reaches cap
    public static final int DEFAULT_RESIZE_FACTOR = ResizeFactor.X8.lg();

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
