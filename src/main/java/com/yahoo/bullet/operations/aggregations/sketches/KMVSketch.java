package com.yahoo.bullet.operations.aggregations.sketches;

/**
 * This class provides some common metadata information for KMV Sketches - Theta and Tuple.
 */
public abstract class KMVSketch extends Sketch {
    /**
     * Gets the theta value for this sketch after the last collect. Only applicable after {@link #collect()}.
     *
     * @return A Double value that is the theta for this sketch.
     * @throws NullPointerException if collect had not been called.
     */
    public abstract Double getTheta();

    /**
     * Gets the lower bound at this standard deviation after the last collect. Only applicable after {@link #collect()}.
     *
     * @param standardDeviation The standard deviation.
     * @return A Double representing the maximum value at this standard deviation.
     * @throws NullPointerException if collect had not been called.
     */
    public abstract Double getLowerBound(int standardDeviation);

    /**
     * Gets the uppper bound at this standard deviation after the last collect. Only applicable after {@link #collect()}.
     *
     * @param standardDeviation The standard deviation.
     * @return A Double representing the minimum value at this standard deviation.
     * @throws NullPointerException if collect had not been called.
     */
    public abstract Double getUpperBound(int standardDeviation);
}
