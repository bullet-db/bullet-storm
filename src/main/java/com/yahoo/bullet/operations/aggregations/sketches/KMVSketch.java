package com.yahoo.bullet.operations.aggregations.sketches;

public interface KMVSketch {
    /**
     * Serializes the sketch.
     *
     * @return A byte[] representing the serialized sketch.
     */
    byte[] serialize();

    /**
     * Union a sketch serialized using {@link #serialize()} into this.
     *
     * @param serialized A sketch serialized using the serialize method.
     */
    void union(byte[] serialized);

    /**
     * Collects and gathers the data presented to the sketch.
     */
    void collect();

    /**
     * Returns whether this sketch was in estimation mode or not after the last collect. Only applicable after {@link #collect()}.
     *
     * @return A boolean denoting whether this sketch was estimating.
     * @throws NullPointerException if collect had not been called.
     */
    boolean isEstimationMode();

    /**
     * Gets the theta value for this sketch after the last collect. Only applicable after {@link #collect()}.
     *
     * @return A double value that is the theta for this sketch.
     * @throws NullPointerException if collect had not been called.
     */
    double getTheta();

    /**
     * Gets the lower bound at this standard deviation after the last collect. Only applicable after {@link #collect()}.
     *
     * @param standardDeviation The standard deviation.
     * @return A double representing the maximum value at this standard deviation.
     * @throws NullPointerException if collect had not been called.
     */
    double getLowerBound(int standardDeviation);

    /**
     * Gets the uppper bound at this standard deviation after the last collect. Only applicable after {@link #collect()}.
     *
     * @param standardDeviation The standard deviation.
     * @return A double representing the minimum value at this standard deviation.
     * @throws NullPointerException if collect had not been called.
     */
    double getUpperBound(int standardDeviation);
}
