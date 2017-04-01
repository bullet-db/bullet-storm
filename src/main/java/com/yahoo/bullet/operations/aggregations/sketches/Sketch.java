package com.yahoo.bullet.operations.aggregations.sketches;

/**
 * This class encapsulates a type of Sketch. Since one type of Sketch is used for updating and another for unioning,
 * this will encapsulate both of them and provide methods to serialize, union and collect results.
 */
public abstract class Sketch {
    protected boolean updated = false;
    protected boolean unioned = false;

    /**
     * Serializes the sketch.
     *
     * @return A byte[] representing the serialized sketch.
     */
    public abstract byte[] serialize();

    /**
     * Union a sketch serialized using {@link #serialize()} into this.
     *
     * @param serialized A sketch serialized using the serialize method.
     */
    public abstract void union(byte[] serialized);

    /**
     * Collects and gathers the data presented to the sketch.
     */
    public abstract void collect();

    /**
     * Returns a String representing the family of this sketch.
     *
     * @return The String family of this sketch.
     */
    public abstract String getFamily();

    /**
     * Returns whether this sketch was in estimation mode or not after the last collect. Only applicable after {@link #collect()}.
     *
     * @return A Boolean denoting whether this sketch was estimating.
     * @throws NullPointerException if collect had not been called.
     */
    public abstract Boolean isEstimationMode();

    /**
     * Returns the size of the Sketch in bytes. Only applicable after {@link #collect()}.
     *
     * @return An Integer representing the size of the sketch.
     * @throws NullPointerException if collect had not been called.
     */
    public abstract Integer getSize();

}
