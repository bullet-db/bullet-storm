package com.yahoo.bullet.operations.aggregations.sketches;

/**
 * This class encapsulates a type of Sketch. Since one type of Sketch is used for updating and another for unioning,
 * this will encapsulate both of them and provide methods to serialize, union and collect results.
 */
public interface Sketch {
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

}
