package com.yahoo.bullet.operations.aggregations.sketches;

import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata.Concept;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.DoublesUnion;
import com.yahoo.sketches.quantiles.DoublesUnionBuilder;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

import java.util.Map;

/**
 * Wraps operations for working with a {@link DoublesSketch} - Quantile Sketch.
 */
public class QuantileSketch extends Sketch {
    private final UpdateDoublesSketch updateSketch;
    private final DoublesUnion unionSketch;
    private DoublesSketch merged;

    private double[] points;
    private int numberOfPoints;

    private QuantileSketch(int k) {
        updateSketch = new DoublesSketchBuilder().build(k);
        unionSketch = new DoublesUnionBuilder().setMaxK(k).build();
    }

    /**
     * Creates a quantile sketch with the given number of entries getting results with the given points.
     *
     * @param k A number representative of the size of the sketch.
     * @param points An array of points to get the quantiles, PMF and/or CDF for.
     */
    public QuantileSketch(int k, double[] points) {
        this(k);
        this.points = points;
    }

    /**
     * Creates a quantile sketch with the given number of entries generating results with the number of
     * points (evenly-spaced).
     *
     * @param k A number representative of the size of the sketch.
     * @param numberOfPoints A number of evenly generated points to get the data for.
     */
    public QuantileSketch(int k, int numberOfPoints) {
        this(k);
        this.numberOfPoints = numberOfPoints;
    }

    /**
     * Updates the sketch with a double.
     *
     * @param data A double to insert into the sketch.
     */
    public void update(double data) {
        updateSketch.update(data);
        updated = true;
    }

    @Override
    public void union(byte[] serialized) {
        DoublesSketch sketch = DoublesSketch.heapify(new NativeMemory(serialized));
        unionSketch.update(sketch);
        unioned = true;
    }

    @Override
    public byte[] serialize() {
        collect();
        return merged.toByteArray();
    }

    @Override
    public Clip getResult(String metaKey, Map<String, String> conceptKeys) {
        Clip data = super.getResult(metaKey, conceptKeys);
        return data;
    }

    @Override
    protected void collect() {
        if (updated && unioned) {
            unionSketch.update(updateSketch);
        }
        merged = unioned ? unionSketch.getResult() : updateSketch.compact();
    }

    @Override
    protected Map<String, Object> getMetadata(Map<String, String> conceptKeys) {
        Map<String, Object> metadata = super.getMetadata(conceptKeys);

        addIfKeyNonNull(metadata, conceptKeys.get(Concept.MINIMUM_VALUE.getName()), this::getMinimum);
        addIfKeyNonNull(metadata, conceptKeys.get(Concept.MAXIMUM_VALUE.getName()), this::getMaximum);
        addIfKeyNonNull(metadata, conceptKeys.get(Concept.ITEMS_SEEN.getName()), this::getNumberOfEntries);
        addIfKeyNonNull(metadata, conceptKeys.get(Concept.NORMALIZED_RANK_ERROR.getName()), this::getNormalizedRankError);

        return metadata;
    }

    @Override
    protected Boolean isEstimationMode() {
        return merged.isEstimationMode();
    }

    @Override
    protected String getFamily() {
        return Family.QUANTILES.getFamilyName();
    }

    @Override
    protected Integer getSize() {
        return merged.getStorageBytes();
    }

    private Double getMinimum() {
        return merged.getMinValue();
    }

    private Double getMaximum() {
        return merged.getMaxValue();
    }

    private Long getNumberOfEntries() {
        return merged.getN();
    }

    private Double getNormalizedRankError() {
        return merged.getNormalizedRankError();
    }
}
