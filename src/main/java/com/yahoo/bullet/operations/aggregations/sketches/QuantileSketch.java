package com.yahoo.bullet.operations.aggregations.sketches;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.DoublesUnion;
import com.yahoo.sketches.quantiles.DoublesUnionBuilder;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import lombok.Getter;

/**
 * Wraps operations for working with a {@link DoublesSketch} - Quantile Sketch.
 */
public class QuantileSketch extends Sketch {
    private final UpdateDoublesSketch updateSketch;
    private final DoublesUnion unionSketch;

    @Getter
    private DoublesSketch result;

    /**
     * Creates a quantile sketch with the given number of entries.
     *
     * @param k A number representative of the size of the sketch.
     */
    public QuantileSketch(int k) {
        updateSketch = new DoublesSketchBuilder().build(k);
        unionSketch = new DoublesUnionBuilder().setMaxK(k).build();
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
        return result.toByteArray();
    }

    @Override
    public void collect() {
        if (updated && unioned) {
            unionSketch.update(updateSketch);
        }
        result = unioned ? unionSketch.getResult() : updateSketch.compact();
    }

    @Override
    public Boolean isEstimationMode() {
        return result.isEstimationMode();
    }

    @Override
    public String getFamily() {
        return Family.QUANTILES.getFamilyName();
    }

    @Override
    public Integer getSize() {
        return result.getStorageBytes();
    }

    /**
     * Gets the smallest entry seen in the stream. Only applicable after {@link #collect()}.
     *
     * @return A Double representing the minimum value seen.
     * @throws NullPointerException if collect had not been called.
     */
    public Double getMinimum() {
        return result.getMinValue();
    }

    /**
     * Gets the largest entry seen in the stream. Only applicable after {@link #collect()}.
     *
     * @return A Double representing the maximum value seen.
     * @throws NullPointerException if collect had not been called.
     */
    public Double getMaximum() {
        return result.getMaxValue();
    }

    /**
     * Gets the number of entries seen in the stream. Only applicable after {@link #collect()}.
     *
     * @return A Long representing the number of values seen.
     * @throws NullPointerException if collect had not been called.
     */
    public Long getNumberOfEntries() {
        return result.getN();
    }

    /**
     * Return the NRE of the sketch.
     *
     * @return A Double representing the Normalized Rank Error. Only applicable after {@link #collect()}.
     * @throws NullPointerException if collect had not been called.
     */
    public Double getNormalizedRankError() {
        return result.getNormalizedRankError();
    }
}
