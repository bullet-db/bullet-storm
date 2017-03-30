package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.operations.aggregations.sketches.Sketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;

import java.util.Map;

public class Distribution<S extends Sketch> extends SketchingStrategy<S> {
    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public Distribution(Aggregation aggregation) {
        super(aggregation);
    }

    @Override
    public void consume(BulletRecord data) {
    }

    @Override
    public void combine(byte[] serializedAggregation) {

    }

    @Override
    public byte[] getSerializedAggregation() {
        return new byte[0];
    }

    @Override
    public Clip getAggregation() {
        return null;
    }

    @Override
    protected Map<String, Object> getSketchMetadata(Map<String, String> conceptKeys) {
        return null;
    }

}
