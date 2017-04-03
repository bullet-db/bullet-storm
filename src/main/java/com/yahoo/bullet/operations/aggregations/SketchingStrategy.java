package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.aggregations.sketches.Sketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;

import java.util.Collections;
import java.util.Map;

/**
 * The parent class for all {@link Strategy} that use Sketches.
 *
 * @param <S> A {@link Sketch} type.
 */
public abstract class SketchingStrategy<S extends Sketch> implements Strategy {
    // The metadata concept to key mapping
    protected final Map<String, String> metadataKeys;
    // A  copy of the configuration
    protected final Map config;

    // The Sketch that should be initialized by a child class
    protected S sketch;

    /**
     * The constructor for creating a Sketch based strategy.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public SketchingStrategy(Aggregation aggregation) {
        config = aggregation.getConfiguration();
        metadataKeys = (Map<String, String>) config.getOrDefault(BulletConfig.RESULT_METADATA_METRICS_MAPPING,
                                                                 Collections.emptyMap());
    }

    @Override
    public void combine(byte[] serializedAggregation) {
        sketch.union(serializedAggregation);
    }

    @Override
    public byte[] getSerializedAggregation() {
        return sketch.serialize();
    }

    @Override
    public Clip getAggregation() {
        String metakey = metadataKeys.getOrDefault(Metadata.Concept.SKETCH_METADATA.getName(), null);
        return sketch.getResult(metakey, metadataKeys);
    }
}
