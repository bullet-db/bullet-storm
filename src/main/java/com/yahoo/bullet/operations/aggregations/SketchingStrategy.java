package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.aggregations.sketches.Sketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.sketches.Family;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

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

    /**
     * Get the key to add the sketch metadata for this {@link Strategy} that was configured.
     *
     * @return The key name to add the metadata for, or null if one was not configured.
     */
    public String getSketchMetaKey() {
        return metadataKeys.getOrDefault(Metadata.Concept.SKETCH_METADATA.getName(), null);
    }

    /**
     * Adds {@link Metadata} to the {@link Clip} if it is enabled.
     *
     * @param clip The clip to add the metadata to.
     * @return The original clip with or without metadata added.
     */
    protected Clip addMetadata(Clip clip) {
        String metaKey = getSketchMetaKey();
        return metaKey == null ? clip : clip.add(new Metadata().add(metaKey, getSketchMetadata(metadataKeys)));
    }

    /**
     * Gets the common metadata for this Sketching strategy.
     *
     * @param conceptKeys The {@link Map} of {@link Metadata.Concept} names to their keys.
     * @return The created {@link Map} of sketch metadata.
     */
    protected Map<String, Object> getSketchMetadata(Map<String, String> conceptKeys) {
        Map<String, Object> metadata = new HashMap<>();

        String familyKey = conceptKeys.get(Metadata.Concept.FAMILY.getName());
        String sizeKey = conceptKeys.get(Metadata.Concept.SIZE.getName());

        addIfKeyNonNull(metadata, familyKey, sketch::getFamily);
        addIfKeyNonNull(metadata, sizeKey, sketch::getSize);

        return metadata;
    }
}
