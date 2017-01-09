package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Specification;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.result.Metadata.Concept;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class CountDistinct implements Strategy {
    private UpdateSketch updateSketch;
    private Union unionSketch;
    private Set<String> fields;
    private String newName;

    private boolean consumed = false;
    private boolean combined = false;

    public static final String NEW_NAME_KEY = "newName";
    public static final String DEFAULT_NEW_NAME = "COUNT DISTINCT";

    // Separator for multiple fields when inserting into the Sketch
    private String separator;

    // Sketch defaults
    // No sampling
    public static final float DEFAULT_SAMPLING_PROBABILITY = 1.0f;

    // Recommended for real-time systems
    public static final String DEFAULT_UPDATE_SKETCH_FAMILY = Family.ALPHA.getFamilyName();

    // Sketch * 8 its size upto 2 * nominal entries everytime it reaches cap
    public static final int DEFAULT_RESIZE_FACTOR = ResizeFactor.X8.lg();

    // This gives us (Alpha sketches fall back to QuickSelect RSEs after compaction or set operations) a 2.34% error
    // rate at 99.73% confidence (3 Standard Deviations).
    public static final int DEFAULT_NOMINAL_ENTRIES = 16384;

    // Sketch metadata keys
    // TODO: Move common Sketch metadata collection into a single place for reuse.
    public static final Set<Concept> CONCEPTS = new HashSet<>(asList(Concept.AGGREGATION_METADATA,
                                                                     Concept.ESTIMATED_RESULT,
                                                                     Concept.STANDARD_DEVIATIONS,
                                                                     Concept.SKETCH_THETA,
                                                                     Concept.SKETCH_FAMILY,
                                                                     Concept.SKETCH_SIZE));
    private Map<String, String> metadataKeys;

    public static final String META_STD_DEV_1 = "1";
    public static final String META_STD_DEV_2 = "2";
    public static final String META_STD_DEV_3 = "3";
    public static final String META_STD_DEV_UB = "upperBound";
    public static final String META_STD_DEV_LB = "lowerBound";

    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public CountDistinct(Aggregation aggregation) {
        Map config = aggregation.getConfiguration();
        Map<String, Object> attributes = aggregation.getAttributes();

        fields = aggregation.getFields().keySet();
        newName = attributes == null ? DEFAULT_NEW_NAME :
                                       attributes.getOrDefault(NEW_NAME_KEY, DEFAULT_NEW_NAME).toString();
        metadataKeys = (Map<String, String>) config.getOrDefault(BulletConfig.RESULT_METADATA_METRICS_MAPPING,
                                                                 Collections.emptyMap());

        separator = config.getOrDefault(BulletConfig.AGGREGATION_COMPOSITE_FIELD_SEPARATOR,
                                        Aggregation.DEFAULT_FIELD_SEPARATOR).toString();

        float samplingProbability = ((Number) config.getOrDefault(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_SAMPLING,
                                                                  DEFAULT_SAMPLING_PROBABILITY)).floatValue();

        Family family = getFamily(config.getOrDefault(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_FAMILY,
                                                      DEFAULT_UPDATE_SKETCH_FAMILY).toString());

        ResizeFactor resizeFactor = getResizeFactor((Number) config.getOrDefault(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_RESIZE_FACTOR,
                                                                                 DEFAULT_RESIZE_FACTOR));

        int nominalEntries = ((Number) config.getOrDefault(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_ENTRIES,
                                                           DEFAULT_NOMINAL_ENTRIES)).intValue();

        updateSketch = UpdateSketch.builder().setFamily(family).setNominalEntries(nominalEntries)
                                             .setP(samplingProbability).setResizeFactor(resizeFactor)
                                             .build();

        unionSketch = SetOperation.builder().setNominalEntries(nominalEntries).setP(samplingProbability)
                                            .setResizeFactor(resizeFactor).buildUnion();
    }

    @Override
    public void consume(BulletRecord data) {
        String field = getFieldsAsString(fields, data, separator);
        updateSketch.update(field);
        consumed = true;
    }

    @Override
    public byte[] getSerializedAggregation() {
        CompactSketch compactSketch = merge();
        return compactSketch.toByteArray();
    }

    @Override
    public void combine(byte[] serializedAggregation) {
        Sketch deserialized = Sketches.wrapSketch(new NativeMemory(serializedAggregation));
        unionSketch.update(deserialized);
        combined = true;
    }

    @Override
    public Clip getAggregation() {
        Sketch result = merge();
        double count = result.getEstimate();

        BulletRecord record = new BulletRecord();
        record.setDouble(newName, count);

        String aggregationMetaKey = metadataKeys.getOrDefault(Concept.AGGREGATION_METADATA.getName(), null);
        if (aggregationMetaKey == null) {
            return Clip.of(record);
        }

        Map<String, Object> sketchMetadata = getSketchMetadata(result, metadataKeys);
        Metadata meta = new Metadata().add(aggregationMetaKey, sketchMetadata);
        return Clip.of(meta).add(record);

    }

    private CompactSketch merge() {
        // Merge the updateSketch into the unionSketch. Supporting it for completeness.
        if (consumed && combined) {
            unionSketch.update(updateSketch.compact(false, null));
        }

        if (combined) {
            return unionSketch.getResult(false, null);
        } else {
            return updateSketch.compact(false, null);
        }
    }

    private static String getFieldsAsString(Set<String> fields, BulletRecord record, String separator) {
        // This explicitly does not do a TypedObject checking. Nulls turn into a String that will be counted.
        return fields.stream().map(field -> Specification.extractField(field, record))
                              .map(Objects::toString)
                              .collect(Collectors.joining(separator));
    }

    private Map<String, Object> getSketchMetadata(Sketch sketch, Map<String, String> conceptKeys) {
        Map<String, Object> metadata = new HashMap<>();

        String standardDeviationsKey = conceptKeys.get(Concept.STANDARD_DEVIATIONS.getName());
        String isEstimatedKey = conceptKeys.get(Concept.ESTIMATED_RESULT.getName());
        String thetaKey = conceptKeys.get(Concept.SKETCH_THETA.getName());
        String familyKey = conceptKeys.get(Concept.SKETCH_FAMILY.getName());
        String sizeKey = conceptKeys.get(Concept.SKETCH_SIZE.getName());

        addIfKeyNonNull(metadata, standardDeviationsKey, () -> getStandardDeviations(sketch));
        addIfKeyNonNull(metadata, isEstimatedKey, sketch::isEstimationMode);
        addIfKeyNonNull(metadata, thetaKey, sketch::getTheta);
        addIfKeyNonNull(metadata, familyKey, () -> sketch.getFamily().getFamilyName());
        addIfKeyNonNull(metadata, sizeKey, () -> sketch.getCurrentBytes(true));

        return metadata;
    }

    private Map<String, Map<String, Double>> getStandardDeviations(Sketch sketch) {
        Map<String, Map<String, Double>> standardDeviations = new HashMap<>();
        standardDeviations.put(META_STD_DEV_1, getStandardDeviation(sketch, 1));
        standardDeviations.put(META_STD_DEV_2, getStandardDeviation(sketch, 2));
        standardDeviations.put(META_STD_DEV_3, getStandardDeviation(sketch, 3));
        return standardDeviations;
    }

    private Map<String, Double> getStandardDeviation(Sketch sketch, int standardDeviation) {
        double lowerBound = sketch.getLowerBound(standardDeviation);
        double upperBound = sketch.getUpperBound(standardDeviation);
        Map<String, Double> bounds = new HashMap<>();
        bounds.put(META_STD_DEV_LB, lowerBound);
        bounds.put(META_STD_DEV_UB, upperBound);
        return bounds;
    }

    private void addIfKeyNonNull(Map<String, Object> metadata, String key, Supplier<Object> supplier) {
        if (key != null) {
            metadata.put(key, supplier.get());
        }
    }

    /**
     * Convert a String family into a {@link Family}. For testing.
     *
     * @param family The string version of the {@link Family}. Currently, QuickSelect and Alpha are supported.
     * @return The Sketch family represented by the string or {@link #DEFAULT_UPDATE_SKETCH_FAMILY} otherwise.
     */
    static Family getFamily(String family) {
        return Family.QUICKSELECT.getFamilyName().equals(family) ? Family.QUICKSELECT : Family.ALPHA;
    }

    /**
     * Converts a integer representing the resizing for Sketches into a {@link ResizeFactor}. For testing.
     *
     * @param factor An int representing the scaling when the Sketch reaches its threshold. Supports 1, 2, 4 and 8.
     * @return A {@link ResizeFactor} represented by the integer or {@link #DEFAULT_RESIZE_FACTOR} otherwise.
     */
    static ResizeFactor getResizeFactor(Number factor) {
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
