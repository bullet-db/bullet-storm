package com.yahoo.bullet.operations.aggregations.sketches;

import com.yahoo.bullet.operations.aggregations.grouping.CachingGroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupDataSummary;
import com.yahoo.bullet.operations.aggregations.grouping.GroupDataSummaryFactory;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata.Concept;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.Union;
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;

import java.util.Map;

public class TupleSketch extends KMVSketch {
    private final UpdatableSketch<CachingGroupData, GroupDataSummary> updateSketch;
    private final Union<GroupDataSummary> unionSketch;
    private Sketch<GroupDataSummary> merged;

    private int maxSize;
    private Map<String, String> fieldNames;
    /**
     * Initialize a tuple sketch for summarizing group data.
     *
     * @param resizeFactor The {@link ResizeFactor} to use for the sketch.
     * @param samplingProbability The sampling probability to use.
     * @param nominalEntries The nominal entries for the sketch.
     * @param maxSize The maximum size of groups to return.
     * @param fieldNames A non-null mapping of field names (to rename them).
     */
    @SuppressWarnings("unchecked")
    public TupleSketch(ResizeFactor resizeFactor, float samplingProbability, int nominalEntries,
                       int maxSize, Map<String, String> fieldNames) {
        GroupDataSummaryFactory factory = new GroupDataSummaryFactory();
        UpdatableSketchBuilder<CachingGroupData, GroupDataSummary> builder = new UpdatableSketchBuilder(factory);

        updateSketch = builder.setResizeFactor(resizeFactor).setNominalEntries(nominalEntries)
                              .setSamplingProbability(samplingProbability).build();
        unionSketch = new Union<>(nominalEntries, factory);

        this.maxSize = maxSize;
        this.fieldNames = fieldNames;
    }

    /**
     * Update the sketch with a key representing a group and the data for it.
     *
     * @param key The key to present the data to the sketch as.
     * @param data The data for the group.
     */
    public void update(String key, CachingGroupData data) {
        updateSketch.update(key, data);
        updated = true;
    }

    @Override
    public void union(byte[] serialized) {
        Sketch<GroupDataSummary> deserialized = Sketches.heapifySketch(new NativeMemory(serialized));
        unionSketch.update(deserialized);
        unioned = true;
    }

    @Override
    public byte[] serialize() {
        collect();
        return merged.toByteArray();
    }

    @Override
    public Clip getResult(String metaKey, Map<String, String> conceptKeys) {
        Clip result = super.getResult(metaKey, conceptKeys);

        SketchIterator<GroupDataSummary> iterator = merged.iterator();
        for (int count = 0; iterator.next() && count < maxSize; count++) {
            GroupData data = iterator.getSummary().getData();
            // Add the record with the remapped group names to new names.
            result.add(data.getAsBulletRecord(fieldNames));
        }
        return result;
    }

    @Override
    protected void collect() {
        if (updated && unioned) {
            unionSketch.update(updateSketch.compact());
        }
        merged = unioned ? unionSketch.getResult() : updateSketch.compact();
    }

    // Metadata

    @Override
    protected Map<String, Object> getMetadata(Map<String, String> conceptKeys) {
        Map<String, Object> metadata = super.getMetadata(conceptKeys);

        addIfKeyNonNull(metadata, conceptKeys.get(Concept.UNIQUES_ESTIMATE.getName()), this::getUniquesEstimate);

        return metadata;
    }

    @Override
    protected Boolean isEstimationMode() {
        return merged.isEstimationMode();
    }

    @Override
    protected String getFamily() {
        return Family.TUPLE.getFamilyName();
    }

    @Override
    protected Integer getSize() {
        // Size need not be calculated since Summaries are arbitrarily large
        return null;
    }

    @Override
    protected Double getTheta() {
        return merged.getTheta();
    }

    @Override
    protected Double getLowerBound(int standardDeviation) {
        return merged.getLowerBound(standardDeviation);
    }

    @Override
    protected Double getUpperBound(int standardDeviation) {
        return merged.getUpperBound(standardDeviation);
    }

    /**
     * Returns the estimate of the uniques in the Sketch. Only applicable after {@link #collect()}.
     *
     * @return A Double representing the number of unique values in the Sketch.
     */
    protected Double getUniquesEstimate() {
        return merged.getEstimate();

    }
}
