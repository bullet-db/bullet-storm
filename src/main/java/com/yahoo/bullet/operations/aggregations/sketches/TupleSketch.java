package com.yahoo.bullet.operations.aggregations.sketches;

import com.yahoo.bullet.operations.aggregations.grouping.CachingGroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupDataSummary;
import com.yahoo.bullet.operations.aggregations.grouping.GroupDataSummaryFactory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.Union;
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;
import lombok.Getter;

public class TupleSketch extends KMVSketch {
    private final UpdatableSketch<CachingGroupData, GroupDataSummary> updateSketch;
    private final Union<GroupDataSummary> unionSketch;

    @Getter
    private Sketch<GroupDataSummary> result;

    /**
     * Initialize a tuple sketch for summarizing group data.
     *
     * @param resizeFactor The {@link ResizeFactor} to use for the sketch.
     * @param samplingProbability The sampling probability to use.
     * @param nominalEntries The nominal entries for the sketch.
     */
    @SuppressWarnings("unchecked")
    public TupleSketch(ResizeFactor resizeFactor, float samplingProbability, int nominalEntries) {
        GroupDataSummaryFactory factory = new GroupDataSummaryFactory();
        UpdatableSketchBuilder<CachingGroupData, GroupDataSummary> builder = new UpdatableSketchBuilder(factory);

        updateSketch = builder.setResizeFactor(resizeFactor).setNominalEntries(nominalEntries)
                              .setSamplingProbability(samplingProbability).build();
        unionSketch = new Union<>(nominalEntries, factory);
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
        return result.toByteArray();
    }

    @Override
    public void collect() {
        if (updated && unioned) {
            unionSketch.update(updateSketch.compact());
        }
        result = unioned ? unionSketch.getResult() : updateSketch.compact();
    }

    @Override
    public Boolean isEstimationMode() {
        return result.isEstimationMode();
    }

    @Override
    public String getFamily() {
        return Family.TUPLE.getFamilyName();
    }

    @Override
    public Integer getSize() {
        // Size need not be calculated since Summaries are arbitrarily large
        return null;
    }

    @Override
    public Double getTheta() {
        return result.getTheta();
    }

    @Override
    public Double getLowerBound(int standardDeviation) {
        return result.getLowerBound(standardDeviation);
    }

    @Override
    public Double getUpperBound(int standardDeviation) {
        return result.getUpperBound(standardDeviation);
    }

    /**
     * Returns the estimate of the uniques in the Sketch. Only applicable after {@link #collect()}.
     *
     * @return A Double representing the number of unique values in the Sketch.
     */
    public Double getUniquesEstimate() {
        return result.getEstimate();

    }
}
