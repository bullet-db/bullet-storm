package com.yahoo.bullet.operations.aggregations.sketches;

import com.yahoo.bullet.operations.aggregations.grouping.CachingGroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupDataSummary;
import com.yahoo.bullet.operations.aggregations.grouping.GroupDataSummaryFactory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.Union;
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Objects;

@AllArgsConstructor
public class TupleSketch implements KMVSketch {
    private final UpdatableSketch<CachingGroupData, GroupDataSummary> updateSketch;
    private final Union<GroupDataSummary> unionSketch;

    @Getter
    private Sketch<GroupDataSummary> mergedSketch;

    private boolean updated = false;
    private boolean unioned = false;

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
        return mergedSketch.toByteArray();
    }

    @Override
    public void collect() {
        if (updated && unioned) {
            unionSketch.update(updateSketch.compact());
        }
        mergedSketch = unioned ? unionSketch.getResult() : updateSketch.compact();
    }

    @Override
    public boolean isEstimationMode() {
        Objects.requireNonNull(mergedSketch);
        return mergedSketch.isEstimationMode();
    }

    @Override
    public double getTheta() {
        Objects.requireNonNull(mergedSketch);
        return mergedSketch.getTheta();
    }

    @Override
    public double getLowerBound(int standardDeviation) {
        Objects.requireNonNull(mergedSketch);
        return mergedSketch.getLowerBound(standardDeviation);
    }

    @Override
    public double getUpperBound(int standardDeviation) {
        Objects.requireNonNull(mergedSketch);
        return mergedSketch.getUpperBound(standardDeviation);
    }
}
