package com.yahoo.bullet.operations.aggregations.sketches;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;
import lombok.Getter;

import java.util.Objects;

public class ThetaSketch implements KMVSketch {
    private final UpdateSketch updateSketch;
    private final Union unionSketch;
    @Getter
    private Sketch mergedSketch;

    private boolean updated = false;
    private boolean unioned = false;

    /**
     * Constructor for creating a theta sketch.
     *
     * @param resizeFactor The {@link ResizeFactor} to use for the sketch.
     * @param family The {@link Family} to use.
     * @param samplingProbability The sampling probability to use.
     * @param nominalEntries The nominal entries for the sketch.
     */
    public ThetaSketch(ResizeFactor resizeFactor, Family family, float samplingProbability, int nominalEntries) {
        updateSketch = UpdateSketch.builder().setFamily(family).setNominalEntries(nominalEntries)
                                             .setP(samplingProbability).setResizeFactor(resizeFactor)
                                             .build();
        unionSketch = SetOperation.builder().setNominalEntries(nominalEntries).setP(samplingProbability)
                                            .setResizeFactor(resizeFactor).buildUnion();
    }

    /**
     * Update the sketch with a String field.
     *
     * @param field The field to present to the sketch.
     */
    public void update(String field) {
        updateSketch.update(field);
        updated = true;
    }

    @Override
    public void union(byte[] serialized) {
        Sketch deserialized = Sketches.wrapSketch(new NativeMemory(serialized));
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
            unionSketch.update(updateSketch.compact(false, null));
        }
        mergedSketch = unioned ? unionSketch.getResult(false, null) : updateSketch.compact(false, null);
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
