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

public class ThetaSketch extends KMVSketch {
    private final UpdateSketch updateSketch;
    private final Union unionSketch;
    @Getter
    private Sketch result;

    private String family;

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
        this.family = family.getFamilyName();
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
        return result.toByteArray();
    }

    @Override
    public void collect() {
        if (updated && unioned) {
            unionSketch.update(updateSketch.compact(false, null));
        }
        result = unioned ? unionSketch.getResult(false, null) : updateSketch.compact(false, null);
    }

    @Override
    public Boolean isEstimationMode() {
        return result.isEstimationMode();
    }

    @Override
    public String getFamily() {
        return family;
    }

    @Override
    public Integer getSize() {
        return result.getCurrentBytes(true);
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
}
