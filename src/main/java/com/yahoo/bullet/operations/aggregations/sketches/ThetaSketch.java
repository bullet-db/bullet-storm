package com.yahoo.bullet.operations.aggregations.sketches;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;

import java.util.Map;

public class ThetaSketch extends KMVSketch {
    private UpdateSketch updateSketch;
    private Union unionSketch;
    private Sketch merged;

    private String family;
    private String newName;

    /**
     * Constructor for creating a theta sketch.
     *
     * @param resizeFactor The {@link ResizeFactor} to use for the sketch.
     * @param family The {@link Family} to use.
     * @param samplingProbability The sampling probability to use.
     * @param nominalEntries The nominal entries for the sketch.
     * @param newName The String name to add the result as.
     */
    public ThetaSketch(ResizeFactor resizeFactor, Family family,
                       float samplingProbability, int nominalEntries, String newName) {
        updateSketch = UpdateSketch.builder().setFamily(family).setNominalEntries(nominalEntries)
                                             .setP(samplingProbability).setResizeFactor(resizeFactor)
                                             .build();
        unionSketch = SetOperation.builder().setNominalEntries(nominalEntries).setP(samplingProbability)
                                            .setResizeFactor(resizeFactor).buildUnion();
        this.family = family.getFamilyName();
        this.newName = newName;
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
        return merged.toByteArray();
    }

    @Override
    public Clip getResult(String metaKey, Map<String, String> conceptKeys) {
        Clip data = super.getResult(metaKey, conceptKeys);
        double count = merged.getEstimate();
        BulletRecord record = new BulletRecord();
        record.setDouble(newName, count);
        return data.add(record);
    }

    @Override
    protected void collect() {
        if (updated && unioned) {
            unionSketch.update(updateSketch.compact(false, null));
        }
        merged = unioned ? unionSketch.getResult(false, null) : updateSketch.compact(false, null);
    }

    @Override
    public void reset() {
        updated = false;
        unioned = false;
        unionSketch.reset();
        updateSketch.reset();
    }

    // Metadata

    @Override
    protected Boolean isEstimationMode() {
        return merged.isEstimationMode();
    }

    @Override
    protected String getFamily() {
        return family;
    }

    @Override
    protected Integer getSize() {
        return merged.getCurrentBytes(true);
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
}
