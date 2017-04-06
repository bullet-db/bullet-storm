package com.yahoo.bullet.operations.aggregations.sketches;

import com.yahoo.bullet.operations.AggregationOperations.DistributionType;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata.Concept;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.DoublesUnion;
import com.yahoo.sketches.quantiles.DoublesUnionBuilder;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Wraps operations for working with a {@link DoublesSketch} - Quantile Sketch.
 */
public class QuantileSketch extends Sketch {
    private UpdateDoublesSketch updateSketch;
    private DoublesUnion unionSketch;
    private DoublesSketch merged;

    private double[] points;
    private Integer numberOfPoints;
    private final DistributionType type;

    public static final double QUANTILE_MIN = 0.0;
    public static final double QUANTILE_MAX = 1.0;

    public static final String QUANTILE_FIELD = "Quantile";
    public static final String VALUE_FIELD = "Value";
    public static final String RANGE_FIELD = "Range";
    public static final String RANGE_INITIAL_PREFIX = "< ";
    public static final String RANGE_FINAL_PREFIX = "> ";
    public static final String RANGE_SEPARATOR = " - ";

    private QuantileSketch(int k, DistributionType type) {
        updateSketch = new DoublesSketchBuilder().build(k);
        unionSketch = new DoublesUnionBuilder().setMaxK(k).build();
        this.type = type;
    }

    /**
     * Creates a quantile sketch with the given number of entries getting results with the given points.
     *
     * @param k A number representative of the size of the sketch.
     * @param type A {@link DistributionType} that determines what the points mean.
     * @param points An array of points to get the quantiles, PMF and/or CDF for.
     */
    public QuantileSketch(int k, DistributionType type, double[] points) {
        this(k, type);
        this.points = points;
    }

    /**
     * Creates a quantile sketch with the given number of entries generating results with the number of
     * points (evenly-spaced).
     *
     * @param k A number representative of the size of the sketch.
     * @param type A {@link DistributionType} that determines what the points mean.
     * @param numberOfPoints A positive number of evenly spaced points in the range for the type to get the data for.
     */
    public QuantileSketch(int k, DistributionType type, int numberOfPoints) {
        this(k, type);
        this.numberOfPoints = numberOfPoints;
    }

    /**
     * Updates the sketch with a double.
     *
     * @param data A double to insert into the sketch.
     */
    public void update(double data) {
        updateSketch.update(data);
        updated = true;
    }

    @Override
    public void union(byte[] serialized) {
        DoublesSketch sketch = DoublesSketch.heapify(new NativeMemory(serialized));
        unionSketch.update(sketch);
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
        double[] domain = getDomain();
        double[] range;
        if (type == DistributionType.QUANTILE) {
            range = merged.getQuantiles(domain);
        } else if (type == DistributionType.PMF) {
            range = merged.getPMF(domain);
        } else {
            range = merged.getCDF(domain);
        }
        data.add(zip(domain, range, getNumberOfEntries()));
        return data;
    }

    @Override
    protected void collect() {
        if (updated && unioned) {
            unionSketch.update(updateSketch);
        }
        merged = unioned ? unionSketch.getResult() : updateSketch.compact();
    }

    @Override
    public void reset() {
        unioned = false;
        updated = false;
        updateSketch.reset();
        unionSketch.reset();
    }

    @Override
    protected Map<String, Object> getMetadata(Map<String, String> conceptKeys) {
        Map<String, Object> metadata = super.getMetadata(conceptKeys);

        addIfKeyNonNull(metadata, conceptKeys.get(Concept.MINIMUM_VALUE.getName()), this::getMinimum);
        addIfKeyNonNull(metadata, conceptKeys.get(Concept.MAXIMUM_VALUE.getName()), this::getMaximum);
        addIfKeyNonNull(metadata, conceptKeys.get(Concept.ITEMS_SEEN.getName()), this::getNumberOfEntries);
        addIfKeyNonNull(metadata, conceptKeys.get(Concept.NORMALIZED_RANK_ERROR.getName()), this::getNormalizedRankError);

        return metadata;
    }

    @Override
    protected Boolean isEstimationMode() {
        return merged.isEstimationMode();
    }

    @Override
    protected String getFamily() {
        return Family.QUANTILES.getFamilyName();
    }

    @Override
    protected Integer getSize() {
        return merged.getStorageBytes();
    }

    private Double getMinimum() {
        return merged.getMinValue();
    }

    private Double getMaximum() {
        return merged.getMaxValue();
    }

    private Long getNumberOfEntries() {
        return merged.getN();
    }

    private Double getNormalizedRankError() {
        return merged.getNormalizedRankError();
    }

    /**
     * For Testing.
     *
     * Generate numberOfPoints evenly spaced between start and end - both inclusive if numberOfPoints > 1. Otherwise,
     * returns points. Monotonically increasing.
     *
     * @return An array of evenly spaced points.
     */
    double[] getDomain() {
        if (numberOfPoints != null) {
            return type == DistributionType.QUANTILE ? getPoints(QUANTILE_MIN, QUANTILE_MAX, numberOfPoints) :
                                                       getPoints(getMinimum(), getMaximum(), numberOfPoints);
        }
        return points;
    }

    /**
     * For Testing.
     *
     * Creates a {@link List} of {@link BulletRecord} for each corresponding entry in domain and range. The domain
     * is first converted into range names depending on the type.
     *
     * @param domain An array of split points of size N > 0.
     * @param range  An array of values for each range in domain: of size N + 1
     *               if type is not {@link DistributionType#QUANTILE} else N.
     * @param n A long to scale the value of each range entry by if type is not {@link DistributionType#QUANTILE}.
     * @return The records that correspond to the data.
     */
    List<BulletRecord> zip(double[] domain, double[] range, long n) {
        return type == DistributionType.QUANTILE ? zipQuantiles(domain, range) : zipRanges(domain, range, n);
    }

    // Static helpers

    private static double[] getPoints(double start, double end, int numberOfPoints) {
        double[] points = new double[numberOfPoints];

        if  (numberOfPoints == 1) {
            points[0] = start;
            return points;
        }
        // Subtract one to generate [start, start + increment, ..., start + (N-1)*increment]
        int count = numberOfPoints - 1;
        double increment = (end - start) / count;
        double begin = start;
        for (int i = 0; i < count; ++i) {
            points[i] = begin;
            begin += increment;
        }
        // Add start + N*increment = end after
        points[count] = end;
        return points;
    }

    private static List<BulletRecord> zipQuantiles(double[] domain, double[] range) {
        List<BulletRecord> records = new ArrayList<>();

        for (int i = 0; i < domain.length; ++i) {
            records.add(new BulletRecord().setDouble(QUANTILE_FIELD, domain[i])
                                          .setDouble(VALUE_FIELD, range[i]));
        }
        return records;
    }

    private static List<BulletRecord> zipRanges(double[] domain, double[] range, long scale) {
        List<BulletRecord> records = new ArrayList<>();
        String[] bins = makeBins(domain);
        for (int i = 0; i < bins.length; ++i) {
            records.add(new BulletRecord().setString(RANGE_FIELD, bins[i])
                                          .setDouble(VALUE_FIELD, range[i] * scale));
        }
        return records;
    }

    private static String[] makeBins(double[] splits) {
        // The bins are created from -infinity to splits[0], split[1] - split[2], ..., split[N] to infinity
        String[] bins = new String[splits.length + 1];
        if (splits.length == 1) {
            double item = splits[0];
            bins[0] = RANGE_INITIAL_PREFIX + item;
            bins[1] = RANGE_FINAL_PREFIX + item;
            return bins;
        }
        String prefix = RANGE_INITIAL_PREFIX;
        int lastIndex = splits.length - 1;
        for (int i = 0; i <= lastIndex; ++i) {
            double binEnd = splits[i];
            bins[i] = prefix + binEnd;
            prefix = binEnd + RANGE_SEPARATOR;
        }
        bins[lastIndex + 1] = RANGE_FINAL_PREFIX + splits[lastIndex];
        return bins;
    }
}
