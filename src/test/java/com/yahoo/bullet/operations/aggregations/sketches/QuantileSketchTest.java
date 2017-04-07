package com.yahoo.bullet.operations.aggregations.sketches;

import com.yahoo.bullet.operations.AggregationOperations.DistributionType;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata.Concept;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.quantiles.DoublesSketch;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.yahoo.bullet.TestHelpers.assertApproxEquals;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.END_EXCLUSIVE;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.NEGATIVE_INFINITY_START;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.POSITIVE_INFINITY_END;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.SEPARATOR;
import static com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch.START_INCLUSIVE;

public class QuantileSketchTest {

    private static final Map<String, String> ALL_METADATA = new HashMap<>();
    static {
        ALL_METADATA.put(Concept.ESTIMATED_RESULT.getName(), "isEst");
        ALL_METADATA.put(Concept.FAMILY.getName(), "family");
        ALL_METADATA.put(Concept.SIZE.getName(), "size");
        ALL_METADATA.put(Concept.NORMALIZED_RANK_ERROR.getName(), "nre");
        ALL_METADATA.put(Concept.ITEMS_SEEN.getName(), "n");
        ALL_METADATA.put(Concept.MINIMUM_VALUE.getName(), "min");
        ALL_METADATA.put(Concept.MAXIMUM_VALUE.getName(), "max");
    }

    public static String makeRange(double start, double end) {
        return START_INCLUSIVE + start + SEPARATOR + end + END_EXCLUSIVE;
    }

    public static String startRange(double start) {
        return START_INCLUSIVE + start + SEPARATOR;
    }

    public static String endRange(double end) {
        return SEPARATOR + end + END_EXCLUSIVE;
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void testBadCreation() {
        new QuantileSketch(-2, null, null);
    }

    @Test
    public void testExactQuantiles() {
        QuantileSketch sketch = new QuantileSketch(64, DistributionType.QUANTILE, 11);

        // Insert 0, 10, 20 ... 100
        IntStream.range(0, 11).forEach(i -> sketch.update(i * 10.0));

        List<BulletRecord> records = sketch.getResult(null, null).getRecords();
        for (BulletRecord record : records) {
            Double quantile = (Double) record.get(QuantileSketch.QUANTILE_FIELD);
            Double value = (Double) record.get(QuantileSketch.VALUE_FIELD);
            // The value is the quantile as a percentage : Value 20.0 is the 0.2 or 20th percentile
            double percent = quantile * 100;

            assertApproxEquals(percent, value);
        }
    }

    @Test
    public void testExactPMF() {
        QuantileSketch sketch = new QuantileSketch(64, DistributionType.PMF, 10);

        // Insert 0, 1, 2 ... 9 three times
        IntStream.range(0, 30).forEach(i -> sketch.update(i % 10));
        // Insert 0, 1, 2 ten times
        IntStream.range(0, 30).forEach(i -> sketch.update(i % 3));

        Clip result = sketch.getResult("meta", ALL_METADATA);
        Map<String, Object> metadata = (Map<String, Object>) result.getMeta().asMap().get("meta");

        Assert.assertEquals(metadata.size(), 7);

        Assert.assertFalse((Boolean) metadata.get("isEst"));
        Assert.assertEquals((String) metadata.get("family"), Family.QUANTILES.getFamilyName());
        // Size should be at least 10 bytes since we inserted 60 items: 10 uniques
        Assert.assertTrue((Integer) metadata.get("size") >= 10);
        // No error
        Assert.assertEquals(metadata.get("nre"), DoublesSketch.getNormalizedRankError(64));
        // 60 items
        Assert.assertEquals(metadata.get("n"), 60L);

        double minimum = (Double) metadata.get("min");
        double maximum = (Double) metadata.get("max");
        Assert.assertEquals(minimum, 0.0);
        Assert.assertEquals(maximum, 9.0);

        List<BulletRecord> records = result.getRecords();
        for (BulletRecord record : records) {
            String range = (String) record.get(QuantileSketch.RANGE_FIELD);
            double value = (Double) record.get(QuantileSketch.VALUE_FIELD);

            if (range.startsWith(NEGATIVE_INFINITY_START)) {
                Assert.assertEquals(value, 0.0);
            } else if (range.endsWith(POSITIVE_INFINITY_END)) {
                // Values >= 9.0
                Assert.assertEquals(value, 3.0);
            } else if (range.startsWith(startRange(0.0)) || range.startsWith(startRange(1.0)) ||
                       range.startsWith(startRange(2.0))){
                // 0.0 - 1.0, 1.0 - 2.0, 2.0 - 3.0 have 3 from the first, 10 from the second
                Assert.assertEquals(value, 13.0);
            } else {
                // The rest are all 3
                Assert.assertEquals(value, 3.0);
            }
        }
    }

    @Test
    public void testResetting() {
    }
}