package com.yahoo.bullet.operations.aggregations.sketches;

import com.yahoo.bullet.TestHelpers;
import com.yahoo.bullet.operations.aggregations.grouping.CachingGroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.AVG;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.COUNT;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.SUM;

public class TupleSketchTest {
    private static final Map<String, String> GROUPS = new HashMap<>();
    static {
        GROUPS.put("fieldA", "A");
        GROUPS.put("fieldB", "B");
    }

    private static final Set<GroupOperation> OPERATIONS =
            new HashSet<>(Arrays.asList(new GroupOperation(COUNT, null, "cnt"),
                                        new GroupOperation(SUM, "fieldB", "sumB"),
                                        new GroupOperation(AVG, "fieldA", "avgA")));

    private CachingGroupData data;

    private BulletRecord get(String fieldA, double fieldB) {
        return RecordBox.get().add("fieldA", fieldA).add("fieldB", fieldB).getRecord();
    }

    private String addToData(String fieldA, double fieldB, CachingGroupData data) {
        BulletRecord record = get(fieldA, fieldB);

        Map<String, String> values = new HashMap<>();
        values.put("fieldA", fieldA);
        values.put("fieldB", Objects.toString(fieldB));

        data.setCachedRecord(record);
        data.setGroupFields(values);
        return fieldA + ";" + Objects.toString(fieldB);
    }

    @BeforeMethod
    public void setup() {
        data = new CachingGroupData(null, GroupData.makeInitialMetrics(OPERATIONS));
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void testBadCreation() {
        new TupleSketch(ResizeFactor.X1, -1.0f, -2, 0, null);

    }

    @Test
    public void testDistincts() {
        data = new CachingGroupData(null, new HashMap<>());

        TupleSketch sketch = new TupleSketch(ResizeFactor.X4, 1.0f, 64, 64, GROUPS);

        sketch.update(addToData("foo", 0.0, data), data);
        sketch.update(addToData("bar", 0.2, data), data);
        sketch.update(addToData("foo", 0.0, data), data);

        List<BulletRecord> actuals = sketch.getResult(null, null).getRecords();

        Assert.assertEquals(actuals.size(), 2);

        // Groups become strings
        BulletRecord expectedA = RecordBox.get().add("A", "foo").add("B", "0.0").getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "bar").add("B", "0.2").getRecord();

        TestHelpers.assertContains(actuals, expectedA);
        TestHelpers.assertContains(actuals, expectedB);
    }

    @Test
    public void testMetrics() {
        TupleSketch sketch = new TupleSketch(ResizeFactor.X4, 1.0f, 64, 64, GROUPS);

        sketch.update(addToData("9", 4.0, data), data);
        sketch.update(addToData("3", 0.2, data), data);
        sketch.update(addToData("9", 4.0, data), data);

        List<BulletRecord> actuals = sketch.getResult(null, null).getRecords();

        Assert.assertEquals(actuals.size(), 2);

        // Groups become strings
        BulletRecord expectedA = RecordBox.get().add("A", "9").add("B", "4.0")
                                                .add("cnt", 2).add("sumB", 8.0).add("avgA", 9.0).getRecord();
        BulletRecord expectedB = RecordBox.get().add("A", "3").add("B", "0.2")
                                                .add("cnt", 1).add("sumB", 0.2).add("avgA", 3.0).getRecord();

        TestHelpers.assertContains(actuals, expectedA);
        TestHelpers.assertContains(actuals, expectedB);
    }
}