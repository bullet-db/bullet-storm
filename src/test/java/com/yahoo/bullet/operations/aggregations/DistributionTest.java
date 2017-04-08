package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.AggregationOperations.DistributionType;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.result.Metadata.Concept;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.yahoo.bullet.parsing.AggregationUtils.addParsedMetadata;
import static com.yahoo.bullet.parsing.AggregationUtils.makeAttributes;
import static java.util.Arrays.asList;

public class DistributionTest {
    private static final List<Map.Entry<Concept, String>> ALL_METADATA =
        asList(Pair.of(Concept.ESTIMATED_RESULT, "isEst"),
               Pair.of(Concept.FAMILY, "family"),
               Pair.of(Concept.SIZE, "size"),
               Pair.of(Concept.NORMALIZED_RANK_ERROR, "nre"),
               Pair.of(Concept.ITEMS_SEEN, "n"),
               Pair.of(Concept.MINIMUM_VALUE, "min"),
               Pair.of(Concept.MAXIMUM_VALUE, "max"));

    public static Distribution makeDistribution(Map<Object, Object> configuration, Map<String, Object> attributes,
                                                String field, int size, List<Map.Entry<Concept, String>> metadata) {
        Aggregation aggregation = new Aggregation();
        aggregation.setFields(Collections.singletonMap(field, field));
        aggregation.setSize(size);
        aggregation.setAttributes(attributes);
        aggregation.configure(addParsedMetadata(configuration, metadata));

        Distribution distribution = new Distribution(aggregation);
        distribution.initialize();
        return distribution;
    }

    public static Distribution makeDistribution(String field, DistributionType type, long numberOfPoints) {
        return makeDistribution(makeConfiguration(100, 512), makeAttributes(type, numberOfPoints), field, 20, ALL_METADATA);
    }

    public static Map<Object, Object> makeConfiguration(int maxPoints, int k) {
        Map<Object, Object> config = new HashMap<>();
        config.put(BulletConfig.DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES, k);
        config.put(BulletConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS, maxPoints);
        return config;
    }

    @Test
    public void testQuantiles() {
        Distribution distribution = makeDistribution("field", DistributionType.QUANTILE, 10);
        Assert.assertTrue(true);
    }
}
