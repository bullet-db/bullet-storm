package com.yahoo.bullet.storm;

import backtype.storm.Config;
import backtype.storm.metric.LoggingMetricsConsumer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class CustomLoggingMetricsConsumerTest {
    @Test
    public void testRegister() {
        Config config = new Config();
        Assert.assertNull(config.get(Config.TOPOLOGY_METRICS_CONSUMER_REGISTER));

        CustomLoggingMetricsConsumer.register(config, null);

        Assert.assertNotNull(config.get(Config.TOPOLOGY_METRICS_CONSUMER_REGISTER));


        List<Object> register = (List<Object>) config.get(Config.TOPOLOGY_METRICS_CONSUMER_REGISTER);
        Assert.assertEquals(register.size(), 1);
        Map<Object, Object> actual = (Map<Object, Object>) register.get(0);
        Map<Object, Object> expected = new HashMap<>();
        expected.put("class", LoggingMetricsConsumer.class.getCanonicalName());
        expected.put("parallelism.hint", 1L);
        expected.put("argument", null);
        Assert.assertEquals(actual, expected);
    }
}
