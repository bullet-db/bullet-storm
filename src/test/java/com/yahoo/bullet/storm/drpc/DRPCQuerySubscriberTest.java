package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.Config;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.storm.CustomTopologyContext;
import com.yahoo.bullet.storm.drpc.utils.DRPCOutputCollector;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.yahoo.bullet.storm.drpc.MockDRPCSpout.makeMessageID;
import static com.yahoo.bullet.storm.drpc.MockDRPCSpout.makeReturnInfo;
import static java.util.Arrays.asList;

public class DRPCQuerySubscriberTest {
    private DRPCOutputCollector collector;
    private DRPCQuerySubscriber subscriber;
    private MockDRPCSpout injectedMockSpout;

    @BeforeMethod
    public void setup() {
        DRPCConfig config = new DRPCConfig("src/test/resources/test_drpc_config.yaml");

        // 1 task for the component named "foo" with task index 0
        CustomTopologyContext context = new CustomTopologyContext(Collections.singletonList(1), "foo", 0);
        config.set(DRPCConfig.STORM_CONTEXT, context);

        Map stormConfig = new Config("src/test/resources/test_storm_config.yaml").getAll(Optional.empty());
        config.set(DRPCConfig.STORM_CONFIG, stormConfig);

        subscriber = new DRPCQuerySubscriber(config, 5);
        collector = subscriber.getCollector();

        // Override the DRPCSpout with our own that emits using our collector.
        injectedMockSpout = new MockDRPCSpout("foo", collector);
        subscriber.setSpout(injectedMockSpout);
    }

    @Test
    public void testReadingNothingFromSpout() throws Exception {
        Assert.assertNull(subscriber.receive());
        Assert.assertNull(subscriber.receive());
    }

    @Test
    public void testReadingFromSpout() throws Exception {
        // The MockDRPCSpout makes messages adds 2 zero based sequences with id foo and given string content.
        // It adds to metadata a string JSON with id: "fake" + foo, host: "testHost" and port: a total count of messages.
        injectedMockSpout.addMessageParts("foo", "{}", "{'duration': 2000}");
        injectedMockSpout.addMessageParts("bar", "{}");

        PubSubMessage actual = subscriber.receive();
        Assert.assertEquals(actual.getId(), "foo");
        Assert.assertEquals(actual.getSequence(), 0);
        Assert.assertEquals(actual.getContent(), "{}");
        Assert.assertFalse(actual.hasSignal());
        Assert.assertTrue(actual.hasMetadata());
        Metadata metadata = actual.getMetadata();
        Assert.assertEquals(metadata.getContent(), makeReturnInfo("fakefoo", "testHost", "0"));

        actual = subscriber.receive();
        Assert.assertEquals(actual.getId(), "foo");
        Assert.assertEquals(actual.getSequence(), 1);
        Assert.assertEquals(actual.getContent(), "{'duration': 2000}");
        Assert.assertFalse(actual.hasSignal());
        Assert.assertTrue(actual.hasMetadata());
        metadata = actual.getMetadata();
        Assert.assertEquals(metadata.getContent(), makeReturnInfo("fakefoo", "testHost", "1"));

        actual = subscriber.receive();
        Assert.assertEquals(actual.getId(), "bar");
        Assert.assertEquals(actual.getSequence(), 0);
        Assert.assertEquals(actual.getContent(), "{}");
        Assert.assertFalse(actual.hasSignal());
        Assert.assertTrue(actual.hasMetadata());
        metadata = actual.getMetadata();
        Assert.assertEquals(metadata.getContent(), makeReturnInfo("fakebar", "testHost", "2"));
    }

    @Test
    public void testClosing() {
        Assert.assertFalse(injectedMockSpout.isClosed());
        subscriber.close();
        Assert.assertTrue(injectedMockSpout.isClosed());
    }

    @Test
    public void testClosingFailsPendingMessages() throws Exception {
        injectedMockSpout.addMessageParts("foo", "{}", "{'duration': 2000}");
        injectedMockSpout.addMessageParts("bar", "{}");

        subscriber.receive();
        subscriber.receive();
        subscriber.receive();
        // 3 uncommitted messages
        Assert.assertFalse(injectedMockSpout.isClosed());
        subscriber.close();
        Assert.assertTrue(injectedMockSpout.isClosed());
        Set<Object> actual = new HashSet<>(injectedMockSpout.getFailed());
        Set<Object> expected = new HashSet<>(asList(makeMessageID("foo", 0), makeMessageID("foo", 1),
                                                    makeMessageID("bar", 2)));
        Assert.assertEquals(actual, expected);
    }
}