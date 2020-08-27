/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.common.Config;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.storm.drpc.utils.DRPCOutputCollector;
import com.yahoo.bullet.storm.testing.CustomTopologyContext;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.yahoo.bullet.storm.drpc.MockDRPCSpout.makeMessageID;
import static com.yahoo.bullet.storm.drpc.MockDRPCSpout.makeReturnInfo;
import static java.util.Arrays.asList;

public class DRPCQuerySubscriberTest {
    private DRPCQuerySubscriber subscriber;
    private MockDRPCSpout mockSpout;

    @BeforeMethod
    public void setup() {
        DRPCConfig config = new DRPCConfig("test_drpc_config.yaml");

        // 1 task for the component named "foo" with task index 0
        CustomTopologyContext context = new CustomTopologyContext(Collections.singletonList(1), "foo", 0);
        config.set(DRPCConfig.STORM_CONTEXT, context);

        Map stormConfig = new Config("test_storm_config.yaml").getAll(Optional.empty());
        config.set(DRPCConfig.STORM_CONFIG, stormConfig);

        DRPCOutputCollector collector = new DRPCOutputCollector();
        mockSpout = new MockDRPCSpout("foo", collector);
        subscriber = new DRPCQuerySubscriber(config, 5, collector, mockSpout);
    }

    @Test
    public void testReadingNothingFromSpout() throws Exception {
        Assert.assertNull(subscriber.receive());
        Assert.assertNull(subscriber.receive());
    }

    @Test
    public void testReadingFromSpout() throws Exception {
        // It adds to metadata a string JSON with id: "fake" + foo, host: "testHost" and port: a total count of messages.
        mockSpout.addMessageParts("foo", "{'duration': 2000}");
        mockSpout.addMessageParts("bar", "{}");
        mockSpout.addMessageParts("baz", "{}");

        PubSubMessage actual = subscriber.receive();
        Assert.assertEquals(actual.getId(), "foo");
        Assert.assertEquals(actual.getContentAsString(), "{'duration': 2000}");
        Assert.assertFalse(actual.hasSignal());
        Assert.assertTrue(actual.hasMetadata());
        Metadata metadata = actual.getMetadata();
        Assert.assertEquals(metadata.getContent(), makeReturnInfo("fakefoo", "testHost", 0));

        actual = subscriber.receive();
        Assert.assertEquals(actual.getId(), "bar");
        Assert.assertEquals(actual.getContentAsString(), "{}");
        Assert.assertFalse(actual.hasSignal());
        Assert.assertTrue(actual.hasMetadata());
        metadata = actual.getMetadata();
        Assert.assertEquals(metadata.getContent(), makeReturnInfo("fakebar", "testHost", 1));

        actual = subscriber.receive();
        Assert.assertEquals(actual.getId(), "baz");
        Assert.assertEquals(actual.getContentAsString(), "{}");
        Assert.assertFalse(actual.hasSignal());
        Assert.assertTrue(actual.hasMetadata());
        metadata = actual.getMetadata();
        Assert.assertEquals(metadata.getContent(), makeReturnInfo("fakebaz", "testHost", 2));
    }

    @Test
    public void testClosing() {
        Assert.assertFalse(mockSpout.isClosed());
        subscriber.close();
        Assert.assertTrue(mockSpout.isClosed());
    }

    @Test
    public void testClosingFailsPendingDRPCRequests() throws Exception {
        mockSpout.addMessageParts("foo", "{'duration': 2000}");
        mockSpout.addMessageParts("bar", "{}");
        mockSpout.addMessageParts("baz", "{}");

        subscriber.receive();
        subscriber.receive();
        subscriber.receive();
        // 3 uncommitted messages
        Assert.assertFalse(mockSpout.isClosed());
        subscriber.close();
        Assert.assertTrue(mockSpout.isClosed());
        Set<Object> actual = new HashSet<>(mockSpout.getFailed());
        Set<Object> expected = new HashSet<>(asList(makeMessageID("foo", 0), makeMessageID("bar", 1),
                                                    makeMessageID("baz", 2)));
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testCommittingRemovesPendingDRPCRequests() throws Exception {
        mockSpout.addMessageParts("foo", "{'duration': 2000}");
        mockSpout.addMessageParts("bar", "{}");
        mockSpout.addMessageParts("baz", "{}");

        subscriber.receive();
        subscriber.receive();
        subscriber.receive();

        // 3 uncommitted messages. Commit the first
        subscriber.commit("foo");

        Assert.assertFalse(mockSpout.isClosed());
        subscriber.close();
        Assert.assertTrue(mockSpout.isClosed());
        Set<Object> actual = new HashSet<>(mockSpout.getFailed());
        Set<Object> expected = new HashSet<>(asList(makeMessageID("bar", 1), makeMessageID("baz", 2)));
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testFailingDoesNotRemovePendingDRPCRequests() throws Exception {
        mockSpout.addMessageParts("foo", "{'duration': 2000}");
        mockSpout.addMessageParts("bar", "{}");
        mockSpout.addMessageParts("baz", "{}");

        subscriber.receive();
        subscriber.receive();
        subscriber.receive();

        Assert.assertTrue(mockSpout.getFailed().isEmpty());
        // 3 uncommitted messages. Fail the second one
        subscriber.fail("bar");
        Assert.assertTrue(mockSpout.getFailed().isEmpty());

        // We should fail all messages included the failed one
        Assert.assertFalse(mockSpout.isClosed());
        subscriber.close();
        Assert.assertTrue(mockSpout.isClosed());
        Set<Object> actual = new HashSet<>(mockSpout.getFailed());
        Set<Object> expected = new HashSet<>(asList(makeMessageID("foo", 0), makeMessageID("bar", 1),
                                                    makeMessageID("baz", 2)));
        Assert.assertEquals(actual, expected);
    }
}
