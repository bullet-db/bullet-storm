/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.storm.drpc.DRPCConfig;
import com.yahoo.bullet.storm.testing.ComponentUtils;
import com.yahoo.bullet.storm.testing.CustomCollector;
import com.yahoo.bullet.storm.testing.CustomOutputFieldsDeclarer;
import com.yahoo.bullet.storm.testing.CustomPublisher;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.yahoo.bullet.storm.testing.TupleUtils.makeTuple;
import static java.util.Arrays.asList;

public class LoopBoltTest {
    private BulletStormConfig config;
    private CustomCollector collector;
    private LoopBolt bolt;
    private CustomPublisher publisher;

    @BeforeMethod
    public void setup() {
        config = new BulletStormConfig("src/test/resources/test_config.yaml");
        bolt = new LoopBolt(config);
        collector = new CustomCollector();
        ComponentUtils.prepare(bolt, collector);
        publisher = (CustomPublisher) bolt.getPublisher();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Cannot create PubSub.*")
    public void testFailingToCreatePubSub() {
        config.set(BulletConfig.PUBSUB_CLASS_NAME, "fake.class");
        ComponentUtils.prepare(new LoopBolt(config), collector);
    }

    @Test
    public void testSwitchingIntoQueryPublishing() {
        Assert.assertEquals(publisher.getContext(), PubSub.Context.QUERY_SUBMISSION);
        // Config is modified in place
        Assert.assertEquals(config.get("fake.setting"), "foo");
    }

    @Test
    public void testMessagesAreLooped() {
        List<PubSubMessage> expected = asList(new PubSubMessage("42", Metadata.Signal.KILL),
                                              new PubSubMessage("43", Metadata.Signal.COMPLETE),
                                              new PubSubMessage("44", (Metadata.Signal) null));
        List<Tuple> tuples = new ArrayList<>();
        expected.forEach(m -> tuples.add(makeTuple(m.getId(), m.getMetadata())));

        for (int i = 0; i < tuples.size(); i++) {
            bolt.execute(tuples.get(i));
            Assert.assertEquals(publisher.getSent().get(i).getId(), expected.get(i).getId());
            Assert.assertEquals(publisher.getSent().get(i).getMetadata(), expected.get(i).getMetadata());
            Assert.assertTrue(collector.wasNthAcked(tuples.get(i), i + 1));
            Assert.assertEquals(collector.getAckedCount(), i + 1);
        }
    }

    @Test
    public void testAcksEvenOnException() {
        publisher.close();
        bolt.execute(makeTuple("42", new Metadata(Metadata.Signal.KILL, null)));
        bolt.execute(makeTuple("43", new Metadata(Metadata.Signal.FAIL, null)));
        Assert.assertTrue(publisher.getSent().isEmpty());
        Assert.assertEquals(collector.getAckedCount(), 2);
    }

    @Test
    public void testCleanupClosesPublisher() {
        Assert.assertFalse(publisher.isClosed());
        bolt.cleanup();
        Assert.assertTrue(publisher.isClosed());
    }

    @Test
    public void testDeclareOutputFields() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        bolt.declareOutputFields(declarer);
        Assert.assertTrue(!declarer.areFieldsDeclared());
    }
}
