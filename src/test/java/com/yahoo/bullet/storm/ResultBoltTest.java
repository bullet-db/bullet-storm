/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ResultBoltTest {
    private CustomCollector collector;
    private ResultBolt bolt;
    private CustomPublisher publisher;

    @BeforeMethod
    public void setup() throws IOException {
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        ResultBolt resultBolt = new ResultBolt(config);
        collector = new CustomCollector();
        bolt = ComponentUtils.prepare(resultBolt, collector);
        publisher = (CustomPublisher) resultBolt.getPublisher();
    }

    @Test
    public void executeMessagesAreSentTest() throws PubSubException {
        List<PubSubMessage> expected = Arrays.asList(new PubSubMessage("42", "This is a PubSubMessage", new Metadata()),
                                                     new PubSubMessage("43", "This is also a PubSubMessage", new Metadata()),
                                                     new PubSubMessage("44", "This is still a PubSubMessage", new Metadata()));
        List<Tuple> tuples = new ArrayList<>();
        expected.forEach(pubSubMessage -> {
                tuples.add(TupleUtils.makeTuple(pubSubMessage.getId(), pubSubMessage.getContent(), pubSubMessage.getMetadata()));
            }
        );

        for (int i = 0; i < tuples.size(); i++) {
            bolt.execute(tuples.get(i));

            for (int j = 0; j <= i; j++) {
                Assert.assertEquals(publisher.getSent().get(j).getId(), expected.get(j).getId());
                Assert.assertEquals(publisher.getSent().get(j).getContent(), expected.get(j).getContent());
                Assert.assertEquals(publisher.getSent().get(j).getMetadata(), expected.get(j).getMetadata());
                Assert.assertTrue(collector.wasNthAcked(tuples.get(j), j + 1));
            }
            Assert.assertEquals(collector.getAckedCount(), i + 1);
        }
    }

    @Test
    public void executeStillAcksWhenPublisherThrowsTest() throws PubSubException {
        // Execute a few tuples
        // Closing the publisher will cause CustomPublisher to throw
        publisher.close();
        bolt.execute(TupleUtils.makeTuple("42", "This is a PubSubMessage", new Metadata()));
        bolt.execute(TupleUtils.makeTuple("43", "This is also a PubSubMessage", new Metadata()));
        bolt.execute(TupleUtils.makeTuple("44", "This is still a PubSubMessage", new Metadata()));

        // Assert that no tuples were sent, committed, or acked
        Assert.assertTrue(publisher.getSent().isEmpty());
        Assert.assertEquals(collector.getAckedCount(), 3);
    }

    @Test
    public void cleanupClosesPublisherTest() {
        Assert.assertFalse(publisher.isClosed());
        bolt.cleanup();
        Assert.assertTrue(publisher.isClosed());
    }

    @Test
    public void declareOutputFieldsTest() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        bolt.declareOutputFields(declarer);
        Assert.assertTrue(!declarer.areFieldsDeclared());
    }
}
