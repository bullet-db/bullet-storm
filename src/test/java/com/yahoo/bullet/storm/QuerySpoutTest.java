/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class QuerySpoutTest {
    private CustomEmitter emitter;
    private QuerySpout spout;
    private CustomSubscriber subscriber;

    @BeforeMethod
    public void setup() throws PubSubException {
        emitter = new CustomEmitter();
        BulletStormConfig config = new BulletStormConfig("src/test/resources/test_config.yaml");
        spout = ComponentUtils.open(new QuerySpout(config), emitter);
        subscriber = (CustomSubscriber) spout.getPubSub().getSubscriber();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Cannot create PubSub.*")
    public void testFailingToCreatePubSub() {
        BulletStormConfig config = new BulletStormConfig("src/test/resources/test_config.yaml");
        config.set(BulletConfig.PUBSUB_CLASS_NAME, "fake.class");
        ComponentUtils.open(new QuerySpout(config), emitter);
    }

    @Test
    public void testNextTupleMessagesAreReceivedAndTupleIsEmitted() {
        // Add messages to be received from subscriber
        PubSubMessage messageA = new PubSubMessage("42", "This is a PubSubMessage", new Metadata());
        PubSubMessage messageB = new PubSubMessage("43", "This is also a PubSubMessage", new Metadata());
        subscriber.addMessages(messageA, messageB);

        Assert.assertEquals(subscriber.getReceived().size(), 0);
        Assert.assertEquals(emitter.getEmitted().size(), 0);

        // subscriber.receive() -> messageA
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 1);
        Assert.assertEquals(subscriber.getReceived().get(0), messageA);

        Tuple emittedFirst = TupleUtils.makeTuple(TupleType.Type.QUERY_TUPLE, messageA.getId(), messageA.getContent(), messageA.getMetadata());
        Assert.assertEquals(emitter.getEmitted().size(), 1);
        Assert.assertTrue(emitter.wasNthEmitted(emittedFirst, 1));

        // subscriber.receive() -> messageB
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 2);
        Assert.assertEquals(subscriber.getReceived().get(0), messageA);
        Assert.assertEquals(subscriber.getReceived().get(1), messageB);

        Tuple emittedSecond = TupleUtils.makeTuple(TupleType.Type.QUERY_TUPLE, messageB.getId(), messageB.getContent(), messageB.getMetadata());
        Assert.assertEquals(emitter.getEmitted().size(), 2);
        Assert.assertTrue(emitter.wasNthEmitted(emittedFirst, 1));
        Assert.assertTrue(emitter.wasNthEmitted(emittedSecond, 2));
    }

    @Test
    public void testNextTupleDoesNothingWhenSubscriberReceivesNull() {
        // Add null messages to be received from subscriber
        subscriber.addMessages(null, null);

        Assert.assertEquals(subscriber.getReceived().size(), 0);
        Assert.assertEquals(emitter.getEmitted().size(), 0);

        // subscriber.receive() -> null
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 1);
        Assert.assertEquals(emitter.getEmitted().size(), 0);

        // subscriber.receive() -> null
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 2);
        Assert.assertEquals(emitter.getEmitted().size(), 0);
    }

    @Test
    public void testNextTupleDoesNothingWhenSubscriberThrows() {
        // No messages to be received from subscriber
        Assert.assertEquals(subscriber.getReceived().size(), 0);
        Assert.assertEquals(emitter.getEmitted().size(), 0);

        // subscriber.receive() -> exception
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 0);
        Assert.assertEquals(emitter.getEmitted().size(), 0);

        // subscriber.receive() -> exception
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 0);
        Assert.assertEquals(emitter.getEmitted().size(), 0);
    }

    @Test
    public void testDeclareOutputFields() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        spout.declareOutputFields(declarer);
        Fields expectedQueryFields = new Fields(TopologyConstants.ID_FIELD, TopologyConstants.QUERY_FIELD, TopologyConstants.METADATA_FIELD);
        Assert.assertTrue(declarer.areFieldsPresent(QuerySpout.QUERY_STREAM, false, expectedQueryFields));
    }

    @Test
    public void testAckCallsSubscriberCommit() {
        spout.ack("42");
        spout.ack("43");
        spout.ack("44");
        Assert.assertEquals(subscriber.getCommitted().size(), 3);
        Assert.assertEquals(subscriber.getCommitted().get(0), "42");
        Assert.assertEquals(subscriber.getCommitted().get(1), "43");
        Assert.assertEquals(subscriber.getCommitted().get(2), "44");
    }

    @Test
    public void testFailCallsSubscriberFail() {
        spout.fail("42");
        spout.fail("43");
        spout.fail("44");
        Assert.assertEquals(subscriber.getFailed().size(), 3);
        Assert.assertEquals(subscriber.getFailed().get(0), "42");
        Assert.assertEquals(subscriber.getFailed().get(1), "43");
        Assert.assertEquals(subscriber.getFailed().get(2), "44");
    }

    @Test
    public void testCloseCallsSubscriberClose() {
        spout.close();
        Assert.assertTrue(subscriber.isClosed());
    }
}
