/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.storm.testing.ComponentUtils;
import com.yahoo.bullet.storm.testing.CustomEmitter;
import com.yahoo.bullet.storm.testing.CustomOutputFieldsDeclarer;
import com.yahoo.bullet.storm.testing.CustomSubscriber;
import com.yahoo.bullet.storm.testing.TupleUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class QuerySpoutTest {
    private CustomEmitter emitter;
    private QuerySpout spout;
    private CustomSubscriber subscriber;

    @BeforeMethod
    public void setup() {
        emitter = new CustomEmitter();
        BulletStormConfig config = new BulletStormConfig("src/test/resources/test_config.yaml");
        spout = ComponentUtils.open(new QuerySpout(config), emitter);
        spout.activate();
        subscriber = (CustomSubscriber) spout.getSubscriber();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Cannot create PubSub.*")
    public void testFailingToCreatePubSub() {
        BulletStormConfig config = new BulletStormConfig("src/test/resources/test_config.yaml");
        config.set(BulletConfig.PUBSUB_CLASS_NAME, "fake.class");
        QuerySpout spout = new QuerySpout(config);
        ComponentUtils.open(spout, emitter);
        spout.activate();
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

        Tuple emittedFirst = TupleUtils.makeTuple(TupleClassifier.Type.QUERY_TUPLE, messageA.getId(), messageA.getContent(), messageA.getMetadata());
        Assert.assertEquals(emitter.getEmitted().size(), 1);
        Assert.assertTrue(emitter.wasNthEmitted(emittedFirst, 1));

        // subscriber.receive() -> messageB
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 2);
        Assert.assertEquals(subscriber.getReceived().get(0), messageA);
        Assert.assertEquals(subscriber.getReceived().get(1), messageB);

        Tuple emittedSecond = TupleUtils.makeTuple(TupleClassifier.Type.QUERY_TUPLE, messageB.getId(), messageB.getContent(), messageB.getMetadata());
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
    public void testSignalOnlyMessagesAreSentOnTheMetadataStream() {
        // Add messages to be received from subscriber
        PubSubMessage messageA = new PubSubMessage("42", Metadata.Signal.KILL);
        PubSubMessage messageB = new PubSubMessage("43", Metadata.Signal.COMPLETE);
        PubSubMessage messageC = new PubSubMessage("44", null, new Metadata());
        subscriber.addMessages(messageA, messageB, messageC);

        Assert.assertEquals(subscriber.getReceived().size(), 0);
        Assert.assertEquals(emitter.getEmitted().size(), 0);

        spout.nextTuple();
        spout.nextTuple();
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 3);
        Assert.assertEquals(subscriber.getReceived().get(0), messageA);
        Assert.assertEquals(subscriber.getReceived().get(1), messageB);
        Assert.assertEquals(subscriber.getReceived().get(2), messageC);

        Assert.assertEquals(emitter.getEmitted().size(), 3);

        Tuple emittedFirst = TupleUtils.makeTuple(TupleClassifier.Type.METADATA_TUPLE, messageA.getId(), messageA.getMetadata());
        Tuple emittedSecond = TupleUtils.makeTuple(TupleClassifier.Type.METADATA_TUPLE, messageB.getId(), messageB.getMetadata());
        Tuple emittedThird = TupleUtils.makeTuple(TupleClassifier.Type.METADATA_TUPLE, messageC.getId(), messageC.getMetadata());
        Assert.assertTrue(emitter.wasTupleEmittedTo(emittedFirst, TopologyConstants.METADATA_STREAM));
        Assert.assertTrue(emitter.wasTupleEmittedTo(emittedSecond, TopologyConstants.METADATA_STREAM));
        Assert.assertTrue(emitter.wasTupleEmittedTo(emittedThird, TopologyConstants.METADATA_STREAM));

        Assert.assertTrue(emitter.wasNthEmitted(emittedFirst, 1));
        Assert.assertTrue(emitter.wasNthEmitted(emittedSecond, 2));
        Assert.assertTrue(emitter.wasNthEmitted(emittedThird, 3));
    }

    @Test
    public void testDeclaredOutputFields() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        spout.declareOutputFields(declarer);
        Fields expectedQueryFields = new Fields(TopologyConstants.ID_FIELD, TopologyConstants.QUERY_FIELD, TopologyConstants.METADATA_FIELD);
        Fields expectedMetadataFields = new Fields(TopologyConstants.ID_FIELD, TopologyConstants.METADATA_FIELD);
        Assert.assertTrue(declarer.areFieldsPresent(TopologyConstants.QUERY_STREAM, false, expectedQueryFields));
        Assert.assertTrue(declarer.areFieldsPresent(TopologyConstants.METADATA_STREAM, false, expectedMetadataFields));
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
    public void testCloseDoesNotCallSubscriberClose() {
        spout.close();
        Assert.assertFalse(subscriber.isClosed());
        Assert.assertFalse(subscriber.isThrown());
    }

    @Test
    public void testDeactivateCallsSubscriberClose() {
        spout.deactivate();
        Assert.assertTrue(subscriber.isClosed());
        Assert.assertFalse(subscriber.isThrown());

        // spout deactivate catches subscriber throw
        spout.deactivate();
        Assert.assertTrue(subscriber.isClosed());
        Assert.assertTrue(subscriber.isThrown());
    }
}
