/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

public class QuerySpoutTest {
    private CustomEmitter emitter;
    private QuerySpout spout;
    private CustomSubscriber subscriber;

    @BeforeMethod
    public void setup() throws IOException {
        emitter = new CustomEmitter();
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        QuerySpout querySpout = new QuerySpout(config);
        spout = ComponentUtils.open(querySpout, emitter);
        subscriber = (CustomSubscriber) spout.getPubSub().getSubscriber();
    }

    @Test
    public void nextTupleMessagesAreReceivedAndTuplesAreEmittedTest() {
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

        Tuple emittedFirst = TupleUtils.makeTuple(TupleType.Type.QUERY_TUPLE, messageA.getId(), messageA.getContent());
        Tuple emittedSecond = TupleUtils.makeTuple(TupleType.Type.METADATA_TUPLE, messageA.getId(), messageA.getMetadata());
        Assert.assertEquals(emitter.getEmitted().size(), 2);
        Assert.assertTrue(emitter.wasNthEmitted(emittedFirst, 1));
        Assert.assertTrue(emitter.wasNthEmitted(emittedSecond, 2));

        // subscriber.receive() -> messageB
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 2);
        Assert.assertEquals(subscriber.getReceived().get(0), messageA);
        Assert.assertEquals(subscriber.getReceived().get(1), messageB);

        Tuple emittedThird = TupleUtils.makeTuple(TupleType.Type.QUERY_TUPLE, messageB.getId(), messageB.getContent());
        Tuple emittedFourth = TupleUtils.makeTuple(TupleType.Type.METADATA_TUPLE, messageB.getId(), messageB.getMetadata());
        Assert.assertEquals(emitter.getEmitted().size(), 4);
        Assert.assertTrue(emitter.wasNthEmitted(emittedFirst, 1));
        Assert.assertTrue(emitter.wasNthEmitted(emittedSecond, 2));
        Assert.assertTrue(emitter.wasNthEmitted(emittedThird, 3));
        Assert.assertTrue(emitter.wasNthEmitted(emittedFourth, 4));
    }

    @Test
    public void nextTupleDoesNothingWhenSubscriberReceivesNullTest() {
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
    public void nextTupleDoesNothingWhenSubscriberThrowsTest() {
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
    public void declareOutputFieldsTest() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        spout.declareOutputFields(declarer);
        Fields expectedQueryFields = new Fields(TopologyConstants.ID_FIELD, TopologyConstants.QUERY_FIELD);
        Fields expectedMetadataFields = new Fields(TopologyConstants.ID_FIELD, TopologyConstants.METADATA_FIELD);
        Assert.assertTrue(declarer.areFieldsPresent(QuerySpout.QUERY_STREAM, false, expectedQueryFields));
        Assert.assertTrue(declarer.areFieldsPresent(QuerySpout.METADATA_STREAM, false, expectedMetadataFields));
    }

    @Test
    public void ackCallsSubscriberCommitTest() {
        spout.ack("42");
        spout.ack("43");
        spout.ack("44");
        Assert.assertEquals(subscriber.getCommitted().size(), 3);
        Assert.assertEquals(subscriber.getCommitted().get(0), "42");
        Assert.assertEquals(subscriber.getCommitted().get(1), "43");
        Assert.assertEquals(subscriber.getCommitted().get(2), "44");
    }

    @Test
    public void failCallsSubscriberFailTest() {
        spout.fail("42");
        spout.fail("43");
        spout.fail("44");
        Assert.assertEquals(subscriber.getFailed().size(), 3);
        Assert.assertEquals(subscriber.getFailed().get(0), "42");
        Assert.assertEquals(subscriber.getFailed().get(1), "43");
        Assert.assertEquals(subscriber.getFailed().get(2), "44");
    }

    @Test
    public void closeCallsSubscriberCloseTest() {
        spout.close();
        Assert.assertTrue(subscriber.isClosed());
    }
}
