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
import com.yahoo.bullet.storm.testing.CustomTopologyContext;
import com.yahoo.bullet.storm.testing.TupleUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;

public class QuerySpoutTest {
    private CustomEmitter emitter;
    private QuerySpout spout;
    private CustomSubscriber subscriber;
    private CustomTopologyContext context;

    @BeforeMethod
    public void setup() {
        emitter = new CustomEmitter();
        BulletStormConfig config = new BulletStormConfig("src/test/resources/test_config.yaml");
        config.set(BulletStormConfig.TOPOLOGY_METRICS_BUILT_IN_ENABLE, true);
        config.validate();
        context = new CustomTopologyContext();
        spout = ComponentUtils.open(new HashMap<>(), new QuerySpout(config), context, emitter);
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
    public void testNextTupleCommitsWhenMetadataIsNull() {
        // Add message with null metadata
        subscriber.addMessages(new PubSubMessage("", "", null));

        Assert.assertEquals(subscriber.getReceived().size(), 0);
        Assert.assertEquals(subscriber.getCommitted().size(), 0);

        // subscriber.receive() -> message with null metadata
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 1);
        Assert.assertEquals(subscriber.getCommitted().size(), 1);
    }

    @Test
    public void testSignalOnlyMessagesAreSentOnTheMetadataStream() {
        // Add messages to be received from subscriber
        PubSubMessage messageA = new PubSubMessage("42", Metadata.Signal.KILL);
        PubSubMessage messageB = new PubSubMessage("43", Metadata.Signal.COMPLETE);
        PubSubMessage messageC = new PubSubMessage("44", (byte[]) null, new Metadata());
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

    @Test
    public void testForcedReplaySignal() {
        // Send forced replay signal
        PubSubMessage message = new PubSubMessage("123", (byte[]) null, new Metadata(Metadata.Signal.REPLAY, null));
        subscriber.addMessages(message);

        spout.nextTuple();

        Assert.assertEquals(emitter.getEmitted().size(), 1);
        Assert.assertEquals(emitter.getEmitted().get(0).getTuple().get(0), "123");
        Assert.assertEquals(subscriber.getCommitted().size(), 1);
        Assert.assertEquals(subscriber.getCommitted().get(0), "123");
    }

    @Test
    public void testHandleReplayRequest() {
        long startTimestamp = System.currentTimeMillis();

        // Replay request
        PubSubMessage message = new PubSubMessage("FilterBolt-18", (byte[]) null, new Metadata(Metadata.Signal.REPLAY, startTimestamp));
        subscriber.addMessages(message, message, message);

        Assert.assertEquals(subscriber.getReceived().size(), 0);
        Assert.assertEquals(subscriber.getCommitted().size(), 0);
        Assert.assertEquals(emitter.getEmitted().size(), 0);
        Assert.assertEquals(spout.getReplays().size(), 0);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC).longValue(), 0L);

        // Receives messageA. Should handle as a replay request
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 1);
        Assert.assertEquals(subscriber.getCommitted().size(), 1);
        Assert.assertEquals(emitter.getEmitted().size(), 1);
        Assert.assertEquals(spout.getReplays().size(), 1);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC).longValue(), 1L);

        QuerySpout.Replay replay = spout.getReplays().get("FilterBolt-18");

        Assert.assertEquals(replay.getId(), "FilterBolt-18");
        Assert.assertEquals(replay.getTimestamp(), startTimestamp);
        Assert.assertFalse(replay.isStopped());

        Assert.assertEquals(emitter.getEmitted().get(0).getMessageId(), "FilterBolt-18");
        Assert.assertEquals(emitter.getEmitted().get(0).getTuple().size(), 3);
        Assert.assertEquals(emitter.getEmitted().get(0).getTuple().get(0), "FilterBolt-18");
        Assert.assertEquals(emitter.getEmitted().get(0).getTuple().get(1), startTimestamp);
        Assert.assertEquals(emitter.getEmitted().get(0).getTuple().get(2), false);

        // Receives messageB. Should handle as a replay request and ignore
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 2);
        Assert.assertEquals(subscriber.getCommitted().size(), 2);
        Assert.assertEquals(emitter.getEmitted().size(), 1);
        Assert.assertEquals(spout.getReplays().size(), 1);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC).longValue(), 1L);

        // Replay loop continues on ack
        spout.ack("FilterBolt-18");

        Assert.assertFalse(replay.isStopped());
        Assert.assertEquals(emitter.getEmitted().size(), 2);
        Assert.assertEquals(emitter.getEmitted().get(1).getMessageId(), "FilterBolt-18");
        Assert.assertEquals(emitter.getEmitted().get(1).getTuple().get(2), true);
        Assert.assertEquals(subscriber.getCommitted().size(), 2);

        // Replay loop stops on fail
        spout.fail("FilterBolt-18");

        Assert.assertTrue(replay.isStopped());
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC).longValue(), 0L);

        // Replay loop continues on periodic replay request
        spout.nextTuple();

        Assert.assertFalse(replay.isStopped());
        Assert.assertEquals(emitter.getEmitted().size(), 3);
        Assert.assertEquals(emitter.getEmitted().get(2).getMessageId(), "FilterBolt-18");
        Assert.assertEquals(emitter.getEmitted().get(2).getTuple().get(2), false);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC).longValue(), 1L);
    }

    @Test
    public void testHandleReplayRequestDifferentTimestamp() {
        long startTimestamp = System.currentTimeMillis();

        // Replay request
        PubSubMessage messageA = new PubSubMessage("FilterBolt-18", (byte[]) null, new Metadata(Metadata.Signal.REPLAY, startTimestamp));
        PubSubMessage messageB = new PubSubMessage("FilterBolt-18", (byte[]) null, new Metadata(Metadata.Signal.REPLAY, startTimestamp - 1));
        PubSubMessage messageC = new PubSubMessage("FilterBolt-18", (byte[]) null, new Metadata(Metadata.Signal.REPLAY, startTimestamp + 1));
        PubSubMessage messageD = new PubSubMessage("FilterBolt-18", (byte[]) null, new Metadata(Metadata.Signal.REPLAY, startTimestamp + 2));
        subscriber.addMessages(messageA, messageB, messageC, messageD);

        Assert.assertEquals(emitter.getEmitted().size(), 0);
        Assert.assertEquals(spout.getReplays().size(), 0);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC).longValue(), 0L);

        // Receives messageA. Start a replay request
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 1);
        Assert.assertEquals(subscriber.getCommitted().size(), 1);
        Assert.assertEquals(emitter.getEmitted().size(), 1);
        Assert.assertEquals(spout.getReplays().size(), 1);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC).longValue(), 1L);

        QuerySpout.Replay replay = spout.getReplays().get("FilterBolt-18");

        Assert.assertEquals(replay.getId(), "FilterBolt-18");
        Assert.assertEquals(replay.getTimestamp(), startTimestamp);
        Assert.assertFalse(replay.isStopped());

        // Receives messageB. Ignore since old timestamp
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 2);
        Assert.assertEquals(subscriber.getCommitted().size(), 2);
        Assert.assertEquals(emitter.getEmitted().size(), 1);
        Assert.assertEquals(spout.getReplays().size(), 1);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC).longValue(), 1L);

        // Receives messageC. Start new replay request since newer timestamp
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 3);
        Assert.assertEquals(subscriber.getCommitted().size(), 3);
        Assert.assertEquals(emitter.getEmitted().size(), 1);
        Assert.assertEquals(spout.getReplays().size(), 1);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC).longValue(), 1L);

        Assert.assertEquals(replay.getTimestamp(), startTimestamp + 1);
        Assert.assertFalse(replay.isStopped());

        // Replay loop stops on fail
        spout.fail("FilterBolt-18");

        Assert.assertTrue(replay.isStopped());
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC).longValue(), 0L);

        // Receives messageD. Start new replay request since even newer timestamp
        spout.nextTuple();

        Assert.assertEquals(subscriber.getReceived().size(), 4);
        Assert.assertEquals(subscriber.getCommitted().size(), 4);
        Assert.assertEquals(emitter.getEmitted().size(), 2);
        Assert.assertEquals(spout.getReplays().size(), 1);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC).longValue(), 1L);

        Assert.assertEquals(replay.getTimestamp(), startTimestamp + 2);
        Assert.assertFalse(replay.isStopped());
    }

    @Test
    public void testDeactivateClearsReplays() {
        PubSubMessage message = new PubSubMessage("FilterBolt-18", (byte[]) null, new Metadata(Metadata.Signal.REPLAY, System.currentTimeMillis()));
        subscriber.addMessages(message);
        spout.nextTuple();

        Assert.assertEquals(spout.getReplays().size(), 1);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC).longValue(), 1L);

        spout.deactivate();

        Assert.assertEquals(spout.getReplays().size(), 0);
        Assert.assertEquals(context.getLongMetric(TopologyConstants.ACTIVE_REPLAYS_METRIC).longValue(), 0L);
    }
}
