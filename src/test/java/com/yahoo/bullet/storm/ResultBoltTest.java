package com.yahoo.bullet.storm;

import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResultBoltTest {
    private CustomCollector collector;
    private ResultBolt bolt;
    private CustomPublisher publisher;

    @BeforeMethod
    public void setup() {
        publisher = new CustomPublisher();

        PubSub mockPubSub = mock(PubSub.class);
        when(mockPubSub.getPublisher()).thenReturn(publisher);

        collector = new CustomCollector();
        bolt = ComponentUtils.prepare(new ResultBolt(mockPubSub), collector);
    }

    @Test
    public void executeMessagesAreSentTest() throws PubSubException {
        List<PubSubMessage> expected = Arrays.asList(new PubSubMessage("42", "This is a PubSubMessage"),
                                                     new PubSubMessage("43", "This is also a PubSubMessage"),
                                                     new PubSubMessage("44", "This is still a PubSubMessage"));
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
        // null metadata will cause CustomPublisher to throw
        bolt.execute(TupleUtils.makeTuple("42", "This is a PubSubMessage"));
        bolt.execute(TupleUtils.makeTuple("43", "This is also a PubSubMessage"));
        bolt.execute(TupleUtils.makeTuple("44", "This is still a PubSubMessage"));

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
