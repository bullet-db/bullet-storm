package com.yahoo.bullet.storm;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.yahoo.bullet.storm.PrepareRequestBolt.ARGS_FIELD;
import static com.yahoo.bullet.storm.PrepareRequestBolt.ARGS_STREAM;
import static com.yahoo.bullet.storm.PrepareRequestBolt.ID_STREAM;
import static com.yahoo.bullet.storm.PrepareRequestBolt.REQUEST_FIELD;
import static com.yahoo.bullet.storm.PrepareRequestBolt.RETURN_FIELD;
import static com.yahoo.bullet.storm.PrepareRequestBolt.RETURN_STREAM;

public class PrepareRequestBoltTest {
    private PrepareRequestBolt bolt;
    private CustomCollector collector;

    @BeforeMethod
    public void setup() {
        collector = new CustomCollector();
        bolt = ComponentUtils.prepare(new PrepareRequestBolt(), collector);
    }

    @Test
    public void testOutputFields() {
        CustomOutputFieldsDeclarer declarer = new CustomOutputFieldsDeclarer();
        bolt.declareOutputFields(declarer);
        Fields expectedArgsFields = new Fields(REQUEST_FIELD, ARGS_FIELD);
        Fields expectedReturnFields = new Fields(REQUEST_FIELD, RETURN_FIELD);
        Fields expectedIDFields = new Fields(REQUEST_FIELD);
        Assert.assertTrue(declarer.areFieldsPresent(ARGS_STREAM, false, expectedArgsFields));
        Assert.assertTrue(declarer.areFieldsPresent(RETURN_STREAM, false, expectedReturnFields));
        Assert.assertTrue(declarer.areFieldsPresent(ID_STREAM, false, expectedIDFields));
    }

    @Test
    public void testExecution() {
        String requestBody = "fake drpc request";
        String returnInfo = "fake return info";

        Tuple sample = TupleUtils.makeTuple(requestBody, returnInfo);
        bolt.execute(sample);

        Assert.assertEquals(collector.getEmittedCount(), 3);
        Assert.assertTrue(collector.wasAcked(sample));

        Object generatedID = collector.getMthElementFromNthTupleEmittedTo(ARGS_STREAM, 1, 0).get();
        Long longID = (Long) generatedID;

        Assert.assertEquals(collector.getMthElementFromNthTupleEmittedTo(ID_STREAM, 1, 0).get(), longID);
        Assert.assertEquals(collector.getMthElementFromNthTupleEmittedTo(RETURN_STREAM, 1, 0).get(), longID);

        Assert.assertEquals(collector.getMthElementFromNthTupleEmittedTo(ARGS_STREAM, 1, 1).get(), requestBody);
        Assert.assertEquals(collector.getMthElementFromNthTupleEmittedTo(RETURN_STREAM, 1, 1).get(), returnInfo);
    }
}
