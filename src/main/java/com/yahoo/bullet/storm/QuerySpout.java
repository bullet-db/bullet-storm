package com.yahoo.bullet.storm;

import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Subscriber;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

@Slf4j
public class QuerySpout extends BaseRichSpout {
    public static final String QUERY_STREAM = Utils.DEFAULT_STREAM_ID;
    public static final String METADATA_STREAM = "meta";
    public static final String ID_FIELD = "id";
    public static final String QUERY_FIELD = "query";
    public static final String METADATA_FIELD = "metadata";

    private PubSub pubSub;
    private Subscriber subscriber;
    private SpoutOutputCollector collector;

    /**
     * Creates a QuerySpout and passes in a {@link PubSub}.
     *
     * @param pubSub PubSub to get a {@link Subscriber} from
     */
    public QuerySpout(PubSub pubSub) {
        this.pubSub = pubSub;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.subscriber = pubSub.getSubscriber();
    }

    @Override
    public void nextTuple() {
        PubSubMessage message = null;
        try {
            message = subscriber.receive();
        } catch (PubSubException e) {
            log.error(e.getMessage());
        }
        if (message != null) {
            collector.emit(QUERY_STREAM, new Values(message.getId(), message.getContent()), message.getId());
            collector.emit(METADATA_STREAM, new Values(message.getId(), message.getMetadata()));
        } else {
            sleep(1);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(QUERY_STREAM, new Fields(ID_FIELD, QUERY_FIELD));
        declarer.declareStream(METADATA_STREAM, new Fields(ID_FIELD, METADATA_FIELD));
    }

    @Override
    public void ack(Object id) {
        subscriber.commit((String) id);
    }

    @Override
    public void fail(Object id) {
        subscriber.fail((String) id);
    }

    @Override
    public void close() {
        subscriber.close();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }
    }
}
