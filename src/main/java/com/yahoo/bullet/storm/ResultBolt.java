package com.yahoo.bullet.storm;

import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Metadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

@Slf4j
public class ResultBolt extends BaseRichBolt {
    private PubSub pubSub;
    private Publisher publisher;
    private OutputCollector collector;

    /**
     * Creates a ResultBolt and passes in a {@link PubSub}.
     *
     * @param pubSub PubSub to get a {@link Publisher} from
     */
    public ResultBolt(PubSub pubSub) {
        this.pubSub = pubSub;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.publisher = pubSub.getPublisher();
    }

    @Override
    public void execute(Tuple tuple) {
        PubSubMessage message = new PubSubMessage(tuple.getString(0), tuple.getString(1), (Metadata) tuple.getValue(2));
        try {
            publisher.send(message);
        } catch (PubSubException e) {
            log.error(e.getMessage());
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void cleanup() {
        publisher.close();
    }
}
