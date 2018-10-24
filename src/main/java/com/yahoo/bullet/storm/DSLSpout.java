package com.yahoo.bullet.storm;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.connector.BulletConnector;
import com.yahoo.bullet.dsl.connector.BulletConnectorException;
import com.yahoo.bullet.dsl.converter.BulletRecordConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

@Slf4j
public class DSLSpout extends BaseRichSpout {

    public static final String RECORD_FIELD = "record";
    public static final String TIMESTAMP_FIELD = "timestamp";

    private SpoutOutputCollector collector;
    private TopologyContext context;

    private BulletDSLConfig config;
    private BulletConnector connector;
    private BulletRecordConverter converter;

    public DSLSpout(BulletStormConfig config) {
        this.config = new BulletDSLConfig(config);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.context = context;
    }

    @Override
    public void activate() {
        connector = BulletConnector.from(config);
        converter = BulletRecordConverter.from(config);
        log.info("DSLSpout activated");
    }

    @Override
    public void deactivate() {
        try {
            if (connector != null) {
                connector.close();
            }
        } catch (Exception e) {
            log.error("Could not close BulletConnector.", e);
        }
        connector = null;
        converter = null;
        log.info("DSLSpout deactivated");
    }

    @Override
    public void nextTuple() {
        try {
            List<Object> objects = connector.read();
            if (!objects.isEmpty()) {
                long timestamp = System.currentTimeMillis();
                objects.stream().map(converter::convert).forEach(record -> collector.emit(new Values(record, timestamp)));
            }
        } catch (BulletConnectorException e) {
            log.error("Could not read from BulletConnector.", e);
        }
        // TODO ................ BulletRecordConverter tends to throw RuntimeException.............. that's not good..?
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(RECORD_FIELD, TIMESTAMP_FIELD));
    }

    @Override
    public void close() {
        try {
            if (connector != null) {
                connector.close();
            }
        } catch (Exception e) {
            log.error("Could not close BulletConnector.", e);
        }
    }
}
