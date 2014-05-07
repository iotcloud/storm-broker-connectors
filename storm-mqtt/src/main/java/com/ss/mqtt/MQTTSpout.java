package com.ss.mqtt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class MQTTSpout extends BaseRichSpout {
    private Logger logger;

    private transient SpoutOutputCollector collector;

    private Map<Long, String> queueMessageMap = new HashMap<Long, String>();

    private Map<String, MessageConsumer> messageConsumers = new HashMap<String, MessageConsumer>();

    private BlockingQueue<Message> messages;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

    }

    @Override
    public void nextTuple() {

    }
}
