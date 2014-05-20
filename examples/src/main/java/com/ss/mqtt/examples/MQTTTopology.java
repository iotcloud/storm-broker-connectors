package com.ss.mqtt.examples;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.ss.mqtt.MQTTConfigurator;
import com.ss.mqtt.MQTTMessage;
import com.ss.mqtt.MQTTSpout;
import com.ss.mqtt.MessageBuilder;
import org.fusesource.mqtt.client.QoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MQTTTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        MQTTSpout spout = new MQTTSpout(new Configurator());
        builder.setSpout("word", spout, 1);
        builder.setBolt("time1", new PerfAggrBolt(), 2).shuffleGrouping("word");

        Config conf = new Config();
        if (args != null && args.length > 0) {
            conf.setNumWorkers(6);
            StormSubmitter.submitTopology("test", conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Thread.sleep(60000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

    private static class TimeStampMessageBuilder implements MessageBuilder {
        @Override
        public List<Object> deSerialize(MQTTMessage message) {
            return null;
        }

        @Override
        public MQTTMessage serialize(Tuple tuple) {
            return null;
        }
    }

    private static class Configurator implements MQTTConfigurator {
        private String url = "mqtt://localhost:1883";

        private String queueName = "send";

        @Override
        public String getURL() {
            return url;
        }

        @Override
        public List<String> getQueueName() {
            return new ArrayList<String>(Arrays.asList(queueName));
        }

        @Override
        public MessageBuilder getMessageBuilder() {
            return new TimeStampMessageBuilder();
        }

        @Override
        public QoS qosLevel() {
            return QoS.AT_MOST_ONCE;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("time1"));
        }

        @Override
        public int queueSize() {
            return 1024;
        }
    }

    private static class PerfAggrBolt extends BaseRichBolt {
        private static Logger LOG = LoggerFactory.getLogger(PerfAggrBolt.class);
        OutputCollector _collector;

        double averageLatency = 0;

        long count = 0;

        long initTime = 0;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            Long val = (Long) tuple.getValue(0);
            if (initTime == 0) {
                initTime = System.currentTimeMillis();
            }
            // don't count the values in the first 5 secs
            if (System.currentTimeMillis() - initTime > 5000) {
                count++;
                if (val < 0) {
                    averageLatency = 0;
                    count = 0;
                } else {
                    double delta = val - averageLatency;
                    averageLatency = averageLatency + delta / count;
                    _collector.emit(new Values(averageLatency));
                }

                LOG.info("The latency: " + averageLatency + " count: " + count + " val: " + val);
            }
            _collector.ack(tuple);

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("time"));
        }
    }
}
