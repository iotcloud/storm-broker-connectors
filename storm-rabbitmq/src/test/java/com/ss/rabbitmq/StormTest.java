package com.ss.rabbitmq;

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
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StormTest {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        ErrorReporter r = new ErrorReporter() {
            @Override
            public void reportError(Throwable t) {
                t.printStackTrace();
            }
        };

        RabbitMQSpout spout = new RabbitMQSpout(new Configurator(), r);
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
        public List<Object> deSerialize(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
            Map<String, Object> headers = properties.getHeaders();
            Long timeStamp = (Long) headers.get("time");
            long currentTime = System.currentTimeMillis();

            System.out.println("latency: " + (currentTime - timeStamp) + " initial time: " + timeStamp + " current: " + currentTime);
            List<Object> tuples = new ArrayList<Object>();
            tuples.add(new Long((currentTime - timeStamp)));
            return tuples;
        }
    }

    private static class Configurator implements RabbitMQConfigurator {
        private String url = "amqp://localhost:5672";

        private String queueName = "send";

        @Override
        public String getURL() {
            return url;
        }

        @Override
        public boolean isAutoAcking() {
            return false;
        }

        @Override
        public int getPrefetchCount() {
            return 1024;
        }

        @Override
        public boolean isReQueueOnFail() {
            return false;
        }

        @Override
        public String getConsumerTag() {
            return "sender";
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
