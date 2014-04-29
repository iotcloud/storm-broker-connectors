package com.ss.jms;

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
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.*;

public class StormTest {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        JMSSpout spout = new JMSSpout(new Configurator("tcp://localhost:61616", 10), null);
        builder.setSpout("word", spout, 1);
        builder.setBolt("time1", new PerfAggrBolt(), 1).shuffleGrouping("word");

        Config conf = new Config();
        if (args != null && args.length > 0) {
            conf.setNumWorkers(2);
            StormSubmitter.submitTopology("test", conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Thread.sleep(6000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

    private static class TimeStampMessageBuilder implements MessageBuilder {
        @Override
        public List<Object> deSerialize(Message envelope) {
            Long timeStamp = null;
            try {
                timeStamp = envelope.getJMSTimestamp();
                long currentTime = System.currentTimeMillis();

                System.out.println("latency: " + (currentTime - timeStamp) + " initial time: " + timeStamp + " current: " + currentTime);
                List<Object> tuples = new ArrayList<Object>();
                tuples.add(new Long((currentTime - timeStamp)));
                return tuples;
            } catch (JMSException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private static class Configurator implements JMSConfigurator {
        private String url = "tcp://localhost:61616";

        private String queueName = "send";

        ActiveMQConnectionFactory connectionFactory;
        Map<String, Destination> destinations;

        int number;

        private Configurator(String url, int number) {
            this.url = url;
            this.number = number;
            destinations = new HashMap<String, Destination>();
            this.connectionFactory = new ActiveMQConnectionFactory(url);
            connectionFactory.setOptimizeAcknowledge(true);
            connectionFactory.setAlwaysSessionAsync(false);

            Connection connection;
            try {
                connection = connectionFactory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                for (int i = 0; i < number; i++) {
                    this.destinations.put(queueName + i, session.createQueue(queueName + i));
                }

                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        @Override
        public int ackMode() {
            return Session.AUTO_ACKNOWLEDGE;
        }

        @Override
        public ConnectionFactory connectionFactory() throws Exception {
            return connectionFactory;
        }

        @Override
        public Map<String, Destination> destinations() throws Exception {
            return destinations;
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
