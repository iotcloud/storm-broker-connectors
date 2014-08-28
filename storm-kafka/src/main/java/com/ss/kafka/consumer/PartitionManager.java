package com.ss.kafka.consumer;

import com.google.common.collect.ImmutableMap;
import com.ss.commons.MessageContext;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ss.kafka.consumer.KConsumer.MessageAndRealOffset;
import com.ss.kafka.consumer.KConsumer.EmitState;

import java.util.*;
import java.util.concurrent.BlockingQueue;

public class PartitionManager {
    public static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

    Long _emittedToOffset;
    SortedSet<Long> _pending = new TreeSet<Long>();
    SortedSet<Long> failed = new TreeSet<Long>();
    Long _committedTo;
    LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
    Partition _partition;
    ConsumerConfig _consumerConfig;
    String _topologyInstanceId;
    SimpleConsumer _consumer;
    DynamicPartitionConnections _connections;
    ZkState _state;
    long numberFailed, numberAcked;

    String _sensor;

    public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId,
                            ZkState state, ConsumerConfig consumerConfig, Partition id, String sensor) {
        _partition = id;
        _connections = connections;
        _consumerConfig = consumerConfig;
        _topologyInstanceId = topologyInstanceId;
        _consumer = connections.register(id.host, id.partition);
        _state = state;
        numberAcked = numberFailed = 0;

        String jsonTopologyId = null;
        Long jsonOffset = null;
        String path = committedPath();
        try {
            Map<Object, Object> json = _state.readJSON(path);
            LOG.info("Read partition information from: " + path +  "  --> " + json );
            if (json != null) {
                jsonTopologyId = (String) ((Map<Object, Object>) json.get("topology")).get("id");
                jsonOffset = (Long) json.get("offset");
            }
        } catch (Throwable e) {
            LOG.warn("Error reading and/or parsing at ZkNode: " + path, e);
        }

        Long currentOffset = KafkaUtils.getOffset(_consumer, consumerConfig.topic, id.partition, consumerConfig);

        if (jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
            _committedTo = currentOffset;
            LOG.info("No partition information found, using configuration to determine offset");
        } else if (!topologyInstanceId.equals(jsonTopologyId) && consumerConfig.forceFromStart) {
            _committedTo = KafkaUtils.getOffset(_consumer, consumerConfig.topic, id.partition, consumerConfig.startOffsetTime);
            LOG.info("Topology change detected and reset from start forced, using configuration to determine offset");
        } else {
            _committedTo = jsonOffset;
            LOG.info("Read last commit offset from zookeeper: " + _committedTo + "; old topology_id: " + jsonTopologyId + " - new topology_id: " + topologyInstanceId );
        }

        if (currentOffset - _committedTo > consumerConfig.maxOffsetBehind || _committedTo <= 0) {
            LOG.info("Last commit offset from zookeeper: " + _committedTo);
            _committedTo = currentOffset;
            LOG.info("Commit offset " + _committedTo + " is more than " +
                    consumerConfig.maxOffsetBehind + " behind, resetting to startOffsetTime=" + consumerConfig.startOffsetTime);
        }

        LOG.info("Starting Kafka " + _consumer.host() + ":" + id.partition + " from offset " + _committedTo);
        _emittedToOffset = _committedTo;
        _sensor = sensor;
    }

    //returns false if it's reached the end of current batch
    public KConsumer.EmitState next(BlockingQueue<MessageContext> collector) {
        if (_waitingToEmit.isEmpty()) {
            fill();
        }
        KConsumer.MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();
        if (toEmit == null) {
            return EmitState.NO_EMITTED;
        }

        try {
            byte []b = new byte[toEmit.msg.payload().remaining()];
            toEmit.msg.payload().get(b);
            MessageContext messageContext = new MessageContext(b, null);
            collector.put(messageContext);
        } catch (Exception e) {
            LOG.error("Failed to convert the bytes to Thrift object", e);
        }

        ack(toEmit.offset);
        if (!_waitingToEmit.isEmpty()) {
            return EmitState.EMITTED_MORE_LEFT;
        } else {
            return EmitState.EMITTED_END;
        }
    }

    private void fill() {
        long offset;
        final boolean had_failed = !failed.isEmpty();

        // Are there failed tuples? If so, fetch those first.
        if (had_failed) {
            offset = failed.first();
        } else {
            offset = _emittedToOffset;
        }

        ByteBufferMessageSet msgs = KafkaUtils.fetchMessages(_consumerConfig, _consumer, _partition, offset);
        if (msgs != null) {
            for (MessageAndOffset msg : msgs) {
                final Long cur_offset = msg.offset();
                if (cur_offset < offset) {
                    // Skip any old offsets.
                    continue;
                }
                if (!had_failed || failed.contains(cur_offset)) {
                    _pending.add(cur_offset);
                    _waitingToEmit.add(new MessageAndRealOffset(msg.message(), cur_offset));
                    _emittedToOffset = Math.max(msg.nextOffset(), _emittedToOffset);
                    if (had_failed) {
                        failed.remove(cur_offset);
                    }
                }
            }
        }
        ack(offset);
    }

    public void ack(Long offset) {
        if (!_pending.isEmpty() && _pending.first() < offset - _consumerConfig.maxOffsetBehind) {
            // Too many things pending!
            _pending.headSet(offset).clear();
        } else {
            _pending.remove(offset);
        }
        numberAcked++;
    }

    public void fail(Long offset) {
        if (offset < _emittedToOffset - _consumerConfig.maxOffsetBehind) {
            LOG.info(
                    "Skipping failed tuple at offset=" + offset +
                            " because it's more than maxOffsetBehind=" + _consumerConfig.maxOffsetBehind +
                            " behind _emittedToOffset=" + _emittedToOffset
            );
        } else {
            LOG.debug("failing at offset=" + offset + " with _pending.size()=" + _pending.size() + " pending and _emittedToOffset=" + _emittedToOffset);
            failed.add(offset);
            numberFailed++;
            if (numberAcked == 0 && numberFailed > _consumerConfig.maxOffsetBehind) {
                throw new RuntimeException("Too many tuple failures");
            }
        }
    }

    public void commit() {
        long lastCompletedOffset = lastCompletedOffset();
        if (_committedTo != lastCompletedOffset) {
            LOG.debug("Writing last completed offset (" + lastCompletedOffset + ") to ZK for " + _partition + " for topology: " + _topologyInstanceId);
            Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
                    .put("topology", ImmutableMap.of("id", _topologyInstanceId,
                            "name", _sensor))
                    .put("offset", lastCompletedOffset)
                    .put("partition", _partition.partition)
                    .put("broker", ImmutableMap.of("host", _partition.host.host,
                            "port", _partition.host.port))
                    .put("topic", _consumerConfig.topic).build();
            _state.writeJSON(committedPath(), data);

            _committedTo = lastCompletedOffset;
            LOG.debug("Wrote last completed offset (" + lastCompletedOffset + ") to ZK for " + _partition + " for topology: " + _topologyInstanceId);
        } else {
            LOG.debug("No new offset for " + _partition + " for topology: " + _topologyInstanceId);
        }
    }

    private String committedPath() {
        return _consumerConfig.zkRoot + "/" + _consumerConfig.id + "/" + _partition.getId();
    }

    public long lastCompletedOffset() {
        if (_pending.isEmpty()) {
            return _emittedToOffset;
        } else {
            return _pending.first();
        }
    }

    public Partition getPartition() {
        return _partition;
    }

    public void close() {
        _connections.unregister(_partition.host, _partition.partition);
    }

    static class KafkaMessageId {
        public Partition partition;
        public long offset;

        public KafkaMessageId(Partition partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }
}
