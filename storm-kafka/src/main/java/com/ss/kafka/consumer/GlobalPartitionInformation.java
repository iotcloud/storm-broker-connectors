package com.ss.kafka.consumer;

import java.io.Serializable;
import java.util.*;

public class GlobalPartitionInformation implements Iterable<Partition>, Serializable {

    private Map<Integer, Broker> partitionMap;

    public GlobalPartitionInformation() {
        partitionMap = new TreeMap<Integer, Broker>();
    }

    public void addPartition(int partitionId, Broker broker) {
        partitionMap.put(partitionId, broker);
    }

    @Override
    public String toString() {
        return "GlobalPartitionInformation{" +
                "partitionMap=" + partitionMap +
                '}';
    }

    public Broker getBrokerFor(Integer partitionId) {
        return partitionMap.get(partitionId);
    }

    public List<Partition> getOrderedPartitions() {
        List<Partition> partitions = new LinkedList<Partition>();
        for (Map.Entry<Integer, Broker> partition : partitionMap.entrySet()) {
            partitions.add(new Partition(partition.getValue(), partition.getKey()));
        }
        return partitions;
    }

    @Override
    public Iterator<Partition> iterator() {
        final Iterator<Map.Entry<Integer, Broker>> iterator = partitionMap.entrySet().iterator();

        return new Iterator<Partition>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Partition next() {
                Map.Entry<Integer, Broker> next = iterator.next();
                return new Partition(next.getValue(), next.getKey());
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }

    @Override
    public int hashCode() {
        return com.google.common.base.Objects.hashCode(partitionMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final GlobalPartitionInformation other = (GlobalPartitionInformation) obj;
        return com.google.common.base.Objects.equal(this.partitionMap, other.partitionMap);
    }
}