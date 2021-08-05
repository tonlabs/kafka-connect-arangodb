package io.github.jaredpetersen.kafkaconnectarangodb.consumer;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

import org.javatuples.Triplet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ForcedPartitionsAssignor 
    implements ConsumerPartitionAssignor, Configurable 
{

    private ForcedPartitionsAssignorConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return "ForcedPartitions";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public short version() {
        return (short) 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new ForcedPartitionsAssignorConfig(configs);
    }

    /**
     * {@inheritDoc}
     */
    public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
        final String clientId = this.config.clientId();
        Map<String, Set<Integer>> topicPartitions = new HashMap<String, Set<Integer>>();
        for (final Triplet<String, Integer, Integer> topicPartitionsRange : this.config.topicRanges()) {
            final String topic = topicPartitionsRange.getValue0();
            final Set<Integer> partitions = new HashSet<Integer>();
            for (int i = topicPartitionsRange.getValue1(); i <= topicPartitionsRange.getValue2(); i++) {
                partitions.add(i);
            } 
            if (topicPartitions.containsKey(topic)) {
                partitions.addAll(topicPartitions.get(topic));
            }
            topicPartitions.put(topic, partitions);
        }
        Assignment assignment = new Assignment(new ArrayList());
        topicPartitions.forEach((topic, partitions) -> {
            partitions
                .stream()
                .forEachOrdered(p -> assignment.partitions().add(new TopicPartition(topic, p)));
        });
        Map<String, Assignment> assignments = new HashMap<String, Assignment>();
        assignments.put(clientId, assignment);
        return new GroupAssignment(assignments); 
    }
}
