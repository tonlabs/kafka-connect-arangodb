package io.github.jaredpetersen.kafkaconnectarangodb.consumer;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.javatuples.Triplet;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class ForcedPartitionsAssignorConfig extends AbstractConfig {
    private static final String TOPIC_PARTITIONS_RANGE_REGEX_STR = "^(?<topic>.+)\\:\\[(?<from>\\d+)-(?<to>\\d+)\\]$";
    public static final String TOPICS_SEPARATOR = ",";
    public static final String TOPIC_PARTITIONS_CONFIG = "assignment.topic.partitions";
    public static final String TOPIC_PARTITIONS_CONFIG_DOC = "A set of partitions to be assigned for this consumer.  " +
            "It accepts < topic >:[< from >-< to >] comma separated topic partitions.";

    private static final ConfigDef CONFIG;
    private static final Pattern TOPIC_PARTITIONS_RANGE_REGEX;

    static {
        CONFIG = new ConfigDef()
                .define(TOPIC_PARTITIONS_CONFIG,  ConfigDef.Type.STRING, null,
                        ConfigDef.Importance.HIGH, TOPIC_PARTITIONS_CONFIG_DOC);
        TOPIC_PARTITIONS_RANGE_REGEX = Pattern.compile(TOPIC_PARTITIONS_RANGE_REGEX_STR);
    }

    public ForcedPartitionsAssignorConfig(final Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    public String clientId() {
        return getString(ConsumerConfig.CLIENT_ID_CONFIG);
    }

    public List<Triplet<String, Integer, Integer>> topicRanges() {
        final String[] topics = getString(TOPIC_PARTITIONS_CONFIG).split(TOPICS_SEPARATOR);
        ArrayList<Triplet<String, Integer, Integer>> ranges = new ArrayList<Triplet<String, Integer, Integer>>();
        for (String topicPartitionsConfig : topics) {
            Matcher parsed = TOPIC_PARTITIONS_RANGE_REGEX.matcher(topicPartitionsConfig);
            if (parsed.matches()) {
                final String topic = parsed.group("topic");
                final int from = Integer.parseInt(parsed.group("from"));
                final int to = Integer.parseInt(parsed.group("to")); 
                ranges.add(Triplet.with(topic, from, to)); 
            } 
        }
        return ranges;
    }
}
