/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.v0.AirbyteStream;
import io.airbyte.protocol.models.v0.CatalogHelpers;
import io.airbyte.protocol.models.v0.SyncMode;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonFormat extends AbstractFormat {

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonFormat.class);

  private KafkaConsumer<String, JsonNode> consumer;

  public JsonFormat(JsonNode jsonConfig) {
    super(jsonConfig);
  }

  @Override
  protected Map<String, Object> getKafkaConfig() {
    Map<String, Object> props = super.getKafkaConfig();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
    return props;
  }

  @Override
  protected KafkaConsumer<String, JsonNode> getConsumer() {
    if (consumer != null) {
      return consumer;
    }
    Map<String, Object> filteredProps = getKafkaConfig();
    consumer = new KafkaConsumer<>(filteredProps);

    final JsonNode subscription = config.get("subscription");
    LOGGER.info("Kafka subscribe method: {}", subscription.toString());
    switch (subscription.get("subscription_type").asText()) {
      case "subscribe" -> {
        final String topicPattern = subscription.get("topic_pattern").asText();
        consumer.subscribe(Pattern.compile(topicPattern));
        topicsToSubscribe = consumer.listTopics().keySet().stream()
            .filter(topic -> topic.matches(topicPattern))
            .collect(Collectors.toSet());
        LOGGER.info("Topic list: {}", topicsToSubscribe);
      }
      case "assign" -> {
        topicsToSubscribe = new HashSet<>();
        final String topicPartitions = subscription.get("topic_partitions").asText();
        final String[] topicPartitionsStr = topicPartitions.replaceAll("\\s+", "").split(",");
        final List<TopicPartition> topicPartitionList = Arrays.stream(topicPartitionsStr).map(topicPartition -> {
          final String[] pair = topicPartition.split(":");
          topicsToSubscribe.add(pair[0]);
          return new TopicPartition(pair[0], Integer.parseInt(pair[1]));
        }).collect(Collectors.toList());
        LOGGER.info("Topic-partition list: {}", topicPartitionList);
        consumer.assign(topicPartitionList);
      }
    }
    return consumer;
  }

  public Set<String> getTopicsToSubscribe() {
    if (topicsToSubscribe == null) {
      getConsumer();
    }
    return topicsToSubscribe;
  }

  @Override
  public List<AirbyteStream> getStreams() {
    final Set<String> topicsToSubscribe = getTopicsToSubscribe();
    final List<AirbyteStream> streams = topicsToSubscribe.stream().map(topic -> CatalogHelpers
        .createAirbyteStream(topic, Field.of("value", JsonSchemaType.STRING))
        .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL)))
        .collect(Collectors.toList());
    return streams;
  }

  @Override
  public boolean isAccessible() {
    try {
      final String testTopic = config.has("test_topic") ? config.get("test_topic").asText() : "";
      if (!testTopic.isBlank()) {
        final KafkaConsumer<String, JsonNode> consumer = getConsumer();
        consumer.subscribe(Pattern.compile(testTopic));
        consumer.listTopics();
        consumer.close();
        LOGGER.info("Successfully connected to Kafka brokers for topic '{}'.", config.get("test_topic").asText());
      }
      return true;
    } catch (final Exception e) {
      LOGGER.error("Exception attempting to connect to the Kafka brokers: ", e);
      return false;
    }
  }

  protected JsonNode ensureJsonNode(ConsumerRecord<String, ?> record) {
    try {
      return (JsonNode) record.value();
    } catch (ClassCastException e) {
      LOGGER.error("Exception whilst reading avro data from stream", e);
      throw new RuntimeException(e);
    }
  }
}
