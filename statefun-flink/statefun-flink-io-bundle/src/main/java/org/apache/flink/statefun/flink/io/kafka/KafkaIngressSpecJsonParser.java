/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.flink.io.kafka;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.NamespaceNamePair;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.generated.TargetFunctionType;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressAutoResetPosition;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressStartupPosition;
import org.apache.flink.statefun.sdk.kafka.KafkaTopicPartition;

final class KafkaIngressSpecJsonParser {

  private KafkaIngressSpecJsonParser() {}

  private static final JsonPointer DESCRIPTOR_SET_POINTER =
      JsonPointer.compile("/ingress/spec/descriptorSet");
  private static final JsonPointer TOPIC_PATTERN_POINTER =
      JsonPointer.compile("/ingress/spec/topicPattern");
  private static final JsonPointer TOPICS_POINTER = JsonPointer.compile("/ingress/spec/topics");
  private static final JsonPointer MESSAGE_TYPE_POINTER =
      JsonPointer.compile("/ingress/spec/messageType");
  private static final JsonPointer PROPERTIES_POINTER =
      JsonPointer.compile("/ingress/spec/properties");
  private static final JsonPointer ADDRESS_POINTER = JsonPointer.compile("/ingress/spec/address");
  private static final JsonPointer GROUP_ID_POINTER =
      JsonPointer.compile("/ingress/spec/consumerGroupId");
  private static final JsonPointer AUTO_RESET_POS_POINTER =
      JsonPointer.compile("/ingress/spec/autoOffsetResetPosition");

  private static final JsonPointer STARTUP_POS_POINTER =
      JsonPointer.compile("/ingress/spec/startupPosition");
  private static final JsonPointer STARTUP_POS_TYPE_POINTER =
      JsonPointer.compile("/ingress/spec/startupPosition/type");
  private static final JsonPointer STARTUP_SPECIFIC_OFFSETS_POINTER =
      JsonPointer.compile("/ingress/spec/startupPosition/offsets");
  private static final JsonPointer STARTUP_DATE_POINTER =
      JsonPointer.compile("/ingress/spec/startupPosition/date");

  private static final JsonPointer ROUTABLE_TOPIC_NAME_POINTER = JsonPointer.compile("/topic");
  private static final JsonPointer ROUTABLE_TOPIC_PATTERN_POINTER = JsonPointer.compile("/pattern");
  private static final JsonPointer ROUTABLE_TOPIC_DISCOVERY_POINTER =
      JsonPointer.compile("/discoveryInterval");
  private static final JsonPointer ROUTABLE_TOPIC_VALUE_TYPE_POINTER =
      JsonPointer.compile("/valueType");
  private static final JsonPointer ROUTABLE_TOPIC_TARGETS_POINTER = JsonPointer.compile("/targets");

  private static final String STARTUP_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS Z";
  private static final DateTimeFormatter STARTUP_DATE_FORMATTER =
      DateTimeFormatter.ofPattern(STARTUP_DATE_PATTERN);

  static List<String> topics(JsonNode json) {
    return Selectors.textListAt(json, TOPICS_POINTER);
  }

  static RoutingSubscriber routingSubscriber(JsonNode jsonNode) {
    boolean hasTopicList = Selectors.hasValueAt(jsonNode, TOPICS_POINTER);
    boolean hasTopicPattern = Selectors.hasValueAt(jsonNode, TOPIC_PATTERN_POINTER);

    if (hasTopicList == hasTopicPattern) {
      throw new IllegalStateException(
          "Kafka ingress must be configured with either a list of Kafka topics or a single topic pattern but not both.");
    }

    if (hasTopicList) {
      return routableTopics(jsonNode);
    } else {
      return routablePattern(jsonNode);
    }
  }

  private static RoutingSubscriber routablePattern(JsonNode json) {
    JsonNode patternTopicNode = json.at(TOPIC_PATTERN_POINTER);
    final Pattern pattern;

    try {
      pattern = Pattern.compile(Selectors.textAt(patternTopicNode, ROUTABLE_TOPIC_PATTERN_POINTER));
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException("Invalid Kafka topic pattern", e);
    }

    final Duration discoveryInterval =
        Selectors.durationAt(patternTopicNode, ROUTABLE_TOPIC_DISCOVERY_POINTER);
    final String typeUrl = Selectors.textAt(patternTopicNode, ROUTABLE_TOPIC_VALUE_TYPE_POINTER);
    final List<TargetFunctionType> targets = parseRoutableTargetFunctionTypes(patternTopicNode);

    return RoutingSubscriber.fromPattern(
        pattern,
        discoveryInterval,
        RoutingConfig.newBuilder().setTypeUrl(typeUrl).addAllTargetFunctionTypes(targets).build());
  }

  private static RoutingSubscriber routableTopics(JsonNode json) {
    Map<String, RoutingConfig> routableTopics = new HashMap<>();
    for (JsonNode routableTopicNode : Selectors.listAt(json, TOPICS_POINTER)) {
      final String topic = Selectors.textAt(routableTopicNode, ROUTABLE_TOPIC_NAME_POINTER);
      final String typeUrl = Selectors.textAt(routableTopicNode, ROUTABLE_TOPIC_VALUE_TYPE_POINTER);
      final List<TargetFunctionType> targets = parseRoutableTargetFunctionTypes(routableTopicNode);

      routableTopics.put(
          topic,
          RoutingConfig.newBuilder()
              .setTypeUrl(typeUrl)
              .addAllTargetFunctionTypes(targets)
              .build());
    }
    return RoutingSubscriber.fromConfigMap(routableTopics);
  }

  static Properties kafkaClientProperties(JsonNode json) {
    Map<String, String> kvs = Selectors.propertiesAt(json, PROPERTIES_POINTER);
    Properties properties = new Properties();
    kvs.forEach(properties::setProperty);
    return properties;
  }

  static String kafkaAddress(JsonNode json) {
    return Selectors.textAt(json, ADDRESS_POINTER);
  }

  @SuppressWarnings("unchecked")
  static <T> KafkaIngressDeserializer<T> deserializer(JsonNode json) {
    String descriptorSetPath = Selectors.textAt(json, DESCRIPTOR_SET_POINTER);
    String messageType = Selectors.textAt(json, MESSAGE_TYPE_POINTER);
    // this cast is safe since we validate that the produced message type (T) is assignable to a
    // Message.
    // see asJsonIngressSpec()
    return (KafkaIngressDeserializer<T>)
        new ProtobufKafkaIngressDeserializer(descriptorSetPath, messageType);
  }

  static Optional<String> optionalConsumerGroupId(JsonNode json) {
    return Selectors.optionalTextAt(json, GROUP_ID_POINTER);
  }

  static Optional<KafkaIngressAutoResetPosition> optionalAutoOffsetResetPosition(JsonNode json) {
    Optional<String> conf = Selectors.optionalTextAt(json, AUTO_RESET_POS_POINTER);
    if (!conf.isPresent()) {
      return Optional.empty();
    }

    String autoOffsetResetConfig = conf.get().toUpperCase(Locale.ENGLISH);

    try {
      return Optional.of(KafkaIngressAutoResetPosition.valueOf(autoOffsetResetConfig));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid autoOffsetResetPosition: "
              + autoOffsetResetConfig
              + "; valid values are "
              + Arrays.toString(KafkaIngressAutoResetPosition.values()),
          e);
    }
  }

  static Optional<KafkaIngressStartupPosition> optionalStartupPosition(JsonNode json) {
    if (json.at(STARTUP_POS_POINTER).isMissingNode()) {
      return Optional.empty();
    }

    String startupType =
        Selectors.textAt(json, STARTUP_POS_TYPE_POINTER).toLowerCase(Locale.ENGLISH);
    switch (startupType) {
      case "group-offsets":
        return Optional.of(KafkaIngressStartupPosition.fromGroupOffsets());
      case "earliest":
        return Optional.of(KafkaIngressStartupPosition.fromEarliest());
      case "latest":
        return Optional.of(KafkaIngressStartupPosition.fromLatest());
      case "specific-offsets":
        return Optional.of(
            KafkaIngressStartupPosition.fromSpecificOffsets(specificOffsetsStartupMap(json)));
      case "date":
        return Optional.of(KafkaIngressStartupPosition.fromDate(startupDate(json)));
      default:
        throw new IllegalArgumentException(
            "Invalid startup position type: "
                + startupType
                + "; valid values are [group-offsets, earliest, latest, specific-offsets, date]");
    }
  }

  private static Map<KafkaTopicPartition, Long> specificOffsetsStartupMap(JsonNode json) {
    Map<String, Long> kvs = Selectors.longPropertiesAt(json, STARTUP_SPECIFIC_OFFSETS_POINTER);
    Map<KafkaTopicPartition, Long> offsets = new HashMap<>(kvs.size());
    kvs.forEach(
        (partition, offset) ->
            offsets.put(KafkaTopicPartition.fromString(partition), validateOffsetLong(offset)));
    return offsets;
  }

  private static ZonedDateTime startupDate(JsonNode json) {
    String dateStr = Selectors.textAt(json, STARTUP_DATE_POINTER);
    try {
      return ZonedDateTime.parse(dateStr, STARTUP_DATE_FORMATTER);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          "Unable to parse date string for startup position: "
              + dateStr
              + "; the date should conform to the pattern "
              + STARTUP_DATE_PATTERN,
          e);
    }
  }

  private static Long validateOffsetLong(Long offset) {
    if (offset < 0) {
      throw new IllegalArgumentException(
          "Invalid offset value: "
              + offset
              + "; must be a numeric integer with value between 0 and "
              + Long.MAX_VALUE);
    }

    return offset;
  }

  private static List<TargetFunctionType> parseRoutableTargetFunctionTypes(
      JsonNode routableTopicNode) {
    final List<TargetFunctionType> targets = new ArrayList<>();
    for (String namespaceAndName :
        Selectors.textListAt(routableTopicNode, ROUTABLE_TOPIC_TARGETS_POINTER)) {
      NamespaceNamePair namespaceNamePair = NamespaceNamePair.from(namespaceAndName);
      targets.add(
          TargetFunctionType.newBuilder()
              .setNamespace(namespaceNamePair.namespace())
              .setType(namespaceNamePair.name())
              .build());
    }
    return targets;
  }
}
